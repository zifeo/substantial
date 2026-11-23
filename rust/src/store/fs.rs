use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;

use super::memory::{Clock, wall_clock};
use super::names::{TMP, check_key, is_component};
use super::retry::{self, Fault};
use super::{
    Capabilities, Cas, Create, DAY_MS, ListWait, Listing, Object, Store, StoreFuture, check_days,
};
use crate::error::{Error, Result};
use crate::settings::StoreRetry;

/// The bytes before a version's value: the instant it expires at, ms since the epoch big-endian,
/// 0 for none, `TOMBSTONE` for a tombstone, then the key's incarnation.
const HEADER: usize = 24;

/// `file:<path>`: the directory is the namespace, one directory of numbered versions per key. For
/// development, CI and one machine.
///
/// Keys never differ only by case (I-17), so a file system that folds case holds them as they are.
#[derive(Clone)]
pub struct FsStore {
    root: PathBuf,
    retry: StoreRetry,
    clock: Clock,
    /// When the last sweep began.
    swept: Arc<Mutex<Option<i64>>>,
}

impl std::fmt::Debug for FsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsStore").field("root", &self.root).finish()
    }
}

impl FsStore {
    pub fn open(path: impl AsRef<Path>, retry: StoreRetry) -> Result<Self> {
        Self::with_clock(path, retry, wall_clock())
    }

    /// The store at `path`, its expiries judged by `clock`, in ms since the epoch; for the tests.
    #[doc(hidden)]
    pub fn with_clock(path: impl AsRef<Path>, retry: StoreRetry, clock: Clock) -> Result<Self> {
        retry.validate()?;
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join(TMP))
            .map_err(|e| Error::StoreError(format!("cannot prepare {}: {e}", root.display())))?;
        let store = Self {
            root,
            retry,
            clock,
            swept: Arc::default(),
        };
        store.sweep_when_due();
        Ok(store)
    }

    /// Buries the expired keys now and removes the versions old enough; how many keys it buried.
    #[doc(hidden)]
    pub fn sweep(&self) -> Result<usize> {
        sweep(&self.root, (self.clock)()).map_err(|e| match e {
            Fault::Transient(m) | Fault::Ambiguous(m) | Fault::Permanent(m) => Error::StoreError(m),
        })
    }

    /// Starts a sweep in a thread of its own at open and then daily.
    fn sweep_when_due(&self) {
        // § qf98sbvzj.a@29zck
        let now = (self.clock)();
        {
            let mut swept = self.swept.lock().expect("never poisoned");
            if swept.is_some_and(|at| now - at < DAY_MS as i64) {
                return;
            }
            *swept = Some(now);
        }
        let root = self.root.clone();
        std::thread::spawn(move || {
            if let Err(Fault::Transient(e) | Fault::Ambiguous(e) | Fault::Permanent(e)) =
                sweep(&root, now)
            {
                tracing::warn!("the sweep of the file store {} failed: {e}", root.display());
            }
        });
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Path, i64) -> Result<T, Fault> + Send + 'static,
    ) -> Result<T, Fault> {
        self.sweep_when_due();
        let root = self.root.clone();
        let now = (self.clock)();
        tokio::task::spawn_blocking(move || f(&root, now))
            .await
            .map_err(|e| Fault::Permanent(format!("file store task failed: {e}")))?
    }
}

fn transient(what: &str, path: &Path) -> impl FnOnce(std::io::Error) -> Fault {
    let at = format!("{what} {}", path.display());
    move |e| Fault::Transient(format!("{at}: {e}"))
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Creates the missing directories between `root` and `dir`, each fsynced in its parent; false
/// when a sweep removed one of them meanwhile, for the caller to try again.
fn ensure_dirs(root: &Path, dir: &Path) -> Result<bool, Fault> {
    // § c7pz5rk26.b@3q1a8
    let mut missing = Vec::new();
    let mut at = dir;
    while at != root && !at.is_dir() {
        missing.push(at);
        at = at.parent().expect("a key directory lies under the root");
    }
    for dir in missing.into_iter().rev() {
        let parent = dir.parent().expect("under the root");
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            // macOS answers EINVAL in a directory being removed.
            Err(_) if !parent.is_dir() => return Ok(false),
            Err(e) if e.kind() == ErrorKind::NotADirectory => {
                return Err(Fault::Permanent(format!(
                    "{} lies under another key: {e}",
                    dir.display()
                )));
            }
            Err(e) => return Err(transient("create directory", dir)(e)),
        }
        match sync_dir(parent) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(transient("sync directory", parent)(e)),
        }
    }
    Ok(true)
}

/// The instant a write of `expiry` days at `now` expires at, 0 for none.
fn expires(expiry: Option<u32>, now: i64) -> i64 {
    expiry.map_or(0, |d| now + (u64::from(d) * DAY_MS) as i64)
}

/// The header's expiry of a tombstone, the version a delete or the sweep writes.
const TOMBSTONE: i64 = -1;

/// A version is removed only once its successor is this old, and a tombstone, latest of its key,
/// once it is this old itself; an operation that decides on a read stops and reads again once
/// it is this old: an older read could predate the removal of a version it relies on.
const SUPERSEDED_KEPT: Duration = Duration::from_secs(600);

/// The incarnation of a key, drawn at each create: an ETag names it, so no ETag of a removed key
/// matches the key created again.
type Incarnation = [u8; 16];

/// The name of version `n` in its key's directory, never a key component.
fn version(n: u64) -> String {
    format!(".{n:020}")
}

fn version_of(name: &str) -> Option<u64> {
    let digits = name.strip_prefix('.')?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// The ETag of version `n` of the incarnation `inc`.
fn etag_of(inc: &Incarnation, n: u64) -> String {
    format!("{}.{n}", hex::encode(inc))
}

fn parse_etag(etag: &str) -> Option<(Incarnation, u64)> {
    let (inc, n) = etag.split_once('.')?;
    let inc: Incarnation = hex::decode(inc).ok()?.try_into().ok()?;
    (!n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        .then(|| n.parse().ok().map(|n| (inc, n)))
        .flatten()
}

/// The versions the directory of a key lists, lowest first.
fn versions(dir: &Path) -> Result<Vec<u64>, Fault> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(Vec::new());
        }
        Err(e) => return Err(transient("list", dir)(e)),
    };
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.map_err(transient("list", dir))?;
        if let Some(n) = entry.file_name().to_str().and_then(version_of) {
            found.push(n);
        }
    }
    found.sort_unstable();
    Ok(found)
}

/// A version as its file holds it.
struct Version {
    value: Vec<u8>,
    /// The expiry instant, 0 for none, `TOMBSTONE` for a tombstone.
    at: i64,
    inc: Incarnation,
}

/// A version, `None` when it is missing.
fn read(path: &Path) -> Result<Option<Version>, Fault> {
    // § qf98sbvzj@gpvz5
    let mut file = match fs::read(path) {
        Ok(file) => file,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(None);
        }
        Err(e) => return Err(transient("read", path)(e)),
    };
    if file.len() < HEADER {
        return Err(Fault::Permanent(format!(
            "{} is not a file of this store: shorter than its header",
            path.display()
        )));
    }
    let value = file.split_off(HEADER);
    let at = i64::from_be_bytes(file[..8].try_into().expect("8 bytes"));
    let inc = file[8..].try_into().expect("16 bytes");
    Ok(Some(Version { value, at, inc }))
}

fn exists(path: &Path) -> Result<bool, Fault> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => Ok(false),
        Err(e) => Err(transient("stat", path)(e)),
    }
}

/// Whether a version of header `at` reads as missing at `now`: a tombstone, or past its expiry.
fn gone(at: i64, now: i64) -> bool {
    at == TOMBSTONE || (at > 0 && now >= at)
}

/// The latest version of a key and its number, read so that no later one existed when it
/// answers.
///
/// A version goes only once its successor is `SUPERSEDED_KEPT` old, the lowest first, so the
/// latest one and every one written during the listing stay: the listing shows the latest at
/// its start, and the next ones are found by name. A version gone when read was superseded, and
/// is listed again.
fn latest(dir: &Path) -> Result<Option<(u64, Version)>, Fault> {
    // § j4b9rkhg8.c@y1vp5
    'listing: loop {
        let started = Instant::now();
        let Some(mut n) = versions(dir)?.last().copied() else {
            return Ok(None);
        };
        loop {
            let Some(found) = read(&dir.join(version(n)))? else {
                continue 'listing;
            };
            if exists(&dir.join(version(n + 1)))? {
                n += 1;
                continue;
            }
            if started.elapsed() >= SUPERSEDED_KEPT {
                continue 'listing;
            }
            return Ok(Some((n, found)));
        }
    }
    // § end j4b9rkhg8.c
}

/// The value as a version file holds it: its header first, so all of it lands together.
fn write_temporary(
    root: &Path,
    bytes: &[u8],
    at: i64,
    inc: &Incarnation,
) -> Result<PathBuf, Fault> {
    let tmp = root.join(TMP).join(hex::encode(rand::random::<[u8; 16]>()));
    let written = File::create_new(&tmp).and_then(|mut f| {
        f.write_all(&at.to_be_bytes())?;
        f.write_all(inc)?;
        f.write_all(bytes)?;
        f.sync_all()
    });
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        return Err(transient("write", &tmp)(e));
    }
    Ok(tmp)
}

/// What linking a version found.
#[derive(PartialEq)]
enum Link {
    Done,
    /// The version exists.
    Taken,
    /// The key's directory is gone, removed by a sweep.
    NoDirectory,
}

/// Writes version `n` of the key directory `dir` by linking `tmp` to it, which fails if the
/// version exists.
fn link(tmp: &Path, dir: &Path, n: u64) -> Result<Link, Fault> {
    // § j4b9rkhg8@nr9b3
    let path = dir.join(version(n));
    match fs::hard_link(tmp, &path) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::AlreadyExists => return Ok(Link::Taken),
        // macOS answers EINVAL in a directory being removed.
        Err(_) if !dir.is_dir() => return Ok(Link::NoDirectory),
        Err(e) if e.kind() == ErrorKind::NotADirectory => {
            return Err(Fault::Permanent(format!(
                "{} lies under another key: {e}",
                dir.display()
            )));
        }
        // On NFS a link that succeeded may still report an error.
        Err(e) => return Err(Fault::Ambiguous(format!("link {}: {e}", path.display()))),
    }
    // § one kkad6aqxp.c@7gqr8
    sync_dir(dir)
        .map_err(|e| Fault::Ambiguous(format!("sync directory {}: {e}", dir.display())))?;
    Ok(Link::Done)
}

/// When the file at `path` was linked, by the store's clock: its ctime, which a link sets.
fn linked_at(path: &Path) -> Result<Option<i64>, Fault> {
    match fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta.ctime() * 1000 + meta.ctime_nsec() / 1_000_000)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(transient("stat", path)(e)),
    }
}

/// Removes the versions below the latest whose successor is `SUPERSEDED_KEPT` old at `now`, the
/// store's clock, the lowest first, so a version is gone only once every version below it is.
fn clean(dir: &Path, now: i64) -> Result<(), Fault> {
    // § j4b9rkhg8.b@ds3s0
    let listed = versions(dir)?;
    let kept = SUPERSEDED_KEPT.as_millis() as i64;
    for pair in listed.windows(2) {
        let superseded = linked_at(&dir.join(version(pair[1])))?.is_some_and(|at| at <= now - kept);
        if pair[1] != pair[0] + 1 || !superseded {
            break;
        }
        let path = dir.join(version(pair[0]));
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(transient("delete", &path)(e)),
        }
    }
    Ok(())
    // § end j4b9rkhg8.b
}

/// Links `tmp` as the version after the latest of `dir`, given the latest when it reads as
/// present, or as version 0 of a directory made again when a sweep removed it; `None` when
/// `decide` refuses.
fn write_after_latest(
    root: &Path,
    dir: &Path,
    tmp: &Path,
    now: i64,
    decide: impl Fn(Option<&Version>) -> bool,
) -> Result<Option<u64>, Fault> {
    loop {
        if !ensure_dirs(root, dir)? {
            continue;
        }
        let started = Instant::now();
        let latest = latest(dir)?;
        let live = latest.as_ref().map(|(_, v)| v).filter(|v| !gone(v.at, now));
        if !decide(live) {
            return Ok(None);
        }
        let next = latest.map_or(0, |(n, _)| n + 1);
        if started.elapsed() >= SUPERSEDED_KEPT {
            continue;
        }
        if link(tmp, dir, next)? == Link::Done {
            return Ok(Some(next));
        }
    }
}

fn create(root: &Path, key: &str, bytes: &[u8], at: i64, now: i64) -> Result<Create, Fault> {
    // § c7pz5rk26@z8g8x
    check_key(key)?;
    let dir = root.join(key);
    // § one j4b9rkhg8.d@28peb
    let inc: Incarnation = rand::random();
    let tmp = write_temporary(root, bytes, at, &inc)?;
    let created = write_after_latest(root, &dir, &tmp, now, |live| live.is_none());
    let _ = fs::remove_file(&tmp);
    match created? {
        Some(n) => {
            clean(&dir, now)?;
            Ok(Create::Created {
                etag: etag_of(&inc, n),
            })
        }
        None => Ok(Create::Exists),
    }
}

fn cas(root: &Path, key: &str, bytes: &[u8], etag: &str, at: i64, now: i64) -> Result<Cas, Fault> {
    // § j4b9rkhg8@yxbdy
    check_key(key)?;
    let dir = root.join(key);
    let Some((inc, n)) = parse_etag(etag) else {
        return Ok(Cas::Mismatch);
    };
    let tmp = write_temporary(root, bytes, at, &inc)?;
    let written = (|| {
        let started = Instant::now();
        // The successor absent, then the version present: no later one was written by then.
        if exists(&dir.join(version(n + 1)))? {
            return Ok(false);
        }
        match read(&dir.join(version(n)))? {
            // § one j4b9rkhg8.d@hfsrm
            Some(found) if found.inc == inc && !gone(found.at, now) => {}
            // § one kkad6aqxp.a@at93x
            _ => return Ok(false),
        }
        if started.elapsed() >= SUPERSEDED_KEPT {
            return Err(Fault::Transient(format!("cas {key}: read too long ago")));
        }
        Ok(link(&tmp, &dir, n + 1)? == Link::Done)
    })();
    let _ = fs::remove_file(&tmp);
    if !written? {
        return Ok(Cas::Mismatch);
    }
    clean(&dir, now)?;
    Ok(Cas::Written {
        etag: etag_of(&inc, n + 1),
    })
}

fn get(root: &Path, key: &str, now: i64) -> Result<Option<Object>, Fault> {
    // § dfd6vt90h.a@7j0bp
    check_key(key)?;
    match latest(&root.join(key))? {
        Some((n, found)) if !gone(found.at, now) => Ok(Some(Object {
            etag: etag_of(&found.inc, n),
            bytes: found.value.into(),
        })),
        _ => Ok(None),
    }
}

/// Writes a tombstone after the latest version of `dir` when that one reads as present at
/// `now`, or, with `only_expired`, as past its expiry: a write naming an earlier version then
/// finds its successor taken.
fn bury(root: &Path, dir: &Path, now: i64, only_expired: bool) -> Result<bool, Fault> {
    // § j4b9rkhg8.a@758cx
    let mut tmp = None;
    let buried = (|| loop {
        let started = Instant::now();
        let Some((n, found)) = latest(dir)? else {
            return Ok(false);
        };
        let wanted = match only_expired {
            true => found.at > 0 && now >= found.at,
            false => !gone(found.at, now),
        };
        if !wanted {
            return Ok(false);
        }
        if tmp.is_none() {
            tmp = Some(write_temporary(root, &[], TOMBSTONE, &found.inc)?);
        }
        if started.elapsed() >= SUPERSEDED_KEPT {
            continue;
        }
        match link(tmp.as_ref().expect("written"), dir, n + 1)? {
            Link::Done => return Ok(true),
            Link::NoDirectory => return Ok(false),
            Link::Taken => {}
        }
    })();
    if let Some(tmp) = &tmp {
        let _ = fs::remove_file(tmp);
    }
    let buried = buried?;
    if buried {
        clean(dir, now)?;
    }
    Ok(buried)
    // § end j4b9rkhg8.a
}

fn delete(root: &Path, key: &str, now: i64) -> Result<(), Fault> {
    // § dfd6vt90h.d@c9and
    check_key(key)?;
    bury(root, &root.join(key), now, false).map(|_| ())
}

/// Removes the directory of a key whose only version is a tombstone `SUPERSEDED_KEPT` old, then
/// each parent left empty, up to the root: a key deleted leaves nothing behind.
///
/// The tombstone goes first: a create that read it links the version after it, which stays. A
/// directory goes only by `rmdir`, which fails while it holds anything, so it never takes a
/// version linked into it; a link that finds its directory gone makes it again.
fn reclaim(root: &Path, dir: &Path, now: i64) -> Result<(), Fault> {
    // § w8a4smsae@pbg96
    let listed = versions(dir)?;
    if let [t] = listed[..] {
        let path = dir.join(version(t));
        let kept = SUPERSEDED_KEPT.as_millis() as i64;
        let old = linked_at(&path)?.is_some_and(|at| at <= now - kept);
        let mut header = [0; 8];
        let buried = File::open(&path)
            .and_then(|mut f| f.read_exact(&mut header))
            .is_ok_and(|()| i64::from_be_bytes(header) == TOMBSTONE);
        if !(old && buried) {
            return Ok(());
        }
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(transient("delete", &path)(e)),
        }
    }
    // § one w8a4smsae.a@v3ybb
    let mut at = dir;
    while at != root {
        match fs::remove_dir(at) {
            Ok(()) => {}
            // Not empty, as a key or a parent of keys, or removed by another sweep.
            Err(_) => return Ok(()),
        }
        at = at.parent().expect("under the root");
    }
    Ok(())
}

/// Buries every key past its expiry at `now`, removes the versions old enough, and the keys
/// buried long enough with their empty parents; how many keys it buried. Every step is a link
/// that may fail or a removal that may find nothing, so sweeps of several processes may overlap.
fn sweep(root: &Path, now: i64) -> Result<usize, Fault> {
    // § qf98sbvzj.a@h3rwm
    let mut dirs = Vec::new();
    key_dirs(root, true, &mut dirs)?;
    let mut buried = 0;
    // The deepest first, so a parent is tried once its children are gone.
    for dir in dirs.iter().rev() {
        if bury(root, dir, now, true)? {
            buried += 1;
        } else {
            clean(dir, now)?;
            reclaim(root, dir, now)?;
        }
    }
    Ok(buried)
}

/// The directories under `dir` that could hold a key, the store's own skipped at the root, each
/// before those under it.
fn key_dirs(dir: &Path, root: bool, out: &mut Vec<PathBuf>) -> Result<(), Fault> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(());
        }
        Err(e) => return Err(transient("list", dir)(e)),
    };
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|n| is_component(n)) else {
            continue;
        };
        if root && name == TMP {
            continue;
        }
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            out.push(entry.path());
            key_dirs(&entry.path(), false, out)?;
        }
    }
    Ok(())
}

/// The header of the latest version a key's directory lists, `None` without one: what a listing
/// needs to leave out a key whose latest version is a tombstone.
fn listed_header(dir: &Path) -> Result<Option<i64>, Fault> {
    loop {
        let Some(n) = versions(dir)?.last().copied() else {
            return Ok(None);
        };
        let path = dir.join(version(n));
        let mut header = [0; 8];
        match File::open(&path).and_then(|mut f| f.read_exact(&mut header)) {
            Ok(()) => return Ok(Some(i64::from_be_bytes(header))),
            // Superseded and removed: list again.
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(transient("read", &path)(e)),
        }
    }
}

/// Walks the deepest directory holding `prefix`, keeps the keys in range, sorts by full key.
fn list(
    root: &Path,
    prefix: &str,
    start_after: Option<&str>,
    max_keys: u32,
) -> Result<Listing, Fault> {
    let (dir_key, _) = prefix.rsplit_once('/').unwrap_or(("", prefix));
    if !dir_key.is_empty() {
        check_key(dir_key)?;
    }
    let mut keys = Vec::new();
    let base = if dir_key.is_empty() {
        String::new()
    } else {
        format!("{dir_key}/")
    };
    walk(&root.join(dir_key), &base, prefix, &mut keys)?;
    keys.sort_unstable();
    Ok(Listing {
        keys: super::in_range(keys.iter(), prefix, start_after, max_keys),
        etag: None,
    })
}

fn walk(dir: &Path, base: &str, prefix: &str, keys: &mut Vec<String>) -> Result<(), Fault> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            return Ok(());
        }
        Err(e) => return Err(transient("list", dir)(e)),
    };
    for entry in entries {
        let entry = entry.map_err(transient("list", dir))?;
        let Some(name) = entry
            .file_name()
            .to_str()
            .filter(|n| is_component(n))
            .map(str::to_owned)
        else {
            continue;
        };
        if base.is_empty() && name == TMP {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(transient("stat", &entry.path()))?;
        if !kind.is_dir() {
            continue;
        }
        let key = format!("{base}{name}");
        if key.starts_with(prefix)
            && listed_header(&entry.path())?.is_some_and(|at| at != TOMBSTONE)
        {
            keys.push(key.clone());
        }
        let sub = format!("{key}/");
        if sub.starts_with(prefix) || prefix.starts_with(&sub) {
            walk(&entry.path(), &sub, prefix, keys)?;
        }
    }
    Ok(())
}

impl Store for FsStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: false }
    }

    fn open(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            check_days(expiry).map_err(super::StoreFailure::new)?;
            let attempt = || {
                let (key, bytes) = (key.to_owned(), bytes.clone());
                self.blocking(move |root, now| {
                    create(root, &key, &bytes, expires(expiry, now), now)
                })
            };
            let read = || {
                let key = key.to_owned();
                self.blocking(move |root, now| get(root, &key, now))
            };
            retry::create(self.retry, &bytes, attempt, read, tokio::time::sleep).await
        })
    }

    fn cas<'a>(
        &'a self,
        key: &'a str,
        bytes: Bytes,
        etag: &'a str,
        expiry: Option<u32>,
    ) -> StoreFuture<'a, Cas> {
        Box::pin(async move {
            check_days(expiry).map_err(super::StoreFailure::new)?;
            let attempt = || {
                let (key, bytes, etag) = (key.to_owned(), bytes.clone(), etag.to_owned());
                self.blocking(move |root, now| {
                    cas(root, &key, &bytes, &etag, expires(expiry, now), now)
                })
            };
            let read = || {
                let key = key.to_owned();
                self.blocking(move |root, now| get(root, &key, now))
            };
            retry::cas(self.retry, &bytes, etag, attempt, read, tokio::time::sleep).await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Object>> {
        Box::pin(retry::with_retry(
            self.retry,
            move || {
                let key = key.to_owned();
                self.blocking(move |root, now| get(root, &key, now))
            },
            tokio::time::sleep,
        ))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        max_keys: u32,
        wait: Option<ListWait>,
    ) -> StoreFuture<'a, Listing> {
        Box::pin(async move {
            super::refuse_wait(&wait)?;
            retry::with_retry(
                self.retry,
                || {
                    let (prefix, start_after) = (prefix.to_owned(), start_after.map(str::to_owned));
                    self.blocking(move |root, _| {
                        list(root, &prefix, start_after.as_deref(), max_keys)
                    })
                },
                tokio::time::sleep,
            )
            .await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(retry::with_retry(
            self.retry,
            move || {
                let key = key.to_owned();
                self.blocking(move |root, now| delete(root, &key, now))
            },
            tokio::time::sleep,
        ))
    }

    fn check_expiry<'a>(&'a self, days: &'a [u32]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { super::check_all_days(days) })
    }
}
