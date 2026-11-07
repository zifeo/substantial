use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::names::{TMP, check_key, is_component};
use super::retry::{self, Fault};
use super::{Capabilities, Create, ListWait, Listing, Store, StoreFuture};
use crate::error::{Error, Result};
use crate::settings::StoreRetry;

/// `file:<path>`: the directory is the namespace, one file per key. For development, CI and one
/// machine.
///
/// Keys never differ only by case (I-17), so a file system that folds case holds them as they are.
#[derive(Clone, Debug)]
pub struct FsStore {
    root: PathBuf,
    retry: StoreRetry,
}

impl FsStore {
    pub fn open(path: impl AsRef<Path>, retry: StoreRetry) -> Result<Self> {
        retry.validate()?;
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(root.join(TMP))
            .map_err(|e| Error::StoreError(format!("cannot prepare {}: {e}", root.display())))?;
        Ok(Self { root, retry })
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Path) -> Result<T, Fault> + Send + 'static,
    ) -> Result<T, Fault> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || f(&root))
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

/// Creates the missing directories between `root` and `dir`, each fsynced in its parent.
fn ensure_dirs(root: &Path, dir: &Path) -> Result<(), Fault> {
    let mut missing = Vec::new();
    let mut at = dir;
    while at != root && !at.is_dir() {
        missing.push(at);
        at = at.parent().expect("a key directory lies under the root");
    }
    for dir in missing.into_iter().rev() {
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) if e.kind() == ErrorKind::NotADirectory => {
                return Err(Fault::Permanent(format!(
                    "{} lies under another key: {e}",
                    dir.display()
                )));
            }
            Err(e) => return Err(transient("create directory", dir)(e)),
        }
        let parent = dir.parent().expect("under the root");
        sync_dir(parent).map_err(transient("sync directory", parent))?;
    }
    Ok(())
}

fn create(root: &Path, key: &str, bytes: &[u8]) -> Result<Create, Fault> {
    check_key(key)?;
    let path = root.join(key);
    let tmp = root.join(TMP).join(hex::encode(rand::random::<[u8; 16]>()));
    let written = File::create_new(&tmp).and_then(|mut f| {
        f.write_all(bytes)?;
        f.sync_all()
    });
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        return Err(transient("write", &tmp)(e));
    }
    let parent = path.parent().expect("a key has a parent");
    let linked = ensure_dirs(root, parent).and_then(|()| match fs::hard_link(&tmp, &path) {
        Ok(()) => sync_dir(parent)
            .map(|()| Create::Created)
            .map_err(|e| Fault::Ambiguous(format!("sync directory {}: {e}", parent.display()))),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(Create::Exists),
        Err(e) if e.kind() == ErrorKind::NotADirectory => Err(Fault::Permanent(format!(
            "key {key:?} lies under another key: {e}"
        ))),
        // On NFS a link that succeeded may still report an error.
        Err(e) => Err(Fault::Ambiguous(format!("link {}: {e}", path.display()))),
    });
    let _ = fs::remove_file(&tmp);
    linked
}

fn get(root: &Path, key: &str) -> Result<Option<Bytes>, Fault> {
    check_key(key)?;
    let path = root.join(key);
    match fs::read(&path) {
        Ok(bytes) => Ok(Some(bytes.into())),
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(transient("read", &path)(e)),
    }
}

fn delete(root: &Path, key: &str) -> Result<(), Fault> {
    check_key(key)?;
    let path = root.join(key);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory
            ) =>
        {
            Ok(())
        }
        Err(e) => Err(transient("delete", &path)(e)),
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
        let key = format!("{base}{name}");
        let kind = entry
            .file_type()
            .map_err(transient("stat", &entry.path()))?;
        if kind.is_dir() {
            let sub = format!("{key}/");
            if sub.starts_with(prefix) || prefix.starts_with(&sub) {
                walk(&entry.path(), &sub, prefix, keys)?;
            }
        } else if key.starts_with(prefix) {
            keys.push(key);
        }
    }
    Ok(())
}

impl Store for FsStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities { wait: false }
    }

    fn create<'a>(&'a self, key: &'a str, bytes: Bytes) -> StoreFuture<'a, Create> {
        Box::pin(async move {
            let attempt = || {
                let (key, bytes) = (key.to_owned(), bytes.clone());
                self.blocking(move |root| create(root, &key, &bytes))
            };
            let read = || {
                let key = key.to_owned();
                self.blocking(move |root| get(root, &key))
            };
            retry::create(self.retry, &bytes, attempt, read).await
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> StoreFuture<'a, Option<Bytes>> {
        Box::pin(retry::with_retry(self.retry, move || {
            let key = key.to_owned();
            self.blocking(move |root| get(root, &key))
        }))
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
            retry::with_retry(self.retry, || {
                let (prefix, start_after) = (prefix.to_owned(), start_after.map(str::to_owned));
                self.blocking(move |root| list(root, &prefix, start_after.as_deref(), max_keys))
            })
            .await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> StoreFuture<'a, ()> {
        Box::pin(retry::with_retry(self.retry, move || {
            let key = key.to_owned();
            self.blocking(move |root| delete(root, &key))
        }))
    }
}
