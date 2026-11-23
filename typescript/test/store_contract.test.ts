// The store contract of I-26, run against every adapter of this host; on S3 and Redis only when
// their variables are set (see `helpers.ts`).
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { ConfigError, Credentials } from "../src/mod.ts";
import { STORE_RETRY } from "../src/settings.ts";
import { FsStore } from "../src/stores/fs.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { openStore } from "../src/stores/open.ts";
import { parseS3Url } from "../src/stores/s3.ts";
import { type Create, DAY_MS, type Store, StoreFailure } from "../src/stores/store.ts";
import { eachAdapter, kind, S3_VERSIONED_URL, s3Url, tempDir, token, value } from "./helpers.ts";

const bytes = (text: string) => new TextEncoder().encode(text);

eachAdapter("create is exclusive", async (store) => {
  assert.equal(kind(await store.create("runs/a/start", bytes("one"), undefined)), "created");
  assert.equal(await store.create("runs/a/start", bytes("two"), undefined), "exists");
  assert.deepEqual(await value(store, "runs/a/start"), bytes("one"));
  assert.equal(await value(store, "runs/b/start"), undefined);
  assert.equal(await value(store, "runs/a"), undefined);
  assert.equal(kind(await store.create("runs/a/empty", new Uint8Array(), undefined)), "created");
  assert.deepEqual(await value(store, "runs/a/empty"), new Uint8Array());
});

eachAdapter("delete semantics", async (store) => {
  await store.delete("claims/missing");
  await store.create("claims/x", bytes("1"), undefined);
  await store.delete("claims/x");
  assert.equal(await value(store, "claims/x"), undefined);
  await store.delete("claims/x");
  assert.equal(
    kind(await store.create("claims/x", bytes("2"), undefined)),
    "created",
    "a hint may come back",
  );
  assert.deepEqual(await value(store, "claims/x"), bytes("2"));
});

eachAdapter("list in byte order", async (store) => {
  // "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
  const keys = ["q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0"];
  for (const k of keys) await store.create(k, bytes(k), undefined);
  const ls = async (prefix: string, startAfter?: string, maxKeys = 100) => {
    const listing = await store.list(prefix, { maxKeys, ...(startAfter ? { startAfter } : {}) });
    assert.equal(listing.etag, undefined);
    return listing.keys;
  };
  const every = ["q/9", "q/a-b", "q/a.b", "q/a/x", "q/a0", "q/b/c/d", "q/é"];
  assert.deepEqual(await ls("q/"), every);
  assert.deepEqual(await ls("q/", undefined, 3), every.slice(0, 3));
  assert.deepEqual(await ls("q/", "q/a.b", 3), every.slice(3, 6));
  assert.deepEqual(await ls("q/", "q/a/"), every.slice(3), "startAfter need not be a key");
  assert.deepEqual(await ls("q/", "a"), every, "startAfter before the prefix");
  assert.deepEqual(await ls("q/", "r"), []);
  assert.deepEqual(await ls("q/a"), every.slice(1, 5));
  assert.deepEqual(await ls("q/a/"), ["q/a/x"]);
  assert.deepEqual(await ls("q"), [...every, "q0", "qq/a"]);
  assert.deepEqual(await ls("q/b/"), ["q/b/c/d"]);
  assert.deepEqual(await ls("none/"), []);
  assert.deepEqual(await ls("", "q0"), ["qq/a"]);
  assert.deepEqual(await ls("", undefined, 1), ["p/a"]);
});

eachAdapter(
  "list refuses wait",
  async (store) => {
    assert.ok(!store.capabilities.wait);
    await assert.rejects(store.list("q/", { maxKeys: 10, wait: { seconds: 1 } }), StoreFailure);
  },
  ["memory", "file", "s3"],
);

eachAdapter("concurrent creates of one key", async (store) => {
  const outcomes = await Promise.all(
    Array.from({ length: 32 }, (_, i) =>
      store.create("runs/r/log/0", bytes(`writer ${i}`), undefined),
    ),
  );
  const winners = outcomes.flatMap((o, i) => (o === "exists" ? [] : [i]));
  assert.equal(winners.length, 1, `exactly one create wins: ${winners}`);
  assert.deepEqual(await value(store, "runs/r/log/0"), bytes(`writer ${winners[0]}`));
});

test("the file store checks key components", async () => {
  const dir = tempDir();
  try {
    const store = new FsStore(join(dir, "ns"), STORE_RETRY);
    const long = "x".repeat(256);
    const keys = [
      "a/./b",
      "a/../b",
      "a//b",
      "/a",
      "a/",
      ".tmp/x",
      "a/\0",
      `a/${long}`,
      "runs/Run/start",
    ];
    for (const key of keys) {
      for (const op of [
        () => store.create(key, bytes("x"), undefined),
        () => store.get(key),
        () => store.delete(key),
      ]) {
        await assert.rejects(op, StoreFailure, key);
      }
    }
    await assert.rejects(store.list("../", { maxKeys: 10 }), StoreFailure);
    assert.equal(
      kind(await store.create(`a/${"x".repeat(255)}`, bytes("x"), undefined)),
      "created",
    );
    assert.equal(
      kind(await store.create("a/.tmp", bytes("x"), undefined)),
      "created",
      ".tmp is reserved at the root only",
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file keys survive a reopen and temporaries stay hidden", async () => {
  const dir = tempDir();
  try {
    const first = new FsStore(dir, STORE_RETRY);
    await first.create("runs/a/start", bytes("one"), undefined);
    writeFileSync(join(dir, ".tmp", "leftover"), "x");
    writeFileSync(join(dir, ".DS_Store"), "x");
    const second = new FsStore(dir, STORE_RETRY);
    assert.deepEqual(await value(second, "runs/a/start"), bytes("one"));
    assert.deepEqual((await second.list("", { maxKeys: 10 })).keys, ["runs/a/start"]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("memory namespaces are shared by name only", async () => {
  const a = new MemoryStore("shared-ns");
  const again = new MemoryStore("shared-ns");
  const other = new MemoryStore("other-ns");
  await a.create("k", bytes("v"), undefined);
  assert.deepEqual(await value(again, "k"), bytes("v"));
  assert.equal(await value(other, "k"), undefined);
});

test("URLs name the adapter", async () => {
  const dir = tempDir();
  try {
    await openStore("memory://url-test", undefined);
    const path = join(dir, "ns");
    const file: Store = await openStore(`file:${path}`, undefined);
    await file.create("k", bytes("v"), undefined);
    const again = await openStore(`file://${path}`, undefined);
    assert.deepEqual(await value(again, "k"), bytes("v"));
    const refused = [
      "memory://",
      "memory:x",
      "memory://x?wait=true",
      "file:",
      "s3://h/bucket/ns",
      "s3://h/bucket/ns?region=r&endpoint=x",
      "redis://h:1/0",
      "nope",
    ];
    for (const url of refused) {
      await assert.rejects(openStore(url, undefined), ConfigError, url);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("credentials go only where a store takes them", async () => {
  const explicit = new Credentials({ accessKeyId: "id", secretAccessKey: "secret" });
  assert.ok(!JSON.stringify(explicit).includes("secret"));
  await assert.rejects(openStore("memory://c", explicit), ConfigError);
  await assert.rejects(openStore(`file:${tempDir()}`, "env"), ConfigError);
  const s3 = "s3+http://127.0.0.1:1/b/ns?region=r";
  await assert.rejects(openStore(s3, undefined), ConfigError);
  await assert.rejects(openStore(s3, "environment" as "env"), ConfigError);
  assert.throws(() => new Credentials({ accessKeyId: "id", secretAccessKey: "" }), ConfigError);
  assert.equal((await openStore(s3, explicit)).capabilities.wait, false);
  const h3 = await openStore("h3://127.0.0.1:1/b/ns?region=r", explicit);
  assert.equal(h3.capabilities.wait, true);
  const saved = { id: process.env.AWS_ACCESS_KEY_ID, secret: process.env.AWS_SECRET_ACCESS_KEY };
  try {
    process.env.AWS_ACCESS_KEY_ID = "id";
    delete process.env.AWS_SECRET_ACCESS_KEY;
    await assert.rejects(openStore(s3, "env"), /AWS_SECRET_ACCESS_KEY/);
    process.env.AWS_SECRET_ACCESS_KEY = "";
    await assert.rejects(openStore(s3, "env"), /AWS_SECRET_ACCESS_KEY/);
    process.env.AWS_SECRET_ACCESS_KEY = "secret";
    await openStore(s3, "env");
  } finally {
    for (const [name, value] of [
      ["AWS_ACCESS_KEY_ID", saved.id],
      ["AWS_SECRET_ACCESS_KEY", saved.secret],
    ] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});

async function etag(store: Store, key: string): Promise<string> {
  const found = await store.get(key);
  assert.ok(found !== undefined, key);
  return found.etag;
}

eachAdapter("compare-and-set", async (store) => {
  // § kkad6aqxp@cjyda
  await store.create("runs/c", bytes("one"), undefined);
  const first = await etag(store, "runs/c");
  // An ETag no store makes, which RustFS takes as well formed.
  const wrong = "substantial-probe-mismatch";
  assert.equal(await store.cas("runs/c", bytes("two"), wrong, undefined), "mismatch");
  assert.deepEqual(await value(store, "runs/c"), bytes("one"));
  const written = await store.cas("runs/c", bytes("two"), first, undefined);
  assert.ok(written !== "mismatch", "the ETag read names the value");
  assert.notEqual(written.etag, first, "the bytes changed, so did the ETag");
  assert.equal(await etag(store, "runs/c"), written.etag);
  assert.deepEqual(await value(store, "runs/c"), bytes("two"));
  assert.equal(await store.cas("runs/c", bytes("three"), first, undefined), "mismatch");
  assert.deepEqual(await value(store, "runs/c"), bytes("two"));
  // § one kkad6aqxp.a@vjad1
  assert.equal(await store.cas("runs/none", bytes("x"), first, undefined), "mismatch");
  assert.equal(await value(store, "runs/none"), undefined);
});

eachAdapter("compare-and-set after a delete", async (store) => {
  await store.create("runs/d", bytes("one"), undefined);
  const read = await etag(store, "runs/d");
  await store.delete("runs/d");
  // § one kkad6aqxp.a@035ss
  assert.equal(await store.cas("runs/d", bytes("two"), read, undefined), "mismatch");
  assert.equal(await value(store, "runs/d"), undefined);
});

/** The probe's last step: a compare-and-set naming the ETag of a deleted key, which must answer
 * mismatch; true when the key came back instead. */
async function deletedKeyComesBack(store: Store): Promise<boolean> {
  await store.create("probe/deleted", bytes(""), undefined);
  const read = await etag(store, "probe/deleted");
  await store.delete("probe/deleted");
  const back = await store.cas("probe/deleted", bytes(""), read, undefined);
  if (back !== "mismatch") await store.delete("probe/deleted");
  return back !== "mismatch";
}

/** RustFS 1.0.1 on a versioned bucket compares `If-Match` with the newest version holding data,
 * past the delete marker, so a compare-and-set brings a deleted key back, where AWS
 * answers 404: it fails the probe, which refuses the store at open. The only use of the versioned
 * bucket. This fails once RustFS meets the contract. */
test("rustfs versioned fails the probe of a deleted key", async () => {
  const store = await openStore(await s3Url("s3", S3_VERSIONED_URL), "env");
  try {
    assert.ok(
      await deletedKeyComesBack(store),
      "RustFS on a versioned bucket now refuses a compare-and-set naming a deleted key: it meets " +
        "the contract, so the versioned bucket can run the S3 tests again",
    );
  } finally {
    await store.close();
  }
});

eachAdapter("the probe", async (store) => {
  const first = await store.create("probe/exclusive", bytes(""), undefined);
  assert.ok(kind(first) === "created" || first === "exists");
  assert.equal(await store.create("probe/exclusive", bytes(""), undefined), "exists");
  // The ETag the core's probe names, which no store makes.
  const wrong = "substantial-probe-mismatch";
  assert.equal(await store.cas("probe/exclusive", bytes(""), wrong, undefined), "mismatch");
  assert.deepEqual(await value(store, "probe/exclusive"), bytes(""));
  assert.equal(await store.cas("probe/missing", bytes(""), wrong, undefined), "mismatch");
  assert.equal(await deletedKeyComesBack(store), false);
});

eachAdapter("a create answers the ETag a cas names", async (store) => {
  const created = await store.create("runs/n", bytes("one"), undefined);
  assert.ok(created !== "exists");
  assert.equal(created.etag, await etag(store, "runs/n"));
  assert.notEqual(await store.cas("runs/n", bytes("two"), created.etag, undefined), "mismatch");
});

eachAdapter("concurrent compare-and-sets of one key", async (store) => {
  await store.create("runs/h", bytes("start"), undefined);
  const read = await etag(store, "runs/h");
  const outcomes = await Promise.all(
    Array.from({ length: 16 }, (_, i) =>
      store.cas("runs/h", bytes(`writer ${i}`), read, undefined),
    ),
  );
  const winners = outcomes.flatMap((o, i) => (o === "mismatch" ? [] : [i]));
  assert.equal(winners.length, 1, `exactly one writer of a read wins: ${winners}`);
  assert.deepEqual(await value(store, "runs/h"), bytes(`writer ${winners[0]}`));
});

eachAdapter("expiries in range", async (store, adapter) => {
  // § r4rsq6spn.c@fsxqd
  for (const days of [0, 36_501, 1.5]) {
    await assert.rejects(store.create("runs/x", bytes("x"), days), StoreFailure, `${days}`);
    await assert.rejects(store.checkExpiry([days]), ConfigError, `${days}`);
  }
  await store.create("runs/x", bytes("x"), 1);
  assert.deepEqual(await value(store, "runs/x"), bytes("x"));
  const read = await etag(store, "runs/x");
  await assert.rejects(store.cas("runs/x", bytes("y"), read, 0), StoreFailure);
  if (adapter === "file") return;
  assert.notEqual(await store.cas("runs/x", bytes("y"), read, 36_500), "mismatch");
  assert.deepEqual(await value(store, "runs/x"), bytes("y"));
});

/** The file of version `n` of `key` in the file store at `root`. */
function versionFile(root: string, key: string, n: number): string {
  return join(root, ...key.split("/"), `.${String(n).padStart(20, "0")}`);
}

/** The header's expiry of the version file at `path`. */
function headerOf(path: string): bigint {
  return readFileSync(path).readBigInt64BE(0);
}

/** The ETag of version `n` of the incarnation `etag` names. */
function withVersion(etag: string, n: number): string {
  return `${etag.split(".")[0]}.${n}`;
}

/** Every path under `root` but the temporary files, as the store leaves it. */
function left(root: string): string[] {
  return (readdirSync(root, { recursive: true }) as string[])
    .filter((p) => !p.startsWith(".tmp"))
    .sort();
}

function created(outcome: Create): string {
  assert.notEqual(outcome, "exists");
  return (outcome as { etag: string }).etag;
}

test("the file store keeps a key as numbered versions", async () => {
  const dir = tempDir();
  try {
    const store = new FsStore(dir, STORE_RETRY);
    await assert.rejects(
      store.create("a/.00000000000000000001", bytes("x"), undefined),
      StoreFailure,
    );
    const first = created(await store.create("runs/a", bytes("x"), undefined));
    assert.ok(first.endsWith(".0") && first.length === 34, first);
    assert.deepEqual(await store.cas("runs/a", bytes("y"), first, undefined), {
      etag: withVersion(first, 1),
    });
    assert.deepEqual(
      new Uint8Array(readFileSync(versionFile(dir, "runs/a", 1)).subarray(24)),
      bytes("y"),
    );
    assert.ok(existsSync(versionFile(dir, "runs/a", 0)), "superseded just now");
    assert.deepEqual((await store.list("", { maxKeys: 10 })).keys, ["runs/a"]);
    // A key may hold keys under it, as layout 2's do.
    await store.create("runs/a/start", bytes("s"), undefined);
    assert.deepEqual((await store.list("runs/", { maxKeys: 10 })).keys, ["runs/a", "runs/a/start"]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("a file store delete takes the version a late write names", async () => {
  const dir = tempDir();
  try {
    const store = new FsStore(dir, STORE_RETRY);
    await store.create("k", bytes("one"), undefined);
    const read = await etag(store, "k");
    await store.delete("k");
    assert.equal(headerOf(versionFile(dir, "k", 1)), -1n);
    assert.equal(await store.cas("k", bytes("late"), read, undefined), "mismatch");
    assert.equal(await value(store, "k"), undefined);
    assert.deepEqual((await store.list("", { maxKeys: 10 })).keys, []);
    // A write that won the version first stands, and the delete buries it next.
    const again = created(await store.create("k", bytes("two"), undefined));
    assert.ok(again.endsWith(".2") && again !== withVersion(read, 2), "a new incarnation");
    const won = await store.cas("k", bytes("three"), again, undefined);
    await store.delete("k");
    assert.deepEqual(won, { etag: withVersion(again, 3) });
    assert.equal(await store.cas("k", bytes("late"), withVersion(again, 3), undefined), "mismatch");
    assert.equal(await value(store, "k"), undefined);
    // A write whose version a removal freed below the latest is refused.
    await store.create("g", bytes("0"), undefined);
    for (let n = 0; n < 3; n += 1) {
      await store.cas("g", bytes(String(n + 1)), await etag(store, "g"), undefined);
    }
    const current = await etag(store, "g");
    for (let n = 0; n < 3; n += 1) rmSync(versionFile(dir, "g", n));
    assert.equal(
      await store.cas("g", bytes("stale"), withVersion(current, 1), undefined),
      "mismatch",
    );
    assert.ok(!existsSync(versionFile(dir, "g", 2)), "no version written below the latest");
    assert.deepEqual(await value(store, "g"), bytes("3"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("the file store refuses an ETag of a removed incarnation", async () => {
  const dir = tempDir();
  try {
    let now = T0;
    const store = new FsStore(dir, STORE_RETRY, () => now);
    await store.create("k", bytes("old"), undefined);
    const stale = await etag(store, "k");
    await store.delete("k");
    // The fake clock is past the real time of the tombstone's link: old enough to reclaim.
    now = Date.now() + 100 * DAY_MS;
    await store.sweep();
    assert.ok(!existsSync(join(dir, "k")), "the key's directory reclaimed");
    const fresh = created(await store.create("k", bytes("new"), undefined));
    assert.ok(fresh.endsWith(".0"), `numbered from 0 again: ${fresh}`);
    assert.notEqual(fresh, stale, "a new incarnation");
    assert.equal(await store.cas("k", bytes("stale"), stale, undefined), "mismatch");
    assert.deepEqual(await value(store, "k"), bytes("new"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("the file store reclaims every deleted key and its empty parents", async () => {
  const dir = tempDir();
  try {
    let now = T0;
    const store = new FsStore(dir, STORE_RETRY, () => now);
    await store.create("runs/a/start", bytes("kept"), undefined);
    for (let cycle = 0; cycle < 3; cycle += 1) {
      for (let i = 0; i < 10; i += 1) {
        const key = `queues/w/due/${String(i).padStart(3, "0")}~${cycle}`;
        await store.create(key, new Uint8Array(), 1);
        await store.delete(key);
        await store.create("probe/deleted", new Uint8Array(), undefined);
        await store.delete("probe/deleted");
      }
    }
    now = Date.now() + 100 * DAY_MS;
    await store.sweep();
    assert.deepEqual(left(dir), [
      "runs",
      "runs/a",
      "runs/a/start",
      "runs/a/start/.00000000000000000000",
    ]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("a file store sweep racing creates loses none", async () => {
  const dir = tempDir();
  try {
    // Every tombstone is old enough to reclaim at once, so sweeps remove directories while the
    // creates and deletes run.
    const store = new FsStore(dir, STORE_RETRY, () => Date.now() + 100 * DAY_MS);
    let stop = false;
    let sweeps = 0;
    const sweeper = (async () => {
      while (!stop) {
        await store.sweep();
        sweeps += 1;
      }
    })();
    const worker = async (w: number) => {
      const key = `q/${w}/k`;
      for (let i = 0; i < 20; i += 1) {
        const mine = bytes(`${w}-${i}`);
        assert.notEqual(await store.create(key, mine, undefined), "exists", "free again");
        assert.deepEqual(await value(store, key), mine, "kept until deleted");
        await store.delete(key);
        assert.equal(await value(store, key), undefined);
      }
    };
    try {
      await Promise.all(Array.from({ length: 8 }, (_, w) => worker(w)));
    } finally {
      stop = true;
      await sweeper;
    }
    assert.ok(sweeps > 0);
    await store.sweep();
    assert.deepEqual(left(dir), []);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("file store writers racing a delete never bring the key back", async () => {
  const dir = tempDir();
  try {
    const store = new FsStore(dir, STORE_RETRY);
    await store.create("r", bytes("0"), undefined);
    const writer = async (i: number) => {
      let written = 0;
      for (let read = await store.get("r"); read !== undefined; read = await store.get("r")) {
        const next = bytes(`${i}-${written}`);
        if ((await store.cas("r", next, read.etag, undefined)) !== "mismatch") written += 1;
      }
      return written;
    };
    const writers = Array.from({ length: 8 }, (_, i) => writer(i));
    await new Promise((resolve) => setTimeout(resolve, 50));
    await store.delete("r");
    const total = (await Promise.all(writers)).reduce((a, b) => a + b, 0);
    assert.ok(total > 0, "the writers wrote before the delete");
    assert.equal(await value(store, "r"), undefined, "no write lands after the delete");
    const latest = readdirSync(join(dir, "r")).sort().at(-1) as string;
    assert.equal(headerOf(join(dir, "r", latest)), -1n, "the tombstone is the latest");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

const T0 = 1_795_000_000_000;

test("the memory store expires keys exactly", async () => {
  let now = T0;
  const store = new MemoryStore(`contract-expiry-${token()}`, () => now);
  await store.create("runs/e", bytes("one"), 1);
  await store.create("runs/kept", bytes("kept"), 1);
  // § one r4rsq6spn.a@dt31v
  const kept = await etag(store, "runs/kept");
  assert.notEqual(await store.cas("runs/kept", bytes("kept"), kept, undefined), "mismatch");
  now = T0 + DAY_MS - 1;
  assert.deepEqual(await value(store, "runs/e"), bytes("one"), "kept for the whole day");
  const read = await etag(store, "runs/e");
  now = T0 + DAY_MS;
  assert.equal(await value(store, "runs/e"), undefined, "removed once its day passed");
  assert.equal(await store.cas("runs/e", bytes("two"), read, 1), "mismatch");
  assert.equal(await value(store, "runs/e"), undefined);
  assert.deepEqual(await value(store, "runs/kept"), bytes("kept"), "its expiry cleared");
  assert.equal(kind(await store.create("runs/e", bytes("new"), undefined)), "created");
  now = T0 + 100 * DAY_MS;
  assert.deepEqual(await value(store, "runs/e"), bytes("new"));
});

test("the file store reads the expiry its header holds", async () => {
  const dir = tempDir();
  try {
    let now = T0;
    const store = new FsStore(dir, STORE_RETRY, () => now);
    await store.create("runs/e", bytes("x"), 2);
    const file = readFileSync(versionFile(dir, "runs/e", 0));
    const at = T0 + 2 * DAY_MS;
    assert.equal(file.readBigInt64BE(0), BigInt(at), "the expiry heads the version");
    assert.deepEqual(new Uint8Array(file.subarray(24)), bytes("x"));
    await store.create("runs/kept", bytes("k"), undefined);
    assert.equal(readFileSync(versionFile(dir, "runs/kept", 0)).readBigInt64BE(0), 0n, "no expiry");
    now = at - 1;
    assert.deepEqual(await value(store, "runs/e"), bytes("x"));
    now = at;
    assert.equal(await value(store, "runs/e"), undefined);
    assert.equal(await store.create("runs/kept", bytes("k"), undefined), "exists");
    assert.equal(await store.sweep(), 1, "a tombstone over the expired key");
    assert.equal(headerOf(versionFile(dir, "runs/e", 1)), -1n);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("only S3 checks its expiries", async () => {
  const dir = tempDir();
  try {
    for (const store of [new MemoryStore("contract-check"), new FsStore(dir, STORE_RETRY)]) {
      await store.checkExpiry([1, 7, 36_500]);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

/** A client of the bucket `url` names, beside the adapter, and its location. */
async function rawS3(url: string) {
  const sdk = await import("@aws-sdk/client-s3");
  const location = parseS3Url(url);
  const client = new sdk.S3Client({
    endpoint: location.endpoint,
    region: location.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID as string,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY as string,
    },
  });
  return { sdk, client, location };
}

for (const variable of ["SUBSTANTIAL_S3_URL"]) {
  test(`S3 writes its expiry as a tag [${variable}]`, async () => {
    const url = await s3Url("s3", variable);
    const store = await openStore(url, "env");
    const { sdk, client, location } = await rawS3(url);
    const tags = async (key: string) => {
      const command = new sdk.GetObjectTaggingCommand({
        Bucket: location.bucket,
        Key: `${location.namespace}/${key}`,
      });
      const { TagSet = [] } = await client.send(command);
      return TagSet.map((t) => `${t.Key}=${t.Value}`);
    };
    try {
      // § c4fyc3wyk@1vj6w
      await store.create("runs/t", bytes("one"), 1);
      assert.deepEqual(await tags("runs/t"), ["substantial-retention=1d"]);
      await store.cas("runs/t", bytes("two"), await etag(store, "runs/t"), 30);
      assert.deepEqual(await tags("runs/t"), ["substantial-retention=30d"]);
      await store.cas("runs/t", bytes("three"), await etag(store, "runs/t"), undefined);
      assert.deepEqual(await tags("runs/t"), [], "a write without an expiry clears it");
      await store.create("runs/u", bytes("one"), undefined);
      assert.deepEqual(await tags("runs/u"), []);
    } finally {
      await store.close();
      client.destroy();
    }
  });

  test(`S3 checks a lifecycle rule expires each retention [${variable}]`, async () => {
    const url = await s3Url("s3", variable);
    const store = await openStore(url, "env");
    const elsewhere = await openStore(url.replace("/tests/", "/elsewhere-"), "env");
    try {
      // § cfdckf61n@43abx
      await store.checkExpiry([1, 30, 1]);
      const ns = parseS3Url(url).namespace;
      await assert.rejects(store.checkExpiry([30, 7, 365]), (e: unknown) => {
        assert.ok(e instanceof ConfigError, String(e));
        assert.match(e.message, /substantial-retention=7d, 365d/);
        const rule = {
          Expiration: { Days: 7 },
          Filter: {
            And: { Prefix: `${ns}/`, Tags: [{ Key: "substantial-retention", Value: "7d" }] },
          },
          ID: "substantial-expire-7d",
          Status: "Enabled",
        };
        assert.ok(e.message.includes(JSON.stringify(rule)), e.message);
        assert.ok(!e.message.includes("30d"), e.message);
        return true;
      });
      await assert.rejects(elsewhere.checkExpiry([1]), /substantial-retention=1d/);
    } finally {
      await store.close();
      await elsewhere.close();
    }
  });
}
