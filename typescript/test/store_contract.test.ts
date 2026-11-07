// The store contract of I-26, run against every adapter of this host; on S3 only when
// `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`).
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { ConfigError, Credentials } from "../src/mod.ts";
import { STORE_RETRY } from "../src/settings.ts";
import { FsStore } from "../src/stores/fs.ts";
import { MemoryStore } from "../src/stores/memory.ts";
import { openStore } from "../src/stores/open.ts";
import { type Store, StoreFailure } from "../src/stores/store.ts";
import { eachAdapter, tempDir } from "./helpers.ts";

const bytes = (text: string) => new TextEncoder().encode(text);

eachAdapter("create is exclusive", async (store) => {
  assert.equal(await store.create("runs/a/start", bytes("one")), "created");
  assert.equal(await store.create("runs/a/start", bytes("two")), "exists");
  assert.deepEqual(await store.get("runs/a/start"), bytes("one"));
  assert.equal(await store.get("runs/b/start"), undefined);
  assert.equal(await store.get("runs/a"), undefined);
  assert.equal(await store.create("runs/a/empty", new Uint8Array()), "created");
  assert.deepEqual(await store.get("runs/a/empty"), new Uint8Array());
});

eachAdapter("delete semantics", async (store) => {
  await store.delete("claims/missing");
  await store.create("claims/x", bytes("1"));
  await store.delete("claims/x");
  assert.equal(await store.get("claims/x"), undefined);
  await store.delete("claims/x");
  assert.equal(await store.create("claims/x", bytes("2")), "created", "a hint may come back");
  assert.deepEqual(await store.get("claims/x"), bytes("2"));
});

eachAdapter("list in byte order", async (store) => {
  // "q/a/x" sorts after "q/a.b" by full key though its directory "a" sorts before "a.b".
  const keys = ["q/a-b", "q/a.b", "q/a/x", "q/a0", "q/9", "q/b/c/d", "q/é", "qq/a", "p/a", "q0"];
  for (const k of keys) await store.create(k, bytes(k));
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

eachAdapter("list refuses wait", async (store) => {
  assert.equal(store.capabilities.wait, false);
  await assert.rejects(store.list("q/", { maxKeys: 10, wait: { seconds: 1 } }), StoreFailure);
});

eachAdapter("concurrent creates of one key", async (store) => {
  const outcomes = await Promise.all(
    Array.from({ length: 32 }, (_, i) => store.create("runs/r/log/0", bytes(`writer ${i}`))),
  );
  const winners = outcomes.flatMap((o, i) => (o === "created" ? [i] : []));
  assert.equal(winners.length, 1, `exactly one create wins: ${winners}`);
  assert.deepEqual(await store.get("runs/r/log/0"), bytes(`writer ${winners[0]}`));
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
        () => store.create(key, bytes("x")),
        () => store.get(key),
        () => store.delete(key),
      ]) {
        await assert.rejects(op, StoreFailure, key);
      }
    }
    await assert.rejects(store.list("../", { maxKeys: 10 }), StoreFailure);
    assert.equal(await store.create(`a/${"x".repeat(255)}`, bytes("x")), "created");
    assert.equal(
      await store.create("a/.tmp", bytes("x")),
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
    await first.create("runs/a/start", bytes("one"));
    writeFileSync(join(dir, ".tmp", "leftover"), "x");
    writeFileSync(join(dir, ".DS_Store"), "x");
    const second = new FsStore(dir, STORE_RETRY);
    assert.deepEqual(await second.get("runs/a/start"), bytes("one"));
    assert.deepEqual((await second.list("", { maxKeys: 10 })).keys, ["runs/a/start"]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("memory namespaces are shared by name only", async () => {
  const a = new MemoryStore("shared-ns");
  const again = new MemoryStore("shared-ns");
  const other = new MemoryStore("other-ns");
  await a.create("k", bytes("v"));
  assert.deepEqual(await again.get("k"), bytes("v"));
  assert.equal(await other.get("k"), undefined);
});

test("URLs name the adapter", async () => {
  const dir = tempDir();
  try {
    openStore("memory://url-test", undefined);
    const path = join(dir, "ns");
    const file: Store = openStore(`file:${path}`, undefined);
    await file.create("k", bytes("v"));
    const again = openStore(`file://${path}`, undefined);
    assert.deepEqual(await again.get("k"), bytes("v"));
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
      assert.throws(() => openStore(url, undefined), ConfigError, url);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("credentials go only where a store takes them", () => {
  const explicit = new Credentials({ accessKeyId: "id", secretAccessKey: "secret" });
  assert.ok(!JSON.stringify(explicit).includes("secret"));
  assert.throws(() => openStore("memory://c", explicit), ConfigError);
  assert.throws(() => openStore(`file:${tempDir()}`, "env"), ConfigError);
  const s3 = "s3+http://127.0.0.1:1/b/ns?region=r";
  assert.throws(() => openStore(s3, undefined), ConfigError);
  assert.throws(() => openStore(s3, "environment" as "env"), ConfigError);
  assert.throws(() => new Credentials({ accessKeyId: "id", secretAccessKey: "" }), ConfigError);
  assert.equal(openStore(s3, explicit).capabilities.wait, false);
  assert.equal(openStore("h3://127.0.0.1:1/b/ns?region=r", explicit).capabilities.wait, true);
  const saved = { id: process.env.AWS_ACCESS_KEY_ID, secret: process.env.AWS_SECRET_ACCESS_KEY };
  try {
    process.env.AWS_ACCESS_KEY_ID = "id";
    delete process.env.AWS_SECRET_ACCESS_KEY;
    assert.throws(() => openStore(s3, "env"), /AWS_SECRET_ACCESS_KEY/);
    process.env.AWS_SECRET_ACCESS_KEY = "";
    assert.throws(() => openStore(s3, "env"), /AWS_SECRET_ACCESS_KEY/);
    process.env.AWS_SECRET_ACCESS_KEY = "secret";
    openStore(s3, "env");
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
