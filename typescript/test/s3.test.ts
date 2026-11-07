// What the S3 adapter does beyond the shared contract; all but URL parsing, signing and the mock h3
// server run only when `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`).
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ConfigError,
  Credentials,
  LayoutError,
  StoreError,
  Substantial,
  workflow,
} from "../src/mod.ts";
import { STORE_RETRY } from "../src/settings.ts";
import { openStore } from "../src/stores/open.ts";
import { parseS3Url, S3Store } from "../src/stores/s3.ts";
import { sign } from "../src/stores/sigv4.ts";
import { StoreFailure } from "../src/stores/store.ts";
import { LONG, NO_S3, s3Url, script } from "./helpers.ts";

test("URLs need every part", () => {
  assert.deepEqual(parseS3Url("h3+http://127.0.0.1:9/b/ns/sub?region=r"), {
    endpoint: "http://127.0.0.1:9",
    host: "127.0.0.1:9",
    bucket: "b",
    namespace: "ns/sub",
    region: "r",
    wait: true,
  });
  const aws = parseS3Url("s3://s3.eu-central-1.amazonaws.com/b/ns?region=eu-central-1");
  assert.equal(aws.endpoint, "https://s3.eu-central-1.amazonaws.com");
  assert.equal(aws.wait, false);
  assert.equal(parseS3Url("s3+http://[::1]:9000/b/ns?region=r").endpoint, "http://[::1]:9000");
  assert.equal(parseS3Url("s3://h:443/b/ns?region=r").host, "h", "the default port is implied");
  assert.equal(parseS3Url("h3://h/b/ns?region=r").wait, true);
  const refused = [
    "s3://h/b/ns",
    "s3://h/b/ns?region=",
    "s3://h/b/ns?region=r&region=s",
    "s3://h/b/ns?region=r&endpoint=http://h",
    "s3://h/b/ns?region=r&wait=true",
    "s3://h/b?region=r",
    "s3://h/b/?region=r",
    "s3://h/?region=r",
    "s3://h?region=r",
    "s3://h//ns?region=r",
    "s3://h/b/ns/?region=r",
    "s3://h/b/a//c?region=r",
    "s3:///b/ns?region=r",
    "s3://k:s@h/b/ns?region=r",
    "s3://h/b/ns?region=r#x",
    "s3://h:port/b/ns?region=r",
    "s3+https://h/b/ns?region=r",
    "gs://h/b/ns?region=r",
  ];
  for (const url of refused) assert.throws(() => parseS3Url(url), ConfigError, url);
});

test("the signature matches the AWS example", async () => {
  // The GET Object example of the S3 SigV4 documentation, with its credentials and time.
  const credentials = new Credentials({
    accessKeyId: "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  });
  const headers = await sign({
    method: "GET",
    host: "examplebucket.s3.amazonaws.com",
    path: "/test.txt",
    query: "",
    headers: { range: "bytes=0-9" },
    payload: new Uint8Array(),
    credentials,
    region: "us-east-1",
    at: new Date(Date.UTC(2013, 4, 24)),
  });
  const expected = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
  assert.ok(headers.authorization?.endsWith(`Signature=${expected}`), headers.authorization);
});

const live = { skip: NO_S3 };

test("create and open run the probe", live, async () => {
  const url = s3Url();
  await assert.rejects(Substantial.open(url, { credentials: "env" }), LayoutError);
  const created = await Substantial.create(url, { partitions: 2, credentials: "env" });
  assert.equal(created.partitions, 2);
  const opened = await Substantial.open(url, { credentials: "env" });
  assert.equal(opened.partitions, 2);
  await created.close();
  await opened.close();
  const store = openStore(url, "env");
  assert.deepEqual(
    (await store.list("probe/", { maxKeys: 10 })).keys,
    ["probe/exclusive"],
    "one probe key, kept",
  );
  await store.close();
});

test("wrong credentials are a store error", live, async () => {
  const wrong = new Credentials({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "",
    secretAccessKey: "not-the-secret",
  });
  await assert.rejects(
    Substantial.create(s3Url(), { partitions: 1, credentials: wrong }),
    (e) =>
      e instanceof StoreError &&
      e.message.includes("HTTP 403 SignatureDoesNotMatch: ") &&
      !e.message.includes("<"),
  );
});

test("a missing bucket is a store error, not a missing key", live, async () => {
  const url = s3Url();
  const missing = url.replace(/^([^:]+:\/\/[^/]+)\/[^/]+/, "$1/substantial-no-such-bucket");
  const store = openStore(missing, "env");
  await assert.rejects(store.get("runs/a/start"), StoreFailure);
  await assert.rejects(store.delete("runs/a/start"), StoreFailure);
  await store.close();
});

test("listings go past one page", live, async () => {
  const store = openStore(s3Url(), "env");
  const keys = Array.from({ length: 1005 }, (_, i) => `due/${String(i).padStart(5, "0")}`);
  for (let i = 0; i < keys.length; i += 32) {
    const batch = keys.slice(i, i + 32).map((k) => store.create(k, new Uint8Array()));
    assert.deepEqual(new Set(await Promise.all(batch)), new Set(["created"]));
  }
  assert.deepEqual((await store.list("due/", { maxKeys: 2000 })).keys, keys);
  const after = await store.list("due/", { startAfter: "due/00002", maxKeys: 1001 });
  assert.deepEqual(after.keys, keys.slice(3, 1004));
  assert.deepEqual((await store.list("due/", { maxKeys: 0 })).keys, []);
  await store.close();
});

test("h3 needs the listing etag of a waiting listing", live, async () => {
  const store = openStore(s3Url("h3"), "env");
  assert.equal(store.capabilities.wait, true);
  await store.create("q/a", new TextEncoder().encode("a"));
  assert.deepEqual((await store.list("q/", { maxKeys: 10 })).keys, ["q/a"]);
  await assert.rejects(
    store.list("q/", { maxKeys: 10, wait: { seconds: 1 } }),
    /x-substantial-listing-etag/,
  );
  await store.close();
});

test("waiting listings send and answer the wait headers", async () => {
  const seen: Request[] = [];
  const answer = async (input: string | URL | Request, init?: RequestInit) => {
    seen.push(new Request(input, init));
    const body =
      "<ListBucketResult><IsTruncated>false</IsTruncated>" +
      "<Contents><Key>ns/q/a&amp;b</Key></Contents></ListBucketResult>";
    return new Response(body, { status: 200, headers: { "x-substantial-listing-etag": "e2" } });
  };
  const credentials = new Credentials({ accessKeyId: "id", secretAccessKey: "s" });
  const h3 = parseS3Url("h3+http://h:1/b/ns?region=r");
  const store = new S3Store(h3, credentials, STORE_RETRY, answer as typeof fetch);
  const listing = await store.list("q/", { maxKeys: 10, wait: { seconds: 20, etag: "e1" } });
  assert.deepEqual(listing, { keys: ["q/a&b"], etag: "e2" });
  const headers = (seen[0] as Request).headers;
  assert.deepEqual(
    [headers.get("x-substantial-wait-seconds"), headers.get("x-substantial-wait-etag")],
    ["20", "e1"],
  );
  const signed = /SignedHeaders=([^,]+)/.exec(headers.get("authorization") ?? "")?.[1]?.split(";");
  assert.ok(
    signed?.includes("x-substantial-wait-seconds") && signed.includes("x-substantial-wait-etag"),
  );
  const first = await store.list("q/", { maxKeys: 10, wait: { seconds: 5 } });
  assert.equal((seen[1] as Request).headers.get("x-substantial-wait-etag"), null);
  assert.equal(first.etag, "e2");
  for (const wait of [{ seconds: 21 }, { seconds: 5, etag: "e\n1" }]) {
    await assert.rejects(store.list("q/", { maxKeys: 10, wait }), StoreFailure);
  }
  await assert.rejects(store.list("q/", { maxKeys: 1001, wait: { seconds: 5 } }), StoreFailure);
  assert.equal(seen.length, 2, "refused before any request");
  const s3 = new S3Store(
    parseS3Url("s3+http://h:1/b/ns?region=r"),
    credentials,
    STORE_RETRY,
    answer as typeof fetch,
  );
  await assert.rejects(s3.list("q/", { maxKeys: 10, wait: { seconds: 5 } }), StoreFailure);
  await store.close();
  await assert.rejects(store.get("k"), /closed/, "a closed store refuses every operation");
});

test("a credentials provider near expiry is called before each request", live, async () => {
  let calls = 0;
  const provider = async () => {
    calls += 1;
    return {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "",
      expiresAt: new Date(Date.now() + 60_000),
    };
  };
  const answer = workflow("answer", async () => 42);
  const sub = await Substantial.create(s3Url(), { partitions: 1, credentials: provider });
  const opened = calls;
  assert.ok(opened >= 1, "called at open");
  {
    await using _ = script(sub, [answer]);
    assert.equal(await (await sub.start(answer)).result({ timeout: LONG }), 42);
  }
  assert.ok(calls > opened, `called again: ${calls}`);
  await sub.close();
});
