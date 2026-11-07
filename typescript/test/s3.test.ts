// What the S3 adapter does beyond the shared contract; all but URL parsing and the fake h3 server
// run only when `SUBSTANTIAL_S3_URL` is set (see `dev/rustfs.compose.yaml`).
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { type AddressInfo, createServer } from "node:net";
import { join } from "node:path";
import { test } from "node:test";
import {
  ConfigError,
  Credentials,
  LayoutError,
  StoreError,
  Substantial,
  workflow,
} from "../src/mod.ts";
import { openStore } from "../src/stores/open.ts";
import { parseS3Url } from "../src/stores/s3.ts";
import { StoreFailure } from "../src/stores/store.ts";
import { FakeH3, signedHeaders } from "./fake_h3.ts";
import { LONG, NO_S3, s3Url, script, tempDir } from "./helpers.ts";

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
  assert.equal(parseS3Url(`s3://h/b/ns?region=${"r".repeat(63)}`).region, "r".repeat(63));
  const refused = [
    "s3://h/b/ns",
    "s3://h/b/ns?region=",
    "s3://h/b/ns?region=r&region=s",
    "s3://h/b/ns?region=us_east",
    "s3://h/b/ns?region=US-EAST-1",
    "s3://h/b/ns?region=-r",
    "s3://h/b/ns?region=r-",
    "s3://h/b/ns?region=us%20east",
    `s3://h/b/ns?region=${"r".repeat(64)}`,
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
    "s3://@h/b/ns?region=r",
    "s3://:@h/b/ns?region=r",
    "s3://h/b/ns?region=r#x",
    "s3://h:port/b/ns?region=r",
    "s3+https://h/b/ns?region=r",
    "gs://h/b/ns?region=r",
  ];
  for (const url of refused) assert.throws(() => parseS3Url(url), ConfigError, url);
});

test("each operation is one request", async () => {
  const h3 = await FakeH3.start();
  const credentials = new Credentials({ accessKeyId: "fake", secretAccessKey: "fake-secret" });
  const store = await openStore(h3.url("ns"), credentials);
  try {
    const a = new TextEncoder().encode("a");
    assert.equal(await store.create("q/a", a), "created");
    assert.equal(await store.create("q/a", new TextEncoder().encode("b")), "exists");
    assert.deepEqual(await store.get("q/a"), a);
    assert.equal(await store.get("q/b"), undefined);
    assert.deepEqual((await store.list("q/", { maxKeys: 10 })).keys, ["q/a"]);
    await store.list("q/", { maxKeys: 10, wait: { seconds: 0 } });
    await store.delete("q/a");
    assert.deepEqual(h3.methods, ["PUT", "PUT", "GET", "GET", "GET", "GET", "DELETE"]);
  } finally {
    await store.close();
    await h3.close();
  }
});

/** A server on 127.0.0.1 answering every request with `answer`, then closing the connection;
 * gives its URL and the method of every request. */
async function rawServer(
  answer: string,
): Promise<{ url: string; methods: string[]; close(): void }> {
  const methods: string[] = [];
  const server = createServer((socket) => {
    let head = "";
    socket.on("data", (chunk) => {
      head += chunk.toString();
      if (!head.includes("\r\n\r\n")) return;
      methods.push(head.split(" ")[0] as string);
      head = "";
      socket.end(answer);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `s3+http://127.0.0.1:${port}/b/ns?region=us-east-1`,
    methods,
    close: () => server.close(),
  };
}

const fake = new Credentials({ accessKeyId: "fake", secretAccessKey: "fake-secret" });

test("a body cut short is retried as transient", async () => {
  const server = await rawServer("HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nabc");
  const store = await openStore(server.url, fake);
  try {
    await assert.rejects(
      store.get("k"),
      (e) => e instanceof StoreFailure && /\(after 5 attempts\)/.test(e.message),
    );
    assert.deepEqual(server.methods, ["GET", "GET", "GET", "GET", "GET"]);
  } finally {
    await store.close();
    server.close();
  }
});

test("a listing without IsTruncated is complete", async () => {
  const body =
    '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>' +
    "<Contents><Key>ns/q/a</Key></Contents></ListBucketResult>";
  const server = await rawServer(
    `HTTP/1.1 200 OK\r\ncontent-length: ${body.length}\r\n\r\n${body}`,
  );
  const store = await openStore(server.url, fake);
  try {
    assert.deepEqual((await store.list("q/", { maxKeys: 10 })).keys, ["q/a"]);
    assert.deepEqual(server.methods, ["GET"]);
  } finally {
    await store.close();
    server.close();
  }
});

// Settings of the AWS environment and shared files that would change how the SDK reaches the
// endpoint, the profile's `s3` section included.
const AMBIENT: Record<string, string>[] = [
  { AWS_PROFILE: "absent" },
  { AWS_CONFIG_FILE: "accelerate" },
  { AWS_USE_FIPS_ENDPOINT: "true" },
  { AWS_USE_DUALSTACK_ENDPOINT: "true" },
];

for (const ambient of AMBIENT) {
  const [name, value] = Object.entries(ambient)[0] as [string, string];
  test(`the AWS environment and files are not read: ${name}`, async () => {
    const config = join(tempDir(), "config");
    writeFileSync(config, "[default]\ns3 =\n  use_accelerate_endpoint = true\n");
    const before = process.env[name];
    process.env[name] = value === "accelerate" ? config : value;
    const h3 = await FakeH3.start();
    const credentials = new Credentials({ accessKeyId: "fake", secretAccessKey: "fake-secret" });
    const store = await openStore(h3.url("ambient"), credentials);
    try {
      assert.equal(await store.create("q/a", new TextEncoder().encode("a")), "created");
      assert.deepEqual(await store.get("q/a"), new TextEncoder().encode("a"));
    } finally {
      await store.close();
      await h3.close();
      if (before === undefined) delete process.env[name];
      else process.env[name] = before;
    }
  });
}

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
  const store = await openStore(url, "env");
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
  const store = await openStore(missing, "env");
  await assert.rejects(store.get("runs/a/start"), StoreFailure);
  await assert.rejects(store.delete("runs/a/start"), StoreFailure);
  await store.close();
});

test("listings go past one page", live, async () => {
  const store = await openStore(s3Url(), "env");
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
  const store = await openStore(s3Url("h3"), "env");
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
  const h3 = await FakeH3.start();
  const credentials = new Credentials({ accessKeyId: "fake", secretAccessKey: "fake-secret" });
  const store = await openStore(h3.url("ns"), credentials);
  const s3 = await openStore(h3.url("ns").replace("h3+http", "s3+http"), credentials);
  try {
    await store.create("q/a&b", new TextEncoder().encode("x"));
    const first = await store.list("q/", { maxKeys: 10, wait: { seconds: 5 } });
    assert.deepEqual(first.keys, ["q/a&b"]);
    assert.ok(first.etag);
    assert.equal(h3.listed.at(-1)?.headers["x-substantial-wait-etag"], undefined);
    const listing = await store.list("q/", { maxKeys: 10, wait: { seconds: 0, etag: "e1" } });
    assert.deepEqual(listing, first);
    const headers = h3.listed.at(-1)?.headers ?? {};
    assert.deepEqual(
      [headers["x-substantial-wait-seconds"], headers["x-substantial-wait-etag"]],
      ["0", "e1"],
    );
    for (const name of ["x-substantial-wait-seconds", "x-substantial-wait-etag"]) {
      assert.ok(signedHeaders(headers).includes(name), name);
    }
    const sent = h3.requests.length;
    for (const wait of [{ seconds: 21 }, { seconds: 5, etag: "e\n1" }]) {
      await assert.rejects(store.list("q/", { maxKeys: 10, wait }), StoreFailure);
    }
    await assert.rejects(store.list("q/", { maxKeys: 1001, wait: { seconds: 5 } }), StoreFailure);
    assert.equal(h3.requests.length, sent, "refused before any request");
    await assert.rejects(s3.list("q/", { maxKeys: 10, wait: { seconds: 5 } }), StoreFailure);
    await store.close();
    await assert.rejects(store.get("k"), /closed/, "a closed store refuses every operation");
  } finally {
    await store.close();
    await s3.close();
    await h3.close();
  }
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
