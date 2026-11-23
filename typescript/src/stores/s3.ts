import type { S3Client, S3ClientConfig, S3ServiceException } from "@aws-sdk/client-s3";
import { ConfigError, messageOf } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import { sent } from "./attempts.ts";
import { type CredentialsArg, Keys } from "./credentials.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Fault, Refused, Transient } from "./retry.ts";
import type {
  Capabilities,
  Cas,
  Create,
  Listing,
  ListOptions,
  ListWait,
  Store,
  StoreObject,
} from "./store.ts";
import { checkAllDays, checkDays, refuseWait, StoreFailure } from "./store.ts";

/** `@aws-sdk/client-s3`, loaded when an S3 store is opened. */
export type Sdk = typeof import("@aws-sdk/client-s3");

/** ListObjectsV2 returns at most this many keys a page. */
export const PAGE = 1000;
// § gvqb04830.b@av6rt
const CONNECT_MS = 5_000;
const ATTEMPT_MS = 60_000;
// § end gvqb04830.b
export const WAIT_SECONDS = "x-herald-wait-seconds";
export const WAIT_ETAG = "x-herald-wait-etag";
export const LISTING_ETAG = "x-herald-listing-etag";
const MAX_WAIT_SECONDS = 20;
/** A region as AWS names them, a DNS label of lowercase letters, digits and hyphens. */
const REGION = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;
const REGION_RULE =
  "1 to 63 lowercase letters, digits and hyphens, neither starting nor ending with a hyphen";
/** The suffix of an S3 directory bucket's name. */
const DIRECTORY_BUCKET = "--x-s3";
const DIRECTORY_REFUSED =
  "names an S3 directory bucket, whose listings are not in byte order of the key as the store " +
  "contract requires: use a general purpose bucket";
const SCHEMES: Record<string, { wait: boolean; protocol: "https" | "http" }> = {
  "s3:": { wait: false, protocol: "https" },
  "s3+http:": { wait: false, protocol: "http" },
  "h3:": { wait: true, protocol: "https" },
  "h3+http:": { wait: true, protocol: "http" },
};

/** The parts of an `s3://` or `h3://` URL, every one required (I-16). */
export interface S3Location {
  readonly endpoint: string;
  readonly host: string;
  readonly bucket: string;
  readonly namespace: string;
  readonly region: string;
  readonly wait: boolean;
}

export function parseS3Url(url: string): S3Location {
  // § e9ern38bz.b@7v1pe
  const invalid = (why: string) => new ConfigError(`${url}: ${why}`);
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw invalid("not a URL");
  }
  const scheme = SCHEMES[parsed.protocol];
  if (scheme === undefined) throw invalid("not an s3:// or h3:// URL");
  if (url.includes("#")) throw invalid("a store URL takes no fragment");
  // The raw authority: `URL` drops an empty userinfo, as in `s3://@host`.
  const authority = url.slice(parsed.protocol.length + 2).split(/[/?#]/, 1)[0] ?? "";
  if (authority.includes("@")) throw invalid("credentials go in `credentials`, not in the URL");
  if (parsed.hostname === "") throw invalid("names no endpoint");
  const defaultPort = scheme.protocol === "https" ? "443" : "80";
  const port = parsed.port === "" || parsed.port === defaultPort ? "" : `:${parsed.port}`;
  const host = `${parsed.hostname}${port}`;
  const [bucket = "", ...rest] = parsed.pathname.replace(/^\//, "").split("/");
  if (bucket === "") throw invalid("names no bucket");
  // § h1b3afst6@m7g81
  if (bucket.endsWith(DIRECTORY_BUCKET)) throw invalid(DIRECTORY_REFUSED);
  // § end h1b3afst6
  if (rest.length === 0 || rest.some((c) => c === "")) {
    throw invalid("names no namespace, or one with an empty component");
  }
  const query = new Map<string, string>();
  // forEach, not for-of: Deno checks with TypeScript 5.9, whose `dom` lib has no iterable
  // URLSearchParams.
  parsed.searchParams.forEach((value, name) => {
    if (query.has(name)) throw invalid(`${name} is given twice`);
    query.set(name, value);
  });
  const region = query.get("region") ?? "";
  if (region === "") throw invalid("the query parameter region is required");
  if (!REGION.test(region))
    throw invalid(`the region ${JSON.stringify(region)} is not ${REGION_RULE}`);
  query.delete("region");
  const unknown = [...query.keys()][0];
  if (unknown !== undefined) throw invalid(`unknown query parameter ${unknown}`);
  return {
    endpoint: `${scheme.protocol}://${host}`,
    host,
    bucket,
    namespace: rest.join("/"),
    region,
    wait: scheme.wait,
  };
}

/** The headers of a waiting listing, signed with the request. */
export function waitHeaders(wait: ListWait | undefined, maxKeys: number): Record<string, string> {
  if (wait === undefined) return {};
  // The listing etag covers one page, so a waiting listing is never continued.
  if (maxKeys > PAGE) throw new Refused(`a waiting listing takes at most ${PAGE} keys`);
  if (!Number.isInteger(wait.seconds) || wait.seconds < 0 || wait.seconds > MAX_WAIT_SECONDS) {
    throw new Refused(`a waiting listing waits 0 to ${MAX_WAIT_SECONDS} s, not ${wait.seconds} s`);
  }
  if (wait.etag !== undefined && !/^[\x21-\x7e]+$/.test(wait.etag)) {
    throw new Refused(`the listing etag ${JSON.stringify(wait.etag)} cannot be sent`);
  }
  // § gvqb04830.d@0fa6r
  const headers: Record<string, string> = { [WAIT_SECONDS]: String(wait.seconds) };
  if (wait.etag !== undefined) headers[WAIT_ETAG] = wait.etag;
  return headers;
}

const RETENTION_TAG = "substantial-retention";

/** The tag of a write's expiry, which a lifecycle rule of the bucket expires. */
function tagging(expiry: number | undefined): { Tagging?: string } {
  // § c4fyc3wyk@k6r2p
  return expiry === undefined ? {} : { Tagging: `${RETENTION_TAG}=${expiry}d` };
}

/** A lifecycle rule as the SDK answers it, the fields the check reads. */
interface Rule {
  Status?: string;
  Expiration?: { Days?: number };
  Filter?: {
    Prefix?: string;
    Tag?: { Key?: string; Value?: string };
    ObjectSizeGreaterThan?: number;
    ObjectSizeLessThan?: number;
    And?: {
      Prefix?: string;
      Tags?: { Key?: string; Value?: string }[];
      ObjectSizeGreaterThan?: number;
      ObjectSizeLessThan?: number;
    };
  };
}

/** Whether `rule` expires, after `days`, the keys under `prefix` tagged with that expiry. */
function expires(rule: Rule, prefix: string, days: number): boolean {
  // § cfdckf61n@fkyxw
  const value = `${days}d`;
  const tagged = (tags: { Key?: string; Value?: string }[]) =>
    tags.length === 1 && tags[0]?.Key === RETENTION_TAG && tags[0]?.Value === value;
  const filter = rule.Filter;
  let filtered = false;
  if (filter?.And !== undefined && filter.Tag === undefined) {
    const and = filter.And;
    filtered =
      and.ObjectSizeGreaterThan === undefined &&
      and.ObjectSizeLessThan === undefined &&
      prefix.startsWith(and.Prefix ?? "") &&
      tagged(and.Tags ?? []);
  } else if (filter?.And === undefined && filter?.Tag !== undefined) {
    filtered = tagged([filter.Tag]);
  }
  return filtered && rule.Status === "Enabled" && rule.Expiration?.Days === days;
}

/** Why a store would never expire the keys of `missing` expiries, with the rules to add as JSON. */
function missingRules(bucket: string, prefix: string, missing: number[]): string {
  const rules = missing.map((d) => ({
    Expiration: { Days: d },
    Filter: { And: { Prefix: prefix, Tags: [{ Key: RETENTION_TAG, Value: `${d}d` }] } },
    ID: `substantial-expire-${d}d`,
    Status: "Enabled",
  }));
  const tags = missing.map((d) => `${d}d`).join(", ");
  return (
    `the bucket ${JSON.stringify(bucket)} has no lifecycle rule expiring the keys under ` +
    `${JSON.stringify(prefix)} tagged ${RETENTION_TAG}=${tags} after as many days, so they would ` +
    `never expire: add these rules to its lifecycle configuration, which needs the permission ` +
    `s3:PutLifecycleConfiguration: ${JSON.stringify({ Rules: rules })}`
  );
}

/** The S3 error answer `e` is; any other failure is thrown again. */
function refusal(what: string, e: unknown): { status: number; code: string; message: string } {
  const status = (e as Partial<S3ServiceException>)?.$metadata?.httpStatusCode;
  if (status === undefined) throw e;
  const { name: code, message } = e as S3ServiceException;
  const text = message && message !== "UnknownError" ? `${code}: ${message}` : code;
  return { status, code, message: `${what}: HTTP ${status} ${text}` };
}

const transient = (message: string) => new Transient(message);

/** Classifies the failure of an operation other than a create, which none makes ambiguous. */
function statusFault(r: { status: number; message: string }): Fault {
  const s = r.status;
  return s >= 500 || s === 408 || s === 409 || s === 429
    ? new Transient(r.message)
    : new Refused(r.message);
}

/** The keys the SDK signs with: those `Keys.get` resolved for the request. The SDK would ask a
 * provider twice a request near expiry, so `Keys` decides when to ask it, as in the Rust host. */
function sdkCredentials(keys: Keys): NonNullable<S3ClientConfig["credentials"]> {
  return async () => {
    const { value: credentials, expiresAt } = keys.current;
    return {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
      ...(credentials.sessionToken === undefined ? {} : { sessionToken: credentials.sessionToken }),
      // The SDK keeps keys without an expiration for good, and reads others again from 5 minutes
      // before it, when `Keys` refreshes them.
      ...(expiresAt === undefined ? {} : { expiration: new Date(expiresAt) }),
    };
  };
}

/** The request and response of a middleware, as the SDK's HTTP handler gives them. */
interface Http {
  readonly request: { headers: Record<string, string> };
}
interface Answered {
  readonly response: { headers: Record<string, string | undefined> };
}

/** `s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
 * created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able to
 * wait. Requests go through `@aws-sdk/client-s3`, its own retries off: `storeRetry` is the only
 * retry. */
export class S3Store implements Store {
  readonly capabilities: Capabilities;
  readonly #sdk: Sdk;
  readonly #keys: Keys;
  readonly #client: S3Client;
  readonly #bucket: string;
  readonly #prefix: string;
  readonly #retry: StoreRetry;
  readonly #closing = new AbortController();

  constructor(location: S3Location, credentials: CredentialsArg, storeRetry: StoreRetry, sdk: Sdk) {
    this.#sdk = sdk;
    this.#keys = new Keys(credentials, ATTEMPT_MS);
    this.#client = new sdk.S3Client({
      endpoint: location.endpoint,
      region: location.region,
      forcePathStyle: true,
      credentials: sdkCredentials(this.#keys),
      maxAttempts: 1,
      // The SDK reads each of these from the environment and the shared files unless given: the
      // URL and the credentials alone define the store, as in every host.
      retryMode: "standard",
      useFipsEndpoint: false,
      useDualstackEndpoint: false,
      useArnRegion: false,
      disableS3ExpressSessionAuth: false,
      authSchemePreference: [],
      requestHandler: {
        connectionTimeout: CONNECT_MS,
        requestTimeout: ATTEMPT_MS,
        // No cap, as in every host: a waiting listing holds its socket up to 20 s, one per
        // workflow served and per `result` waiter, and the SDK's cap of 50 would
        // queue every other request behind them.
        // § x2qrbh9jk.a@72sc0
        httpAgent: { maxSockets: Number.POSITIVE_INFINITY },
        httpsAgent: { maxSockets: Number.POSITIVE_INFINITY },
        // § end x2qrbh9jk.a
      },
      // S3-compatible stores that predate flexible checksums refuse their headers.
      requestChecksumCalculation: "WHEN_REQUIRED",
      responseChecksumValidation: "WHEN_REQUIRED",
      // `Expect: 100-continue` costs a round trip per create, of a few bytes each.
      expectContinueHeader: false,
    });
    this.#bucket = location.bucket;
    this.#prefix = `${location.namespace}/`;
    this.#retry = storeRetry;
    this.capabilities = { wait: location.wait };
  }

  #full(key: string): string {
    return `${this.#prefix}${key}`;
  }

  /** One request; a failure without an answer throws `failed`'s fault. */
  async #send<O>(
    what: string,
    failed: (message: string) => Fault,
    request: (options: { abortSignal: AbortSignal }) => Promise<O>,
    abandoned?: AbortSignal,
  ): Promise<O> {
    if (this.#closing.signal.aborted) throw new Refused("the store is closed");
    if (abandoned?.aborted) throw new Refused(`${what}: abandoned`);
    await this.#keys.get();
    // A request that hangs must become an ambiguous outcome, not a stuck worker.
    const signals = [this.#closing.signal, AbortSignal.timeout(ATTEMPT_MS)];
    const abortSignal = AbortSignal.any(
      abandoned === undefined ? signals : [...signals, abandoned],
    );
    try {
      return await request({ abortSignal });
    } catch (e) {
      if (e instanceof Fault) throw e;
      if (this.#closing.signal.aborted) throw new Refused("the store is closed");
      if (abandoned?.aborted) throw new Refused(`${what}: abandoned`);
      if ((e as Partial<S3ServiceException>)?.$metadata?.httpStatusCode !== undefined) throw e;
      throw failed(`${what}: ${messageOf(e)}`);
    }
  }

  async #put(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    // § gvqb04830@mqpeq
    const what = `create ${key}`;
    const command = new this.#sdk.PutObjectCommand({
      Bucket: this.#bucket,
      Key: this.#full(key),
      Body: data,
      IfNoneMatch: "*",
      ...tagging(expiry),
    });
    let written: string | undefined;
    try {
      ({ ETag: written } = await this.#send(
        what,
        (m) => new Ambiguous(m),
        (o) => this.#client.send(command, o),
      ));
    } catch (e) {
      const r = refusal(what, e);
      if (r.status === 412) return "exists";
      // AWS: a concurrent conditional write of the key was in flight, this one did not happen.
      if (r.status === 409 || r.status === 429) throw new Transient(r.message);
      if (r.status >= 500 || r.status === 408) throw new Ambiguous(r.message);
      throw new Refused(r.message);
    }
    // An answer without its ETag is read back, as one lost.
    if (written === undefined)
      throw new Ambiguous(`${what}: created without an ETag in the answer`);
    return { etag: written };
  }

  async #putIfMatch(
    key: string,
    data: Uint8Array,
    etag: string,
    expiry: number | undefined,
  ): Promise<Cas> {
    // § c4fyc3wyk@px2rs
    const what = `cas ${key}`;
    const command = new this.#sdk.PutObjectCommand({
      Bucket: this.#bucket,
      Key: this.#full(key),
      Body: data,
      IfMatch: etag,
      ...tagging(expiry),
    });
    let written: string | undefined;
    try {
      ({ ETag: written } = await this.#send(
        what,
        (m) => new Ambiguous(m),
        (o) => this.#client.send(command, o),
      ));
    } catch (e) {
      const r = refusal(what, e);
      // § c4fyc3wyk.a@76mf2
      if (r.status === 412 || (r.status === 404 && r.code === "NoSuchKey")) return "mismatch";
      if (r.status === 409 || r.status === 429) throw new Transient(r.message);
      if (r.status >= 500 || r.status === 408) throw new Ambiguous(r.message);
      throw new Refused(r.message);
      // § end c4fyc3wyk.a
    }
    // § one c4fyc3wyk.b@9wxxt
    if (written === undefined)
      throw new Ambiguous(`${what}: written without an ETag in the answer`);
    return { etag: written };
  }

  async #read(key: string): Promise<StoreObject | undefined> {
    const what = `get ${key}`;
    const command = new this.#sdk.GetObjectCommand({ Bucket: this.#bucket, Key: this.#full(key) });
    let body: { transformToByteArray(): Promise<Uint8Array> } | undefined;
    let etag: string | undefined;
    try {
      ({ Body: body, ETag: etag } = await this.#send(what, transient, (o) =>
        this.#client.send(command, o),
      ));
    } catch (e) {
      const r = refusal(what, e);
      // § one gvqb04830.a@my2rp
      if (r.status === 404 && r.code === "NoSuchKey") return undefined;
      throw statusFault(r);
    }
    if (body === undefined) throw new Transient(`${what}: an answer without a body`);
    let data: Uint8Array;
    try {
      data = await body.transformToByteArray();
    } catch (e) {
      throw new Transient(`${what}: ${messageOf(e)}`);
    }
    if (etag === undefined) throw new Refused(`${what}: answered without an ETag`);
    return { data, etag };
  }

  /** The bucket's lifecycle rules, none when it has no configuration. */
  async #lifecycle(): Promise<Rule[]> {
    const what = `lifecycle of ${this.#bucket}`;
    const command = new this.#sdk.GetBucketLifecycleConfigurationCommand({ Bucket: this.#bucket });
    try {
      const { Rules } = await this.#send(what, transient, (o) => this.#client.send(command, o));
      return (Rules ?? []) as Rule[];
    } catch (e) {
      const r = refusal(what, e);
      if (r.code === "NoSuchLifecycleConfiguration") return [];
      // § cfdckf61n.a@ctkz4
      if (r.status === 403) {
        throw new Refused(
          `reading the lifecycle rules of the bucket ${JSON.stringify(this.#bucket)} needs the ` +
            `permission s3:GetLifecycleConfiguration: ${r.message}`,
        );
      }
      // § end cfdckf61n.a
      throw statusFault(r);
    }
  }

  async #listOnce(prefix: string, options: ListOptions): Promise<Listing> {
    // § dfd6vt90h.c@8m2y9
    const { startAfter, maxKeys, wait } = options;
    const keys: string[] = [];
    let etag: string | undefined;
    let after = startAfter === undefined ? undefined : this.#full(startAfter);
    const headers = waitHeaders(wait, maxKeys);
    while (keys.length < maxKeys) {
      const command = new this.#sdk.ListObjectsV2Command({
        Bucket: this.#bucket,
        Prefix: this.#full(prefix),
        MaxKeys: Math.min(maxKeys - keys.length, PAGE),
        ...(after === undefined ? {} : { StartAfter: after }),
      });
      if (wait !== undefined) {
        // At the build step, so the headers are signed with the request.
        command.middlewareStack.add(
          (next) => async (args) => {
            Object.assign((args as unknown as Http).request.headers, headers);
            const result = await next(args);
            etag = (result as unknown as Answered).response.headers[LISTING_ETAG];
            return result;
          },
          { step: "build", name: "substantialWait" },
        );
      }
      const what = `list ${prefix}`;
      let page: { Contents?: { Key?: string }[]; IsTruncated?: boolean | undefined };
      try {
        page = await this.#send(
          what,
          transient,
          (o) => this.#client.send(command, o),
          options.signal,
        );
      } catch (e) {
        throw statusFault(refusal(what, e));
      }
      for (const { Key: full = "" } of page.Contents ?? []) {
        if (!full.startsWith(this.#prefix)) {
          throw new Refused(`listed ${JSON.stringify(full)} outside the namespace`);
        }
        keys.push(full.slice(this.#prefix.length));
      }
      if (wait !== undefined || !(page.IsTruncated && keys.length > 0) || keys.length >= maxKeys) {
        break;
      }
      // The retry layer counted the first page's request.
      sent();
      after = this.#full(keys[keys.length - 1] as string);
    }
    if (wait !== undefined && !etag) {
      // § gvqb04830.d@s3h2e
      throw new Refused(`a waiting listing of ${prefix} answered without ${LISTING_ETAG}`);
    }
    return etag === undefined
      ? { keys: keys.slice(0, maxKeys) }
      : { keys: keys.slice(0, maxKeys), etag };
  }

  async #remove(key: string): Promise<void> {
    const what = `delete ${key}`;
    const command = new this.#sdk.DeleteObjectCommand({
      Bucket: this.#bucket,
      Key: this.#full(key),
    });
    try {
      await this.#send(what, transient, (o) => this.#client.send(command, o));
    } catch (e) {
      const r = refusal(what, e);
      // § one dfd6vt90h.d@bjy4j
      if (r.status === 404 && r.code === "NoSuchKey") return;
      throw statusFault(r);
    }
  }

  async create(key: string, data: Uint8Array, expiry: number | undefined): Promise<Create> {
    checkDays(expiry);
    const bytes = data.slice();
    return retry.create(
      this.#retry,
      bytes,
      () => this.#put(key, bytes, expiry),
      () => this.#read(key),
    );
  }

  async cas(key: string, data: Uint8Array, etag: string, expiry: number | undefined): Promise<Cas> {
    checkDays(expiry);
    const bytes = data.slice();
    return retry.cas(
      this.#retry,
      bytes,
      etag,
      () => this.#putIfMatch(key, bytes, etag, expiry),
      () => this.#read(key),
    );
  }

  async checkExpiry(days: readonly number[]): Promise<void> {
    checkAllDays(days);
    let rules: Rule[];
    try {
      rules = await retry.withRetry(this.#retry, () => this.#lifecycle());
    } catch (e) {
      if (e instanceof StoreFailure) throw new ConfigError(e.message);
      throw e;
    }
    const missing = [...new Set(days)]
      .filter((d) => !rules.some((r) => expires(r, this.#prefix, d)))
      .sort((a, b) => a - b);
    if (missing.length > 0) {
      throw new ConfigError(missingRules(this.#bucket, this.#prefix, missing));
    }
  }

  get(key: string): Promise<StoreObject | undefined> {
    return retry.withRetry(this.#retry, () => this.#read(key));
  }

  async list(prefix: string, options: ListOptions): Promise<Listing> {
    if (!this.capabilities.wait) refuseWait(options.wait);
    return retry.withRetry(this.#retry, () => this.#listOnce(prefix, options));
  }

  delete(key: string): Promise<void> {
    return retry.withRetry(this.#retry, () => this.#remove(key));
  }

  async open(): Promise<void> {}

  /** Aborts the requests in flight; any later operation fails. */
  async close(): Promise<void> {
    this.#closing.abort();
    this.#client.destroy();
  }
}

/** The etag of a listing of `key` alone, waited on until it differs from `etag` or for
 * `MAX_WAIT_SECONDS`, and answered at once without one. A rewrite of the key changes the etag, so
 * a wait on a chain's head sees each commit as it lands, in one request per `MAX_WAIT_SECONDS`
 * meanwhile. `signal` aborts the listing in flight. */
export async function listingChanged(
  store: Store,
  key: string,
  etag: string | undefined,
  signal: AbortSignal,
): Promise<string> {
  const wait: ListWait = etag === undefined ? { seconds: 0 } : { seconds: MAX_WAIT_SECONDS, etag };
  const page = await store.list(key, { maxKeys: 1, wait, signal });
  if (page.etag === undefined) {
    throw new StoreFailure(`a waiting listing answered without ${LISTING_ETAG}`);
  }
  return page.etag;
}
