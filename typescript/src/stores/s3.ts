import type { S3Client, S3ClientConfig, S3ServiceException } from "@aws-sdk/client-s3";
import { ConfigError, messageOf } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import { type CredentialsArg, Keys } from "./credentials.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Fault, Refused, Transient } from "./retry.ts";
import type { Capabilities, Create, Listing, ListOptions, ListWait, Store } from "./store.ts";
import { refuseWait, StoreFailure } from "./store.ts";

/** `@aws-sdk/client-s3`, loaded when an S3 store is opened. */
export type Sdk = typeof import("@aws-sdk/client-s3");

/** ListObjectsV2 returns at most this many keys a page. */
export const PAGE = 1000;
const CONNECT_MS = 5_000;
const ATTEMPT_MS = 60_000;
export const WAIT_SECONDS = "x-substantial-wait-seconds";
export const WAIT_ETAG = "x-substantial-wait-etag";
export const LISTING_ETAG = "x-substantial-listing-etag";
const MAX_WAIT_SECONDS = 20;
/** A region as AWS names them, a DNS label of lowercase letters, digits and hyphens. */
const REGION = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;
const REGION_RULE =
  "1 to 63 lowercase letters, digits and hyphens, neither starting nor ending with a hyphen";
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
  const headers: Record<string, string> = { [WAIT_SECONDS]: String(wait.seconds) };
  if (wait.etag !== undefined) headers[WAIT_ETAG] = wait.etag;
  return headers;
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
        // workflow and partition served and per `result` waiter, and the SDK's cap of 50 would
        // queue every other request behind them.
        httpAgent: { maxSockets: Number.POSITIVE_INFINITY },
        httpsAgent: { maxSockets: Number.POSITIVE_INFINITY },
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

  async #put(key: string, data: Uint8Array): Promise<Create> {
    const what = `create ${key}`;
    const command = new this.#sdk.PutObjectCommand({
      Bucket: this.#bucket,
      Key: this.#full(key),
      Body: data,
      IfNoneMatch: "*",
    });
    try {
      await this.#send(
        what,
        (m) => new Ambiguous(m),
        (o) => this.#client.send(command, o),
      );
    } catch (e) {
      const r = refusal(what, e);
      if (r.status === 412) return "exists";
      // AWS: a concurrent conditional write of the key was in flight, this one did not happen.
      if (r.status === 409 || r.status === 429) throw new Transient(r.message);
      if (r.status >= 500 || r.status === 408) throw new Ambiguous(r.message);
      throw new Refused(r.message);
    }
    return "created";
  }

  async #read(key: string): Promise<Uint8Array | undefined> {
    const what = `get ${key}`;
    const command = new this.#sdk.GetObjectCommand({ Bucket: this.#bucket, Key: this.#full(key) });
    let body: { transformToByteArray(): Promise<Uint8Array> } | undefined;
    try {
      ({ Body: body } = await this.#send(what, transient, (o) => this.#client.send(command, o)));
    } catch (e) {
      const r = refusal(what, e);
      if (r.status === 404 && r.code === "NoSuchKey") return undefined;
      throw statusFault(r);
    }
    if (body === undefined) throw new Transient(`${what}: an answer without a body`);
    try {
      return await body.transformToByteArray();
    } catch (e) {
      throw new Transient(`${what}: ${messageOf(e)}`);
    }
  }

  async #listOnce(prefix: string, options: ListOptions): Promise<Listing> {
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
      if (wait !== undefined || !(page.IsTruncated && keys.length > 0)) break;
      after = this.#full(keys[keys.length - 1] as string);
    }
    if (wait !== undefined && !etag) {
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
      if (r.status === 404 && r.code === "NoSuchKey") return;
      throw statusFault(r);
    }
  }

  create(key: string, data: Uint8Array): Promise<Create> {
    const bytes = data.slice();
    return retry.create(
      this.#retry,
      bytes,
      () => this.#put(key, bytes),
      () => this.#read(key),
    );
  }

  get(key: string): Promise<Uint8Array | undefined> {
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

/** Resolves once the `result` at `key` exists, holding waiting listings that take its key as
 * their prefix: a result waited on is seen as soon as it is created, in one request per
 * `MAX_WAIT_SECONDS` meanwhile; or once `deadline`, a `performance.now()`, has passed. `signal`
 * aborts the listing in flight and ends the wait. */
export async function resultCreated(
  store: Store,
  key: string,
  deadline: number,
  signal: AbortSignal,
): Promise<void> {
  let wait: ListWait = { seconds: 0 };
  while (performance.now() < deadline && !signal.aborted) {
    const page = await store.list(key, { maxKeys: 1, wait, signal });
    if (page.keys.includes(key)) return;
    if (page.etag === undefined) {
      throw new StoreFailure(`a waiting listing answered without ${LISTING_ETAG}`);
    }
    wait = { seconds: MAX_WAIT_SECONDS, etag: page.etag };
  }
}
