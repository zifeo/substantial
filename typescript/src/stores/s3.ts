import { ConfigError } from "../errors.ts";
import type { StoreRetry } from "../settings.ts";
import { type CredentialsArg, Keys } from "./credentials.ts";
import * as retry from "./retry.ts";
import { Ambiguous, Fault, Refused, Transient } from "./retry.ts";
import { errorCode, errorText, parseListing } from "./s3xml.ts";
import { canonicalQuery, encode, sign } from "./sigv4.ts";
import type { Capabilities, Create, Listing, ListOptions, ListWait, Store } from "./store.ts";
import { refuseWait, StoreFailure } from "./store.ts";

/** ListObjectsV2 returns at most this many keys a page. */
export const PAGE = 1000;
// fetch has no connect timeout of its own: the attempt's covers the connection too.
const ATTEMPT_MS = 60_000;
export const WAIT_SECONDS = "x-substantial-wait-seconds";
export const WAIT_ETAG = "x-substantial-wait-etag";
export const LISTING_ETAG = "x-substantial-listing-etag";
const MAX_WAIT_SECONDS = 20;
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
  if (parsed.username !== "" || parsed.password !== "") {
    throw invalid("credentials go in `credentials`, not in the URL");
  }
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
function waitHeaders(wait: ListWait | undefined, maxKeys: number): Record<string, string> {
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

interface Answer {
  readonly status: number;
  readonly headers: Headers;
  readonly body: Uint8Array;
}

/** Classifies the failure of an operation other than a create, which none makes ambiguous. */
function statusFault(what: string, answer: Answer): Fault {
  const message = `${what}: HTTP ${answer.status} ${errorText(answer.body)}`;
  const s = answer.status;
  return s >= 500 || s === 408 || s === 409 || s === 429
    ? new Transient(message)
    : new Refused(message);
}

/** `s3://<endpoint>/<bucket>/<ns>?region=<r>`: one object per key under the prefix `<ns>/`,
 * created with `If-None-Match: *`. `h3://` is the same adapter, its listings declared able to
 * wait. Requests go through the `fetch` given, never retried by it: `storeRetry` is the only
 * retry. */
export class S3Store implements Store {
  readonly capabilities: Capabilities;
  readonly #location: S3Location;
  readonly #keys: Keys;
  readonly #prefix: string;
  readonly #retry: StoreRetry;
  readonly #fetch: typeof fetch;
  readonly #closing = new AbortController();

  constructor(
    location: S3Location,
    credentials: CredentialsArg,
    storeRetry: StoreRetry,
    fetcher: typeof fetch,
  ) {
    this.#keys = new Keys(credentials);
    this.#location = location;
    this.#prefix = `${location.namespace}/`;
    this.#retry = storeRetry;
    this.#fetch = fetcher;
    this.capabilities = { wait: location.wait };
  }

  #full(key: string): string {
    return `${this.#prefix}${key}`;
  }

  async #send(
    method: string,
    key: string | undefined,
    params: Record<string, string> = {},
    body: Uint8Array = new Uint8Array(),
    headers: Record<string, string> = {},
  ): Promise<Answer> {
    if (this.#closing.signal.aborted) throw new Refused("the store is closed");
    const loc = this.#location;
    let path = `/${encode(loc.bucket)}`;
    if (key !== undefined) path += `/${encode(this.#full(key), true)}`;
    const query = canonicalQuery(params);
    const credentials = await this.#keys.get();
    const { host: _, ...signed } = await sign({
      method,
      host: loc.host,
      path,
      query,
      headers,
      payload: body,
      credentials,
      region: loc.region,
      at: new Date(),
    });
    const url = `${loc.endpoint}${path}${query === "" ? "" : `?${query}`}`;
    // A request that hangs must become an ambiguous outcome, not a stuck worker.
    const signal = AbortSignal.any([this.#closing.signal, AbortSignal.timeout(ATTEMPT_MS)]);
    try {
      const response = await this.#fetch(url, {
        method,
        headers: signed,
        ...(method === "PUT" ? { body: body as Uint8Array<ArrayBuffer> } : {}),
        redirect: "manual",
        signal,
      });
      const bytes = new Uint8Array(await response.arrayBuffer());
      return { status: response.status, headers: response.headers, body: bytes };
    } catch (e) {
      if (this.#closing.signal.aborted) throw new Refused("the store is closed");
      throw e;
    }
  }

  async #put(key: string, data: Uint8Array): Promise<Create> {
    let answer: Answer;
    try {
      answer = await this.#send("PUT", key, {}, data, { "if-none-match": "*" });
    } catch (e) {
      if (e instanceof Fault) throw e;
      throw new Ambiguous(`create ${key}: ${e instanceof Error ? e.message : e}`);
    }
    const s = answer.status;
    if (s >= 200 && s < 300) return "created";
    if (s === 412) return "exists";
    const message = `create ${key}: HTTP ${s} ${errorText(answer.body)}`;
    // AWS: a concurrent conditional write of the key was in flight, this one did not happen.
    if (s === 409 || s === 429) throw new Transient(message);
    if (s >= 500 || s === 408) throw new Ambiguous(message);
    throw new Refused(message);
  }

  async #request(
    what: string,
    method: string,
    key?: string,
    params?: Record<string, string>,
    headers?: Record<string, string>,
  ): Promise<Answer> {
    try {
      return await this.#send(method, key, params, undefined, headers);
    } catch (e) {
      if (e instanceof Fault) throw e;
      throw new Transient(`${what}: ${e instanceof Error ? e.message : e}`);
    }
  }

  async #read(key: string): Promise<Uint8Array | undefined> {
    const answer = await this.#request(`get ${key}`, "GET", key);
    if (answer.status === 200) return answer.body;
    if (answer.status === 404 && errorCode(answer.body) === "NoSuchKey") return undefined;
    throw statusFault(`get ${key}`, answer);
  }

  async #listOnce(prefix: string, options: ListOptions): Promise<Listing> {
    const { startAfter, maxKeys, wait } = options;
    const keys: string[] = [];
    let etag: string | undefined;
    let after = startAfter === undefined ? undefined : this.#full(startAfter);
    const headers = waitHeaders(wait, maxKeys);
    while (keys.length < maxKeys) {
      const params: Record<string, string> = {
        "list-type": "2",
        prefix: this.#full(prefix),
        "max-keys": String(Math.min(maxKeys - keys.length, PAGE)),
      };
      if (after !== undefined) params["start-after"] = after;
      const answer = await this.#request(`list ${prefix}`, "GET", undefined, params, headers);
      if (answer.status !== 200) throw statusFault(`list ${prefix}`, answer);
      if (wait !== undefined) etag = answer.headers.get(LISTING_ETAG) ?? undefined;
      const page = parseListing(answer.body);
      if (page === undefined) throw new Transient(`list ${prefix}: not a ListObjectsV2 answer`);
      for (const full of page.keys) {
        if (!full.startsWith(this.#prefix)) {
          throw new Refused(`listed ${JSON.stringify(full)} outside the namespace`);
        }
        keys.push(full.slice(this.#prefix.length));
      }
      if (wait !== undefined || !(page.truncated && keys.length > 0)) break;
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
    const answer = await this.#request(`delete ${key}`, "DELETE", key);
    if (answer.status >= 200 && answer.status < 300) return;
    if (answer.status === 404 && errorCode(answer.body) === "NoSuchKey") return;
    throw statusFault(`delete ${key}`, answer);
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

  /** Aborts the requests in flight; any later operation fails. */
  async close(): Promise<void> {
    this.#closing.abort();
  }
}

/** Resolves once the `result` at `key` exists, holding waiting listings that take its key as
 * their prefix: a result waited on is seen as soon as it is created, in one request per
 * `MAX_WAIT_SECONDS` meanwhile. */
export async function resultCreated(store: Store, key: string): Promise<void> {
  let wait: ListWait = { seconds: 0 };
  for (;;) {
    const page = await store.list(key, { maxKeys: 1, wait });
    if (page.keys.includes(key)) return;
    if (page.etag === undefined) {
      throw new StoreFailure(`a waiting listing answered without ${LISTING_ETAG}`);
    }
    wait = { seconds: MAX_WAIT_SECONDS, etag: page.etag };
  }
}
