// A fake h3 server on 127.0.0.1, ported from the Rust tests: the S3 calls of the store contract on
// one in-memory bucket, its listings held on the wait headers, and every request recorded.
import { createHash } from "node:crypto";
import { createServer, type IncomingHttpHeaders, type Server } from "node:http";
import type { AddressInfo } from "node:net";

const BUCKET = "h3";

/** A listing as the fake received it. */
export interface Listed {
  readonly prefix: string;
  readonly headers: IncomingHttpHeaders;
  /** Held on its wait etag, then answered with another page: a change ended the wait. */
  readonly woken: boolean;
}

function xmlEscape(text: string): string {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function error(status: number, code: string): [number, string] {
  return [status, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code></Error>`];
}

/** The headers `SignedHeaders` of the request's SigV4 `authorization` names. */
export function signedHeaders(headers: IncomingHttpHeaders): string[] {
  const signed = /SignedHeaders=([^,]+)/.exec(headers.authorization ?? "")?.[1];
  if (signed === undefined) throw new Error(`no SignedHeaders in ${headers.authorization}`);
  return signed.split(";");
}

/** The access key id the request's SigV4 `authorization` names. */
export function accessKeyId(headers: IncomingHttpHeaders): string | undefined {
  return /Credential=([^/]+)\//.exec(headers.authorization ?? "")?.[1];
}

export class FakeH3 {
  readonly objects = new Map<string, Uint8Array>();
  readonly listed: Listed[] = [];
  /** Every request, in the order received. */
  readonly requests: IncomingHttpHeaders[] = [];
  /** The method of every request, in the order received. */
  readonly methods: string[] = [];
  /** How many listings were held so far. */
  holds = 0;
  /** Answers waiting listings without their etag, as a server whose listings cannot wait. */
  omitEtag = false;
  readonly #server: Server;
  #changed = Promise.withResolvers<void>();

  private constructor(server: Server) {
    this.#server = server;
  }

  static async start(): Promise<FakeH3> {
    const server = createServer();
    const h3 = new FakeH3(server);
    server.on("request", (request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        h3.#handle(
          request.method ?? "",
          request.url ?? "/",
          request.headers,
          Buffer.concat(chunks),
        ).then(([status, body, headers]) => {
          response.writeHead(status, headers);
          response.end(body);
        });
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    return h3;
  }

  url(namespace: string): string {
    const { port } = this.#server.address() as AddressInfo;
    return `h3+http://127.0.0.1:${port}/${BUCKET}/${namespace}?region=us-east-1`;
  }

  async close(): Promise<void> {
    this.#server.closeAllConnections();
    await new Promise((resolve) => this.#server.close(resolve));
  }

  #change(): void {
    this.#changed.resolve();
    this.#changed = Promise.withResolvers<void>();
  }

  async #handle(
    method: string,
    url: string,
    headers: IncomingHttpHeaders,
    body: Buffer,
  ): Promise<[number, string | Uint8Array, Record<string, string>?]> {
    this.requests.push(headers);
    this.methods.push(method);
    const parsed = new URL(url, "http://h3");
    const path = decodeURIComponent(parsed.pathname);
    if (!path.startsWith(`/${BUCKET}`)) return error(404, "NoSuchBucket");
    const key = path.slice(BUCKET.length + 2);
    if (method === "GET" && key === "" && parsed.searchParams.get("list-type") === "2") {
      return this.#list(headers, parsed.searchParams);
    }
    if (method === "PUT" && key !== "") {
      if (headers["if-none-match"] === "*" && this.objects.has(key)) {
        return error(412, "PreconditionFailed");
      }
      this.objects.set(key, new Uint8Array(body));
      this.#change();
      return [200, ""];
    }
    if (method === "GET" && key !== "") {
      const bytes = this.objects.get(key);
      return bytes === undefined ? error(404, "NoSuchKey") : [200, bytes];
    }
    if (method === "DELETE" && key !== "") {
      this.objects.delete(key);
      this.#change();
      return [204, ""];
    }
    return error(400, "InvalidRequest");
  }

  #page(prefix: string, after: string, maxKeys: number): [string[], boolean] {
    const keys = [...this.objects.keys()]
      .filter((k) => k.startsWith(prefix) && k > after)
      .sort()
      .slice(0, maxKeys + 1);
    return [keys.slice(0, maxKeys), keys.length > maxKeys];
  }

  async #list(
    headers: IncomingHttpHeaders,
    query: URLSearchParams,
  ): Promise<[number, string, Record<string, string>]> {
    const prefix = query.get("prefix") ?? "";
    const after = query.get("start-after") ?? "";
    const maxKeys = Number(query.get("max-keys") ?? 1000);
    const seconds = headers["x-substantial-wait-seconds"];
    const waitEtag = headers["x-substantial-wait-etag"];
    const deadline = performance.now() + Number(seconds ?? 0) * 1000;
    let held = false;
    let keys: string[];
    let truncated: boolean;
    let etag: string;
    for (;;) {
      const changed = this.#changed.promise;
      [keys, truncated] = this.#page(prefix, after, maxKeys);
      etag = createHash("sha256")
        .update(JSON.stringify([keys, truncated]))
        .digest("hex");
      const left = deadline - performance.now();
      if (seconds === undefined || waitEtag !== etag || left <= 0) break;
      if (!held) {
        held = true;
        this.holds += 1;
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([changed, new Promise((resolve) => (timer = setTimeout(resolve, left)))]);
      clearTimeout(timer);
    }
    this.listed.push({ prefix, headers, woken: held && waitEtag !== etag });
    let xml =
      '<?xml version="1.0" encoding="UTF-8"?>' +
      '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
      `<Name>${BUCKET}</Name><Prefix>${xmlEscape(prefix)}</Prefix><KeyCount>${keys.length}` +
      `</KeyCount><MaxKeys>${maxKeys}</MaxKeys><IsTruncated>${truncated}</IsTruncated>`;
    for (const key of keys)
      xml += `<Contents><Key>${xmlEscape(key)}</Key><Size>0</Size></Contents>`;
    xml += "</ListBucketResult>";
    const answer: Record<string, string> = {};
    if (seconds !== undefined && !this.omitEtag) answer["x-substantial-listing-etag"] = etag;
    return [200, xml, answer];
  }
}
