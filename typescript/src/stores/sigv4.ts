// AWS Signature Version 4 for the four S3 requests the adapter makes, over WebCrypto.
import type { Credentials } from "./credentials.ts";

const encoder = new TextEncoder();

/** URI-encodes as SigV4 requires: every byte but the unreserved characters. */
export function encode(value: string, keepSlash = false): string {
  let out = "";
  for (const byte of encoder.encode(value)) {
    const c = String.fromCharCode(byte);
    if (/[A-Za-z0-9\-_.~]/.test(c) || (keepSlash && c === "/")) {
      out += c;
    } else {
      out += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return out;
}

export function canonicalQuery(params: Record<string, string>): string {
  return Object.entries(params)
    .map(([k, v]) => [encode(k), encode(v)] as const)
    .sort(([a, x], [b, y]) => (a < b ? -1 : a > b ? 1 : x < y ? -1 : x > y ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

function hex(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(data: Uint8Array | string): Promise<string> {
  const bytes = typeof data === "string" ? encoder.encode(data) : data;
  return hex(await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>));
}

async function hmac(key: Uint8Array | ArrayBuffer, message: string): Promise<ArrayBuffer> {
  const k = await crypto.subtle.importKey(
    "raw",
    key as Uint8Array<ArrayBuffer>,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return crypto.subtle.sign("HMAC", k, encoder.encode(message));
}

export interface SignInput {
  readonly method: string;
  readonly host: string;
  /** Already encoded as it goes on the wire. */
  readonly path: string;
  /** Already encoded as it goes on the wire. */
  readonly query: string;
  readonly headers: Record<string, string>;
  readonly payload: Uint8Array;
  readonly credentials: Credentials;
  readonly region: string;
  readonly at: Date;
}

function amzDate(at: Date): string {
  return at
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}/, "");
}

/** The headers of a request, `headers` and `host` included, with its `authorization`. */
export async function sign(input: SignInput): Promise<Record<string, string>> {
  const date = amzDate(input.at);
  const day = date.slice(0, 8);
  const payloadHash = await sha256Hex(input.payload);
  const signed: Record<string, string> = {};
  for (const [k, v] of Object.entries(input.headers)) {
    signed[k.toLowerCase()] = v.trim();
  }
  signed.host = input.host;
  signed["x-amz-date"] = date;
  signed["x-amz-content-sha256"] = payloadHash;
  const token = input.credentials.sessionToken;
  if (token !== undefined) {
    signed["x-amz-security-token"] = token;
  }
  const names = Object.keys(signed).sort();
  const canonicalHeaders = names.map((n) => `${n}:${signed[n]}\n`).join("");
  const signedHeaders = names.join(";");
  const request = [
    input.method,
    input.path,
    input.query,
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");
  const scope = `${day}/${input.region}/s3/aws4_request`;
  const toSign = ["AWS4-HMAC-SHA256", date, scope, await sha256Hex(request)].join("\n");
  let key = await hmac(encoder.encode(`AWS4${input.credentials.secretAccessKey}`), day);
  for (const part of [input.region, "s3", "aws4_request"]) {
    key = await hmac(key, part);
  }
  const signature = hex(await hmac(key, toSign));
  signed.authorization =
    `AWS4-HMAC-SHA256 Credential=${input.credentials.accessKeyId}/${scope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return signed;
}
