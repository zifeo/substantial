// Opening the adapter a URL names (I-16), and performing the core's store operations.
import { create } from "@bufbuild/protobuf";
import { ConfigError } from "../errors.ts";
import { boundary } from "../proto.ts";
import { STORE_RETRY } from "../settings.ts";
import { type CredentialsArg, refuse } from "./credentials.ts";
import { FsStore } from "./fs.ts";
import { MemoryStore } from "./memory.ts";
import { parseS3Url, S3Store } from "./s3.ts";
import { type ListOptions, type Store, StoreFailure } from "./store.ts";

function namespace(url: string, rest: string): string {
  if (rest.includes("?")) throw new ConfigError(`${url} takes no query parameters`);
  if (rest === "") throw new ConfigError(`${url} names no namespace`);
  return rest;
}

/** The adapter a URL names; the scheme alone picks it, nothing is detected. */
export function openStore(url: string, credentials: CredentialsArg): Store {
  if (typeof url !== "string") throw new ConfigError(`a store URL is a string, not ${url}`);
  if (url.startsWith("memory://")) {
    const ns = namespace(url, url.slice("memory://".length));
    refuse(url, credentials);
    return new MemoryStore(ns);
  }
  if (url.startsWith("file:")) {
    const rest = url.slice("file:".length);
    const path = namespace(url, rest.startsWith("//") ? rest.slice(2) : rest);
    if (path.startsWith("~")) {
      throw new ConfigError(`${url}: give an absolute or relative path, ~ is not expanded`);
    }
    refuse(url, credentials);
    return new FsStore(path, STORE_RETRY);
  }
  if (/^(s3|h3)(\+http)?:\/\//.test(url)) {
    return new S3Store(parseS3Url(url), credentials, STORE_RETRY, globalThis.fetch);
  }
  const planned = ["redis://", "sqlite:", "postgres://", "mysql://"].find((s) => url.startsWith(s));
  if (planned !== undefined) {
    throw new ConfigError(`${planned} stores are not available in this host yet; ${SUPPORTED}`);
  }
  throw new ConfigError(`unknown scheme in ${url}; ${SUPPORTED}`);
}

const SUPPORTED =
  "the supported schemes are memory://, file:, s3://, s3+http://, h3:// and h3+http://";

async function performOp(store: Store, op: boundary.StoreOp): Promise<boundary.OpResult> {
  const o = op.op;
  switch (o.case) {
    case "create": {
      const outcome = await store.create(o.value.key, o.value.bytes);
      return create(boundary.OpResultSchema, { result: { case: outcome, value: {} } });
    }
    case "get": {
      const data = await store.get(o.value);
      return create(boundary.OpResultSchema, {
        result:
          data === undefined ? { case: "missing", value: {} } : { case: "bytes", value: data },
      });
    }
    case "list": {
      const l = o.value;
      const options: ListOptions = {
        maxKeys: l.maxKeys,
        ...(l.startAfter === undefined ? {} : { startAfter: l.startAfter }),
        ...(l.wait === undefined
          ? {}
          : {
              wait: {
                seconds: l.wait.seconds,
                ...(l.wait.etag === undefined ? {} : { etag: l.wait.etag }),
              },
            }),
      };
      const listing = await store.list(l.prefix, options);
      return create(boundary.OpResultSchema, {
        result: { case: "listing", value: { keys: listing.keys, etag: listing.etag } },
      });
    }
    case "delete":
      await store.delete(o.value);
      return create(boundary.OpResultSchema, { result: { case: "deleted", value: {} } });
    default:
      throw new StoreFailure("empty store op from the core");
  }
}

/** Performs one `StoreOp` of the core and answers with its `OpResult`, a failure included. */
export async function perform(store: Store, op: boundary.StoreOp): Promise<boundary.OpResult> {
  let message: string;
  try {
    return await performOp(store, op);
  } catch (e) {
    if (!(e instanceof StoreFailure)) {
      // An adapter's bug must not leave the core waiting for an answer that never comes.
      console.error(`substantial: store adapter failed on ${op.op.case}:`, e);
    }
    message =
      e instanceof Error
        ? `${e instanceof StoreFailure ? "" : `${e.name}: `}${e.message}`
        : String(e);
  }
  return create(boundary.OpResultSchema, { result: { case: "failed", value: { message } } });
}
