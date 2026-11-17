// Redis store URLs; the adapter is in `redis_store.ts`, which needs `@redis/client`.
import { ConfigError } from "../errors.ts";

export type Topology = "standalone" | "sentinel" | "cluster";

export const CONNECT_MS = 5_000;
export const ATTEMPT_MS = 60_000;
/** How long a create waits for its AOF fsync on the primary and its replicas. */
export const WAITAOF_MS = 5_000;
/** How often `replicas=all` counts the replicas online again, so that one joining is waited for. */
export const RECOUNT_EVERY_MS = 10_000;
export const MAX_WAIT_SECONDS = 20;
export const SCHEMES: Record<string, { topology: Topology; tls: boolean }> = {
  redis: { topology: "standalone", tls: false },
  rediss: { topology: "standalone", tls: true },
  "redis+sentinel": { topology: "sentinel", tls: false },
  "rediss+sentinel": { topology: "sentinel", tls: true },
  "redis+cluster": { topology: "cluster", tls: false },
  "rediss+cluster": { topology: "cluster", tls: true },
};
// The namespace becomes the hash tag `{<ns>}` of every key, so it holds no brace.
const NAMESPACE = /^[A-Za-z0-9._-]+(\/[A-Za-z0-9._-]+)*$/;
const PARAMETERS: Record<Topology, string[]> = {
  standalone: ["db", "replicas"],
  sentinel: ["db", "master", "replicas"],
  cluster: ["replicas"],
};

/** The parts of a Redis URL, every one required (I-16): the server, the sentinels or the cluster
 * nodes to start from, the namespace, and `replicas`, the replicas that fsync a create before it
 * is acknowledged, `"all"` for every replica connected to the primary. */
export interface RedisLocation {
  readonly topology: Topology;
  readonly tls: boolean;
  readonly nodes: readonly (readonly [string, number])[];
  readonly namespace: string;
  readonly db: number;
  readonly replicas: number | "all";
  readonly master: string | undefined;
}

export function parseRedisUrl(url: string): RedisLocation {
  const invalid = (why: string) => new ConfigError(`${url}: ${why}`);
  const at = url.indexOf("://");
  const scheme = at < 0 ? undefined : SCHEMES[url.slice(0, at)];
  if (scheme === undefined) {
    throw invalid(`not a Redis URL; the schemes are ${Object.keys(SCHEMES).join(", ")}`);
  }
  const { topology, tls } = scheme;
  let rest = url.slice(at + 3);
  if (rest.includes("#")) throw invalid("a store URL takes no fragment");
  const q = rest.indexOf("?");
  const queryText = q < 0 ? "" : rest.slice(q + 1);
  rest = q < 0 ? rest : rest.slice(0, q);
  const slash = rest.indexOf("/");
  const netloc = slash < 0 ? rest : rest.slice(0, slash);
  const namespace = slash < 0 ? "" : rest.slice(slash + 1);
  if (netloc.includes("@")) throw invalid("credentials go in `credentials`, not in the URL");
  const nodes = netloc.split(",").map((part) => node(part, invalid));
  if (topology === "standalone" && nodes.length !== 1) throw invalid("names one server, host:port");
  if (!NAMESPACE.test(namespace)) {
    throw invalid("names no namespace, or one outside [A-Za-z0-9._-] and /");
  }
  const query = new Map<string, string>();
  const allowed = PARAMETERS[topology];
  // forEach, not for-of: Deno checks with TypeScript 5.9, whose `dom` lib has no iterable
  // URLSearchParams.
  new URLSearchParams(queryText).forEach((value, name) => {
    if (query.has(name)) throw invalid(`${name} is given twice`);
    if (!allowed.includes(name)) throw invalid(`unknown query parameter ${name}`);
    query.set(name, value);
  });
  const missing = allowed.find((name) => !query.has(name));
  if (missing !== undefined) throw invalid(`the query parameter ${missing} is required`);
  const replicas = replicasOf(query.get("replicas") as string, topology, invalid);
  let db = 0;
  const dbText = query.get("db");
  if (dbText !== undefined) {
    if (!/^[0-9]+$/.test(dbText)) {
      throw invalid(`db is a database number, not ${JSON.stringify(dbText)}`);
    }
    db = Number(dbText);
  }
  const master = query.get("master");
  if (master === "") throw invalid("master names the primary the sentinels monitor");
  return { topology, tls, nodes, namespace, db, replicas, master };
}

function node(part: string, invalid: (why: string) => ConfigError): [string, number] {
  const colon = part.lastIndexOf(":");
  let host = colon < 0 ? "" : part.slice(0, colon);
  const port = colon < 0 ? "" : part.slice(colon + 1);
  if (host === "" || !/^[0-9]+$/.test(port) || !(Number(port) > 0 && Number(port) < 65536)) {
    throw invalid(`${JSON.stringify(part)} is not host:port`);
  }
  if (host.startsWith("[")) {
    if (!host.endsWith("]")) throw invalid(`${JSON.stringify(part)} is not host:port`);
    host = host.slice(1, -1);
  }
  return [host, Number(port)];
}

function replicasOf(
  value: string,
  topology: Topology,
  invalid: (why: string) => ConfigError,
): number | "all" {
  if (value === "all") return "all";
  // Sentinel and Cluster promote a replica on failover: a create one of them misses is lost.
  if (topology !== "standalone") {
    throw invalid("replicas is all on Sentinel and Cluster, so a failover keeps every create");
  }
  if (!/^[0-9]+$/.test(value)) {
    throw invalid(`replicas is a number or all, not ${JSON.stringify(value)}`);
  }
  return Number(value);
}
