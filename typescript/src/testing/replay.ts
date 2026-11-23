// The replay tester: recorded runs copied from a store into an environment, replayed by new code
// against their histories, for CI before a deploy.
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  InvalidArgument,
  LayoutError,
  NonDeterminism,
  onlyOptions,
  ProtocolError,
  RunNotFound,
  StoreError,
} from "../errors.ts";
import { layout } from "../proto.ts";
import type { CredentialsArg } from "../stores/credentials.ts";
import { openStore } from "../stores/open.ts";
import { type Store, StoreFailure } from "../stores/store.ts";
import type { AnyWorkflow } from "../workflow.ts";
import { Env, worldOf } from "./env.ts";
import { endedHead } from "./world.ts";

/** A run the replay tester replayed: `matched` when the code made every call its history
 * recorded, up to where it waits; `diverged`, with the `NonDeterminism` a worker of this code
 * would meet; `ended` when it had ended, so no worker replays it again. */
export interface Replayed {
  readonly run: string;
  readonly outcome: "matched" | "diverged" | "ended";
  readonly error?: NonDeterminism;
}

export interface ReplayOptions {
  /** The ids of the runs to replay: for a chain that continued, its run that is live
   * (`RunInfo.run`). */
  readonly runs: readonly string[];
  /** How the source store authenticates, as for `Substantial.open`. */
  readonly credentials?: CredentialsArg;
}

/** Replays the runs `runs` of the store at `source` with the code of `workflows`: each run is
 * copied into an environment's namespace in memory, by get and list alone, and replayed there up
 * to the end of its history, with no step run. */
export async function replay(
  source: string,
  workflows: Iterable<AnyWorkflow>,
  options: ReplayOptions,
): Promise<Replayed[]> {
  // § awhhtbr5n@hkk0j
  onlyOptions(options, ["runs", "credentials"], "replay");
  const { runs, credentials } = options;
  if (!Array.isArray(runs) || runs.length === 0) {
    throw new InvalidArgument("a replay names at least one run");
  }
  const code = [...workflows];
  const from = await openStore(source, credentials);
  try {
    await stored(() => from.open());
    const now = Date.now();
    const env = await Env.open({ now: new Date(now) });
    try {
      const world = worldOf(env);
      world.checks = new Map();
      const target: Store = world.memory;
      await sameLayout(from, target);
      const replayed: Replayed[] = [];
      for (const run of runs) {
        // § awhhtbr5n.c@t3g2z
        const head = await headOf(from, run);
        if (head === undefined) throw new RunNotFound(run);
        const start = head.start;
        if (start === undefined) throw new LayoutError(`runs/${run}: a head without start`);
        const served = code.some((w) => w.name === start.workflow && w.version === start.version);
        if (!served) {
          throw new InvalidArgument(
            `run ${run} is of workflow ${JSON.stringify(start.workflow)} version ` +
              `${start.version ?? "none"}, which no workflow given serves`,
          );
        }
        // § end awhhtbr5n.c
        // A run its chain continued, retried or reset past, or one that ended.
        if (head.run !== run || endedHead(head)) {
          replayed.push({ run, outcome: "ended" });
          continue;
        }
        // § awhhtbr5n.d@z3tcd
        // Without its lease, which would hold it for its worker, and its block, which would hold
        // it until its backoff.
        const held = create(layout.HeadSchema, { ...head, lease: undefined, blocked: undefined });
        const bytes = toBinary(
          layout.RunSchema,
          create(layout.RunSchema, { kind: { case: "head", value: held } }),
        );
        await stored(() => target.create(`runs/${start.chain}`, bytes, undefined));
        // § end awhhtbr5n.d
        const segments = String(held.segments.length).padStart(10, "0");
        const due = `queues/${start.workflow}/due/${dueTime(now)}~${start.chain}~s${segments}`;
        await stored(() => target.create(due, new Uint8Array(), undefined));
        const worker = env.worker(code, { preset: "script" });
        try {
          await env.settle();
        } finally {
          worker.crash();
        }
        // A replay that starts no step takes no claim, so the next run's worker would replay this
        // one again, its wakeup still due.
        await stored(() => target.delete(due));
        if (!world.checks.has(run)) {
          const errors = world.errors.map(String).join("; ") || "no worker died";
          throw new ProtocolError(`run ${run} was not replayed: ${errors}`);
        }
        const diverged = world.checks.get(run);
        replayed.push(
          diverged === undefined
            ? { run, outcome: "matched" }
            : { run, outcome: "diverged", error: new NonDeterminism(diverged) },
        );
      }
      return replayed;
    } finally {
      await env.close();
    }
  } finally {
    await from.close();
  }
}

/** `<yyyymmdd>t<hhmmssmmm>z`, the time of a due entry. */
function dueTime(ms: number): string {
  return new Date(ms).toISOString().replace(/[-:.]/g, "").toLowerCase();
}

/** A store failure as the client's `StoreError`. */
async function stored<T>(op: () => Promise<T>): Promise<T> {
  try {
    return await op();
  } catch (e) {
    throw e instanceof StoreFailure ? new StoreError(e.message) : e;
  }
}

/** Refuses a source of another layout than the environment's. */
async function sameLayout(source: Store, target: Store): Promise<void> {
  // § awhhtbr5n.c@rtb82
  const marker = "substantial.json";
  const theirs = (await stored(() => source.get(marker)))?.data;
  const ours = (await stored(() => target.get(marker)))?.data ?? new Uint8Array();
  const text = (bytes: Uint8Array) => new TextDecoder().decode(bytes);
  if (theirs === undefined) {
    throw new LayoutError(`the source names no namespace: it has no ${marker}`);
  }
  if (text(theirs) !== text(ours)) {
    throw new LayoutError(
      `the source's ${marker} is ${text(theirs)}, not this version's ${text(ours)}`,
    );
  }
}

/** The head `runs/<id>` holds, through the pointer of a later run to its chain's head. */
async function headOf(store: Store, id: string): Promise<layout.Head | undefined> {
  const key = `runs/${id}`;
  const found = await decodeRun(store, key);
  if (found === undefined) return undefined;
  if (found.kind.case === "head") return found.kind.value;
  if (found.kind.case !== "pointer") throw new LayoutError(`${key}: neither head nor pointer`);
  const chainKey = `runs/${found.kind.value.chain}`;
  const head = await decodeRun(store, chainKey);
  if (head === undefined) return undefined;
  if (head.kind.case !== "head") throw new LayoutError(`${chainKey}: a pointer to a pointer`);
  return head.kind.value;
}

async function decodeRun(store: Store, key: string): Promise<layout.Run | undefined> {
  const bytes = (await stored(() => store.get(key)))?.data;
  if (bytes === undefined) return undefined;
  try {
    return fromBinary(layout.RunSchema, bytes);
  } catch (e) {
    throw new LayoutError(`${key}: ${e}`);
  }
}
