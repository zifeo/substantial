// The requests an adapter sends for one store operation, its retries and read-backs included,
// which the core counts for runs and the observer is told.
import { AsyncLocalStorage } from "node:async_hooks";

/** The requests of the operation being measured. */
export class Sent {
  n = 0;
}

const measuring = new AsyncLocalStorage<Sent>();

/** Counts one request of the operation being measured; called by the adapters' shared retry layer
 * for each attempt it makes. */
export function sent(): void {
  const running = measuring.getStore();
  if (running !== undefined) running.n += 1;
}

/** Performs the store operation `op`, failed or not, counting its requests in `into`. An adapter
 * outside the shared retry layer, the memory store or a test's, sends each operation once. An
 * operation measured inside another, through a store wrapping a store, counts in both. */
export async function measured<T>(into: Sent, op: () => Promise<T>): Promise<T> {
  try {
    return await measuring.run(into, op);
  } finally {
    into.n = Math.max(into.n, 1);
    const outer = measuring.getStore();
    if (outer !== undefined) outer.n += into.n;
  }
}
