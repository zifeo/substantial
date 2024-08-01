import { PromisedResult } from "./gen/substantial.d.ts";
import { utils } from "./gen/substantial.js";
import { globalTasks } from "./host/host.mjs";

export async function resolve(chain: PromisedResult) {
    const item = (globalTasks as any)?.[chain.refHost];
    if (!item) {
        throw new Error(`Fatal: guest called missing "${chain.refHost}" host function`);
    }
    const { asyncFn, args } = item;
    const first = await asyncFn(...args);
    return JSON.parse(utils.evaluateGuest(chain, JSON.stringify(first)));
}
