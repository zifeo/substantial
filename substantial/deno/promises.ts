import { utils } from "./gen/substantial.js";
import { globalTasks } from "./host/host.mjs";

export async function resolve(chain: any) {
    const { asyncFn, args }  = (globalTasks as any)?.[chain.refHost];
    const first = await asyncFn(...args);
    return JSON.parse(utils.evaluateGuest(chain, JSON.stringify(first)));
}
