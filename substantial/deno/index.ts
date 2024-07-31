// import * as exp from "./host/host.mjs";
// import { instantiate } from "./gen/substantial.js";

// const { backend } = await instantiate({ ... exp})
// await backend.readEvents("fs", "run_id");


import { utils } from "./gen/substantial.js";
import { resolve } from "./promises.ts";


const res = await resolve(utils.concatThenUppercase(["one", "two", "three"]));
console.log(res);