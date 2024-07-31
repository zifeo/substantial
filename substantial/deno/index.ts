// import * as exp from "./host/host.mjs";
// import { instantiate } from "./gen/substantial.js";

// const { backend } = await instantiate({ ... exp})
// await backend.readEvents("fs", "run_id");


import { backend } from "./gen/substantial.js";

backend.readEvents("fs", "run_id");
