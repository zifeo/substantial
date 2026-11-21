# Wasm workflows

A workflow written for one occasion, such as the procedure of one experiment, should be published like a file and run at once, without a deploy of any worker. So a workflow also compiles to WebAssembly, a Wasm workflow, which the Wasm runner of the Rust host loads at run time. At run time a worker then holds two modules: the core, as for every workflow ([fsvns3j6x](hosts.md#fsvns3j6x)), and the Wasm workflow's, whose code the runner replays against the core as the host replays its own. A Wasm workflow is code published apart from its worker, so it reaches nothing but through the runner, which records each effect and decides what the workflow may call.

The core stays in the worker rather than in each Wasm workflow. A Wasm workflow published by its digest lives as long as its runs: with the core inside it, a namespace would hold every core version its Wasm workflows were built with, for as long as they run, and a fix of the core would reach none of them. With the core in the worker, a deploy of the worker moves every run to the new core at once, and the boundary a Wasm workflow is built against is the verbs, the API every host already keeps. The cost is a second crossing for each verb, from the workflow to the runner and from the runner to the core: a few microseconds, measured beside a native verb by a test of the runner, against a store operation of milliseconds.

## The world

A Wasm workflow is a component of the WebAssembly component model rather than a plain module with an ABI of its own, as the core is: the component model gives the boundary typed functions and resources described once in WIT, which every language with a component toolchain binds. The cost is a larger toolchain than a plain module's, and a Rust Wasm workflow is made a component after it is compiled.

**§ <a id="j67tqxstm">j67tqxstm</a> A Wasm workflow is a component of the world `substantial:wasm/module`, described in `wit/`, which imports Substantial's verbs and exports the workflow and its steps through the interface `workflow`.**

- (a) The world's package carries its version, and a change that a Wasm workflow compiled against the previous one cannot load is a new version of the package.

The world imports nothing of WASI. A Rust Wasm workflow compiles to `wasm32-unknown-unknown`, whose standard library imports nothing, rather than `wasm32-wasip2`, whose standard library imports WASI's clocks, random and streams: a Wasm workflow then has no clock or randomness the history does not record, by construction. Wasm workflows are written in Rust: the toolchains of JavaScript and Python, ComponentizeJS and componentize-py, embed an interpreter in each module and import WASI, which a runner would answer from the history or refuse.

### WASI preview 2, not 3

WASI preview 3 and the component model's async ABI let a component call its host asynchronously, with `future` and `stream` types, which would map Substantial's pending calls one to one. Wasmtime 50, the version the runner uses, documents that support as very incomplete. So Wasm workflows use preview 2's synchronous ABI: a call of the workflow blocks it, and the runner answers it asynchronously, its future suspending the workflow's stack on a fiber until it is ready. The calls below keep the shape of preview 3's futures, so moving to it is a new version of the world and not a new model.

**§ <a id="gmrzmgpca">gmrzmgpca</a> A Wasm workflow calls the runner through the synchronous ABI of WASI preview 2, and a call the runner cannot answer yet suspends the workflow's stack until it can, without holding a thread.**

## Calls and waits

The code of every host runs branches in parallel, two steps at once or a message raced against a timer, and the core records the order their outcomes reach the code ([k3zma9hjs](hosts.md#k3zma9hjs)). A Wasm workflow whose every call blocked until answered would run one call at a time: simpler, but its `join` of two steps would run them one after the other, and a race would not exist. So a verb returns a pending call at once, as WASI's pollables do, and one blocking `wait` takes the calls the workflow awaits.

**§ <a id="gtezqzzdn">gtezqzzdn</a> A Wasm workflow's verb returns a pending call at once, and its `wait` blocks until one of the calls it names has ended, so a Wasm workflow runs branches in parallel as the code of every host does.**

- (a) The runner makes a Wasm workflow's call with the core when the workflow makes it, so the calls reach the core in the workflow's order.

- (b) `wait` answers the position of every call it names that has ended, in the order named.

## Steps

A step attempt runs as a task of its own while the replay waits for it, and the replay polls the code only between two answers of the core. A step body inside the instance that runs the code would then wait for a poll that never comes, and one body stuck in an effect would hold every other branch. So a step runs in an instance of its own, given its name and its arguments: an attempt that is abandoned drops its instance at once, attempts run in parallel, and the workflow code never sees an effect. The cost is that a Wasm workflow's step is a declared function of arguments that encode to JSON: a closure over the code's variables cannot cross into another instance, so a Wasm workflow has no inline step, and each attempt pays an instantiation.

**§ <a id="ztt9a58wy">ztt9a58wy</a> Each attempt of a Wasm workflow's step runs in an instance of the module of its own, through the export `step`, given the step's name and its arguments as JSON, and the instance that runs the workflow code runs no step.**

- (a) A Wasm workflow's step is a function declared as a step, whose arguments encode to JSON.

- (b) An attempt the worker abandons drops its instance, which stops its code at once.

- (c) An attempt's `step-error` fails it with the failure type `Error`, retryable unless permanent, and a value that is not JSON fails it with `Json`, not retryable.

**§ <a id="mry4d64zv">mry4d64zv</a> A Wasm workflow's code calls only Substantial's verbs, and its steps call the interfaces the embedder adds, so a replay repeats no effect and every effect is a step the history records.**

- (a) A call of an embedder's interface from the workflow code traps, and so does a call of a workflow verb from a step.

## What a Wasm workflow may import and export

An embedder, an application that embeds the runner such as a control plane, adds interfaces of its own beside Substantial's, the effects its Wasm workflows may have, and may call exports of its own on a Wasm workflow, such as one that describes it, outside any run. The runner checks the imports once, when it loads the Wasm workflow, so that one that wants more than it is given fails before any run starts.

**§ <a id="de970qkh4">de970qkh4</a> The runner loads a Wasm workflow only if every interface it imports is one of Substantial's or one the embedder adds, and refuses any other with `ConfigError`, so a Wasm workflow reaches the network, a clock or files only through them.**

**§ <a id="jf9zy7nf7">jf9zy7nf7</a> A Wasm workflow exports Substantial's interface `workflow`, which the runner calls, beside any interface of the embedder's own, which the embedder calls itself.**

The embedder adds its interfaces to a wasmtime linker, so the runner's API names wasmtime types and its major version follows wasmtime's: a wrapper of its own would hide what an embedder's host functions need, at the price of a second API to keep. The runner is a library the embedder builds into its own worker, not a program of its own, as only the embedder can give its interfaces.

## Wasm workflows loaded after the worker started

A worker serves the workflow names it is given when it starts, and a Wasm workflow published later, such as the procedure of one experiment, should run without a restart. So an embedder serves its Wasm workflows through one workflow of its own: its run's input names the Wasm workflow, by the digest of its bytes, and its code loads that module and runs its code within the run. The input is recorded, so every replay of the run finds the same module and replays the same code. The cost is that every Wasm workflow served so shares that workflow's name, its retention and its listing, and that the embedder loads and checks each module itself, caching it by its digest, as loading compiles it. A worker learning new workflow names at run time was the other way, at the price of a new worker API in every host.

**§ <a id="c53nbqaa7">c53nbqaa7</a> A workflow of the embedder's own may run a Wasm workflow's code within its own run, given the input it chooses, so that one workflow serves every Wasm workflow its runs name.**

## Replaying a Wasm workflow

A replay calls its workflow function anew ([g4kv2kj51](promises.md#g4kv2kj51)) and drops its code where it waits ([d9818h4m4](workflows.md#d9818h4m4)). For a Wasm workflow, that is an instance per replay, dropped when the run suspends, and an instance per attempt: its memory never carries a value from one wakeup to the next that its history does not hold, so it replays the same on any worker.

**§ <a id="s3vk5gvsj">s3vk5gvsj</a> The runner instantiates a Wasm workflow anew for every replay and every step attempt, so nothing of its memory outlives them but what its history records.**

WebAssembly computes the same everywhere but for the bits of a NaN, relaxed SIMD and threads. A Wasm workflow branching on a NaN's bits would replay otherwise on another processor. One more difference stays: the depth a Wasm workflow may recurse to depends on the worker's processor, as the stack bound counts native frames, so a recursion near it may trap on one architecture and not on another.

**§ <a id="ad2tnbnvg">ad2tnbnvg</a> The runner compiles every Wasm workflow so that it computes the same on every worker: NaNs canonical, relaxed SIMD deterministic, and no threads.**

## Bounds

A Wasm workflow that loops forever would hold its replay's task, and one that allocates without end the worker's memory. Fuel counts a module's instructions, so a bound by fuel traps at the same point on every worker, but every instruction pays for its count. Epochs interrupt a module once a clock thread ticks past its deadline, at a cost close to nothing, but by time: a replay near its bound may trap on a slow worker and not on a fast one. Epochs are taken, with the bound set well above what the code needs, so that a trap means a workflow stuck. The embedder gives the bounds rather than the runner choosing them, as the right ones depend on its Wasm workflows. An instance's memory is its linear memories and its tables together, a pointer for each table element, as a component may hold several of each.

**§ <a id="jrg0gw7y7">jrg0gw7y7</a> A Wasm workflow runs within the bounds its embedder gives when it loads it: a memory for each of its instances, and a time for its workflow code between two waits.**

- (a) An instance whose memory would grow past its bound traps.

- (b) The workflow code traps once it runs past its time without a wait, its time counted anew from each wait.

- (c) A step attempt yields to its worker at each tick of the epoch clock, so that its timeout abandons it as it abandons any step.

## Values and ends

Every value between a Wasm workflow and the runner is JSON text rather than WIT types of each workflow: the world stays the same for every workflow, as the values are stored as JSON anyway ([e1dc4dar4](values.md#e1dc4dar4)). The cost is one encoding on each side, and a Wasm workflow's types are checked by the crate on its side rather than by the component model.

**§ <a id="ks041gbgm">ks041gbgm</a> Every value between a Wasm workflow and the runner is JSON text, and the runner checks a Wasm workflow's values as the host checks its own code's, refusing one that is not JSON as a `Json` failure.**

A Rust panic stops the worker, as a bug of the worker ([pgv9r1j12](workers.md#pgv9r1j12)). A Wasm workflow's trap is a bug of that workflow, and a worker of a control plane serves the Wasm workflows of many owners, so a trap ends what it was running and the worker goes on. The cost is that a trap that would pass, such as one from memory exhausted, is not retried.

**§ <a id="cy53pm5cn">cy53pm5cn</a> A trap of a Wasm workflow fails what it was running with the failure type `WasmTrap`, and the worker goes on serving its other runs.**

- (a) A trap of the workflow code fails its run.

- (b) A trap of a step attempt fails the attempt, not retryable.

The errors the verbs raise carry more than a failure: a `StepFailed` its cause, a `Cancelled` the end it asks for ([b7wsejcvb](errors.md#b7wsejcvb)). The runner keeps each error it gives a Wasm workflow, which the workflow sees as its failure, so that a Wasm workflow returning that failure ends the run with the error itself, as the Rust host finds a `StepFailed`'s details by its failure.

**§ <a id="q2x1ma914">q2x1ma914</a> A Wasm workflow that returns an error a verb gave it ends its run as the host's own code returning that error would, and one that returns a failure of its own fails its run with it.**

## One crate, two targets

A Wasm workflow in Rust is written with the crate `substantial` itself, so that the same source runs as a native workflow and as a Wasm workflow, and moving a workflow from one to the other changes its build and not its code. Built for wasm32, the crate holds its API and the module's side: the bindings of the world and an executor of its own, a loop that polls the workflow, then waits on every call the poll left pending. Built natively, it holds the host, and the Wasm runner behind a feature. The module's side is a crate of its own inside, `substantial-wasm-sdk`, which `substantial` re-exports for wasm32 and which a user does not name.

The two may record a run's history in different orders: a verb's later calls to the core, such as a timed `receive` after its wait, are made while the runner waits, before the workflow's other branches run, so a Wasm workflow and native code may record the calls of concurrent branches in other orders. Each replays its own history the same, and a run moves between the two as across versions. The error type also differs by target: the native one holds the host's own failures, of stores and configuration among them, which no Wasm workflow sees, so code matching one of those compiles only natively.

**§ <a id="s9ynfgdqm">s9ynfgdqm</a> A workflow's source compiles both to a native workflow and to a Wasm workflow with the crate `substantial`, which offers both the same API: `#[workflow]`, `#[step]`, `export!`, and the verbs of its context with the host's names and types.**

- (a) Built for wasm32, the crate holds the API, the bindings of the world and the verbs a Wasm workflow has, and code calling another verb fails to compile for wasm32.

- (b) `export!` gives the module its workflow and its steps, and natively only checks that they exist.

- (c) The Wasm runner is the feature `wasm-runner` of the native crate, under `substantial::wasm`.

## Open questions

- What a Wasm workflow does not have yet: children and chains, handlers and `wait_until`, sagas, polls, schedules and loops, charges and cost.
- Wasm workflows in another language than Rust: Go, through TinyGo and wit-bindgen-go, builds components without an interpreter, and is the candidate if one is needed.
