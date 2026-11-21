# Guest workflows

A workflow written for one occasion, such as the procedure of one experiment, should be published like a file and run at once, without a deploy of any worker. So the Rust host also runs workflows compiled to WebAssembly and loaded at run time, the guests. At run time a worker then holds two modules: the core, as for every workflow ([fsvns3j6x](hosts.md#fsvns3j6x)), and the guest, whose code the host replays against the core as it replays its own. A guest is code the worker's owner did not write, so it reaches nothing but through the host, which records each effect and decides what the guest may call.

## The world

A guest is a component of the WebAssembly component model rather than a plain module with an ABI of its own, as the core is: the component model gives the boundary typed functions and resources described once in WIT, which every language with a component toolchain binds, so a guest in JavaScript or Python later needs no new boundary. The cost is a larger toolchain than a plain module's for each language, and a Rust guest is componentized after it is compiled.

**§ <a id="g2h6bp5vq">g2h6bp5vq</a> A guest is a component of the world `substantial:guest/guest`, described in `wit/`, which imports Substantial's verbs and exports its workflow and its steps through the interface `workflow`.**

- (a) The world's package carries its version, and a change that a guest compiled against the previous one cannot load is a new version of the package.

The world imports nothing of WASI. A Rust guest compiles to `wasm32-unknown-unknown`, whose standard library imports nothing, rather than `wasm32-wasip2`, whose standard library imports WASI's clocks, random and streams: a guest then has no clock or randomness the history does not record, by construction. JavaScript and Python guests, built by ComponentizeJS and componentize-py, import WASI; their host will provide those interfaces answered from the history or trapping, a decision taken when they come.

### WASI preview 2, not 3

WASI preview 3 and the component model's async ABI let a guest call the host asynchronously, with `future` and `stream` types, which would map Substantial's pending calls one to one. Wasmtime 50, the version the host runs, documents that support as very incomplete, and the guest toolchains of the other languages are built on preview 2. So guests use preview 2's synchronous ABI: a call of the guest blocks the guest, and the host answers it asynchronously, its future suspending the guest's stack on a fiber until it is ready. The calls below keep the shape of preview 3's futures, so moving to it is a new version of the world and not a new model.

**§ <a id="g1k3x3gg5">g1k3x3gg5</a> A guest calls the host through the synchronous ABI of WASI preview 2, and a call the host cannot answer yet suspends the guest's stack until it can, without holding a thread.**

## Calls and waits

The code of every host runs branches in parallel, two steps at once or a message raced against a timer, and the core records the order their outcomes reach the code ([k3zma9hjs](hosts.md#k3zma9hjs)). A guest whose every call blocked until answered would run one call at a time: simpler, but a guest's `join` of two steps would run them one after the other, and a race would not exist. So a verb returns a pending call at once, as WASI's pollables do, and one blocking `wait` takes the calls the guest awaits.

**§ <a id="em5h7z7cg">em5h7z7cg</a> A guest's verb returns a pending call at once, and its `wait` blocks until one of the calls it names has ended, so a guest runs branches in parallel as the code of every host does.**

- (a) The host makes a guest's call with the core when the guest makes it, so the calls reach the core in the guest's order.

- (b) `wait` answers the position of every call it names that has ended, in the order named.

## Steps

A step attempt runs as a task of its own while the replay waits for it, and the replay polls the code only between two answers of the core. A step body inside the instance that runs the code would then wait for a poll that never comes, and one body stuck in an effect would hold every other branch. So a step runs in an instance of its own, given its name and its arguments: an attempt that is abandoned drops its instance at once, attempts run in parallel, and the workflow code never sees an effect. The cost is that a guest's step is a declared function of arguments that encode to JSON: a closure over the code's variables cannot cross into another instance, so a guest has no inline step, and each attempt pays an instantiation.

**§ <a id="sfd6dmd7t">sfd6dmd7t</a> Each attempt of a guest's step runs in an instance of the guest of its own, through the export `step`, given the step's name and its arguments as JSON, and the instance that runs the workflow code runs no step.**

- (a) A guest's step is a function declared as a step, whose arguments encode to JSON.

- (b) An attempt the worker abandons drops its instance, which stops its code at once.

- (c) An attempt's `step-error` fails it with the failure type `Error`, retryable unless permanent, and a value that is not JSON fails it with `Json`, not retryable.

**§ <a id="h85pj12ap">h85pj12ap</a> A guest's workflow code calls only Substantial's verbs, and its steps call the interfaces the embedder adds, so a replay repeats no effect and every effect is a step the history records.**

- (a) A call of an embedder's interface from the workflow code traps, and so does a call of a workflow verb from a step.

## What a guest may import and export

An embedder, a control plane that runs guests, adds interfaces of its own beside Substantial's, the effects its guests may have, and may call exports of its own on a guest, such as one that describes it, outside any run. The host checks the imports once, when it loads the guest, so that a guest that wants more than it is given fails before any run starts.

**§ <a id="dt9hm67zy">dt9hm67zy</a> The host loads a guest only if every interface it imports is one of Substantial's or one the embedder adds, and refuses any other with `ConfigError`, so a guest reaches the network, a clock or files only through them.**

**§ <a id="symbbqcxc">symbbqcxc</a> A guest exports Substantial's interface `workflow`, which the host calls, beside any interface of the embedder's own, which the embedder calls itself.**

The embedder adds its interfaces to a wasmtime linker, so the Rust host's API names wasmtime types and its major version follows wasmtime's for guests: a wrapper of its own would hide what an embedder's host functions need, at the price of a second API to keep.

## Guests loaded after the worker started

A worker serves the workflow names it is given when it starts, and a guest published later, such as the procedure of one experiment, should run without a restart. So an embedder serves its guests through one workflow of its own: its run's input names the guest, by the digest of its bytes, and its code loads that guest and runs the guest's code within the run. The input is recorded, so every replay of the run finds the same guest and replays the same code. The cost is that every guest served so shares that workflow's name, its retention and its listing, and that the embedder loads and checks each guest itself, caching it by its digest, as loading compiles it. A worker learning new workflow names at run time was the other way, at the price of a new worker API in every host.

**§ <a id="xvn8jfd26">xvn8jfd26</a> A workflow of the embedder's own may run a guest's workflow code within its own run, given the input it chooses, so that one workflow serves every guest its runs name.**

## Replaying a guest

A replay calls its workflow function anew ([g4kv2kj51](promises.md#g4kv2kj51)) and drops its code where it waits ([d9818h4m4](workflows.md#d9818h4m4)). For a guest, that is an instance per replay, dropped when the run suspends, and an instance per attempt: a guest's memory never carries a value from one wakeup to the next that its history does not hold, so a guest replays the same on any worker.

**§ <a id="fqyh5tkhx">fqyh5tkhx</a> The host instantiates a guest anew for every replay and every step attempt, so nothing of a guest's memory outlives them but what its history records.**

WebAssembly computes the same everywhere but for the bits of a NaN, relaxed SIMD and threads. A guest's workflow code branching on a NaN's bits would replay otherwise on another processor. One more difference stays: the depth a guest may recurse to depends on the worker's processor, as the stack bound counts native frames, so a recursion near it may trap on one architecture and not on another.

**§ <a id="jgv9ssc51">jgv9ssc51</a> The host compiles every guest so that it computes the same on every worker: NaNs canonical, relaxed SIMD deterministic, and no threads.**

## Bounds

A guest that loops forever would hold its replay's task, and one that allocates without end the worker's memory. Fuel counts a guest's instructions, so a bound by fuel traps at the same point on every worker, but every instruction pays for its count. Epochs interrupt a guest once a clock thread ticks past its deadline, at a cost close to nothing, but by time: a replay near its bound may trap on a slow worker and not on a fast one. Epochs are taken, with the bound set well above what the code needs, so that a trap means a guest stuck. The embedder gives the bounds rather than the host choosing them, as the right ones depend on its guests. An instance's memory is its linear memories and its tables together, a pointer for each table element, as a component may hold several of each.

**§ <a id="p9d3qccma">p9d3qccma</a> A guest runs within the bounds its embedder gives when it loads it: a memory for each of its instances, and a time for its workflow code between two waits.**

- (a) An instance whose memory would grow past its bound traps.

- (b) The workflow code traps once it runs past its time without a wait, its time counted anew from each wait.

- (c) A step attempt yields to its worker at each tick of the epoch clock, so that its timeout abandons it as it abandons any step.

## Values and ends

Every value between a guest and the host is JSON text rather than WIT types of each workflow: the world stays the same for every workflow, as the values are stored as JSON anyway ([e1dc4dar4](values.md#e1dc4dar4)). The cost is one encoding on each side, and a guest's types are checked by its own SDK rather than by the component model.

**§ <a id="c0mcah841">c0mcah841</a> Every value between a guest and the host is JSON text, and the host checks a guest's values as it checks its own code's, refusing one that is not JSON as a `Json` failure.**

A Rust panic stops the worker, as a bug of the worker ([pgv9r1j12](workers.md#pgv9r1j12)). A guest's trap is a bug of the guest, and a worker of a control plane serves the guests of many owners, so a trap ends what it was running and the worker goes on. The cost is that a trap that would pass, such as one from memory exhausted, is not retried.

**§ <a id="a3qbj1ttt">a3qbj1ttt</a> A trap of a guest fails what it was running with the failure type `GuestTrap`, and the worker goes on serving its other runs.**

- (a) A trap of the workflow code fails its run.

- (b) A trap of a step attempt fails the attempt, not retryable.

The errors the verbs raise carry more than a failure: a `StepFailed` its cause, a `Cancelled` the end it asks for ([b7wsejcvb](errors.md#b7wsejcvb)). The host keeps each error it gives a guest, which the guest sees as its failure, so that a guest returning that failure ends the run with the error itself, as the Rust host finds a `StepFailed`'s details by its failure.

**§ <a id="kn3j2hh9y">kn3j2hh9y</a> A guest's workflow that returns an error a verb gave it ends its run as the host's own code returning that error would, and one that returns a failure of its own fails its run with it.**

## The Rust guest SDK

A Rust guest is written as a workflow of the Rust host is, so that moving a workflow into a guest changes its crate and not its code. Its history may differ: a verb's later calls to the core, such as a timed `receive` after its wait, are made while the host waits, before the guest's other branches run, so a guest and the Rust host may record the calls of concurrent branches in other orders. Each replays its own history the same, and a run moves between the two as across versions. Its executor is a loop of its own in the guest: it polls the workflow, then waits on every call the poll left pending.

**§ <a id="b4j0mc19d">b4j0mc19d</a> A Rust guest declares its workflow with `#[workflow]` and its steps with `#[step]`, and calls the verbs of its context with the Rust host's names and types, the guest SDK's `export!` giving the component its workflow and its steps.**

## Open questions

- What a guest does not have yet: children and chains, handlers and `wait_until`, sagas, polls, schedules and loops, charges and cost.
