# Wasm workflows: values and ends

A Wasm workflow ([Wasm workflows](../wasm-workflows.md)) exchanges every value with the runner, and ends its run, through the world of `wit/`.

Every value between a Wasm workflow and the runner is JSON text rather than WIT types of each workflow: the world stays the same for every workflow, as the values are stored as JSON anyway ([e1dc4dar4](../values.md#e1dc4dar4)). The cost is one encoding on each side, and the crate on the module's side, not the component model, checks a Wasm workflow's types.

**§ <a id="ea0fw6qrw">ea0fw6qrw</a> Every value between a Wasm workflow and the runner is JSON text, and both refuse a value of the code as every host does ([z6zyx22ss](../values.md#z6zyx22ss)): the module before the value crosses, and the runner text that is not JSON.**

- (a) ~~A refused message, published value or step's arguments raises `InvalidArgument` at its call, a refused result fails the run with `Json`, and a refused step value fails its attempt with `Json`, not retryable.~~

- (b) A refused message, published value or step's arguments raises `InvalidArgument` at its call, a refused result blocks the run with `Json`, and a refused step value fails its attempt with `Json`, not retryable.

A Rust panic stops the worker, as a bug of the worker ([pgv9r1j12](../workers.md#pgv9r1j12)). A Wasm workflow's trap is a bug of that workflow, and a worker of a control plane serves the Wasm workflows of many owners, so a trap stops what it was running and the worker goes on. A trap of the code blocks its run, as any bug of a workflow's code does ([dxgjekysz](../workflows.md#dxgjekysz)), so a fixed module finishes it. The cost is that a step's trap that would pass, such as one from memory exhausted, is not retried.

**§ <a id="cy53pm5cn">cy53pm5cn</a> ~~A trap of a Wasm workflow fails what it was running with the failure type `WasmTrap`, and the worker goes on serving its other runs.~~**

- (a) ~~A trap of the workflow code fails its run.~~

- (b) ~~A trap of a step attempt fails the attempt, not retryable.~~

- (c) ~~An error that a host function of the embedder returns traps the attempt that called it, so an effect that may pass gives its failure as a value of the embedder's interface.~~

**§ <a id="jy9pjegjy">jy9pjegjy</a> A trap of a Wasm workflow stops what it was running with the failure type `WasmTrap`, and the worker goes on serving its other runs.**

- (a) A trap of the workflow code blocks its run.

- (b) A trap of a step attempt fails the attempt, not retryable.

- (c) An error that a host function of the embedder returns traps the attempt that called it, so an effect that may pass gives its failure as a value of the embedder's interface.

The errors the verbs raise carry more than a failure: a `StepFailed` its cause, a `Cancelled` the end it asks for ([b7wsejcvb](../errors.md#b7wsejcvb)). The runner keeps each error it gives a Wasm workflow, which the workflow sees as its failure, so that a Wasm workflow returning that failure ends the run with the error itself, as the Rust host finds a `StepFailed`'s details by its failure.

**§ <a id="q2x1ma914">q2x1ma914</a> A Wasm workflow that returns an error a verb gave it ends its run as the host's own code returning that error would, and one that returns a failure of its own fails its run with it.**

- (a) A failure of its own of type `Json`, `InvalidArgument` or `NonDeterminism`, which the module's own checks raise without the runner, blocks its run, as that error of the host's own code does.
