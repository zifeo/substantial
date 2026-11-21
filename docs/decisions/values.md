# Values

Every value a run handles is stored in its history (I-15), and a namespace can be shared by workers and clients in all three languages ([README](../../README.md#how-it-works)). Values are JSON: inputs, step values, messages, results (I-2, "Values are JSON"). Each host encodes the values of its own code before the core sees them.

## JSON values

**§ <a id="e1dc4dar4">e1dc4dar4</a> Inputs, step values, messages, published values, loop states and results are stored as JSON, so a value one host wrote is read by every other.**

- (a) A workflow's input is one JSON object, and a workflow without input gets `{}`.

Each host writes a value with its own JSON encoder, and every host refuses what JSON cannot hold the same way, so a value of the code is either written as it is or refused, never written as another value that another host would then read.

## What JSON cannot hold

A JSON encoder may write a value it cannot hold as another one: `JSON.stringify` "would turn NaN into null, a Map into {} and a Date into a string through its toJSON, drop undefined in an object and write a hole of a sparse array as null" (`typescript/src/proto.ts`), Python's `json` "would write a NaN as `NaN`, a key that is not a string as one" and "a tuple as a list" (`python/substantial/_proto.py`), and serde_json writes "a NaN or an infinity inside a value as `null`" (`rust/src/workflow/canonical.rs`). An integer past 2^53 - 1 that Python or Rust writes exactly is read rounded by TypeScript, whose `JSON.parse` gives a double, so it is refused as an input is ([aaev3rs1q.a](#aaev3rs1q)).

**§ <a id="z6zyx22ss">z6zyx22ss</a> Every host refuses a NaN or an infinity anywhere in a value of the code, and the values its clause below names, rather than write them as other values.**

- (a) Python refuses a dict key that is not a string, a tuple, which `json` writes as a list, and any object its `json` module cannot encode.

- (b) TypeScript refuses, before any `toJSON` runs, an object that is neither a plain object nor an array, such as a `Date` or a `Map`, `undefined` inside an object or an array, a hole of a sparse array, a function, a symbol and a `bigint`.

- (c) A refused message, published value or child input raises `InvalidArgument` at its call.

- (d) Rust refuses a NaN or an infinity anywhere in a value, which serde_json would write as `null`.

- (e) Python and Rust refuse an integer past 2^53 - 1 anywhere in a value, which a TypeScript reader would round; TypeScript, whose numbers are doubles, holds such an integer only as a `bigint`, refused by (b).

A refused step value fails its step with cause `Json` ([epxcjcz4s](steps.md#epxcjcz4s)), and a refused result fails its run with `Json` ([the replay drive](workflows.md#ends)). A value the code gives with nothing in it, a TypeScript `undefined` or a Rust `()`, is written `null`. A Rust map whose keys are not strings is written with its keys as strings, which its type reads back as they were.

## Size

Values are stored inline, and a value far larger than the cap traps the core: "Far below what traps the core: a value of about 1.5 GB exhausts its 4 GiB of wasm32 memory" (`python/substantial/_proto.py`), and a trap "then fails every run of its worker" (`typescript/src/proto.ts`). The cap bounds each value, not a run's history, "which every replay reads whole" (I-15). The core checks each value again ([w5rs0gck0](core/history.md#w5rs0gck0)).

**§ <a id="h8wg39cct">h8wg39cct</a> Every host checks a value against the core's cap itself, before the value reaches the core, and refuses one past it with `ValueTooLarge`.**

- (a) A client's `start` or `send` refuses it before any store operation.

- (b) A step's value over the cap fails the step at once, as a permanent error: `StepFailed`, whose `cause` is `ValueTooLarge`.

- (c) A child's input, a message, a published value or a loop state over the cap raises `ValueTooLarge` at its call.

- (d) A workflow's result over the cap fails the run with `ValueTooLarge`.

## Canonical inputs

A start is deduplicated by comparing its input with the stored one byte for byte ([ct862686r](core/lifecycle.md#ct862686r)), so every host writes an input "as canonical JSON (RFC 8785), as the core compares a start's input byte for byte" (`python/substantial/child.py`). Then "keys in another order, `1.0` for `1`, or the same input from another host is the same input" (I-17). An encoder that silently changes a value would break that: serde_jcs, "as serde_json, writes a NaN or an infinity inside a value as `null`, and rounds an integer past 2^53 - 1 to the double JCS numbers are: both are refused here first" (`rust/src/workflow/canonical.rs`).

**§ <a id="aaev3rs1q">aaev3rs1q</a> Every host encodes an input as canonical JSON (RFC 8785), keys sorted and numbers written as the doubles they are, so the same input from any host or in any key order is the same input.**

- (a) An input JSON cannot hold exactly raises `InvalidArgument`: a NaN or an infinity in every host, an integer past 2^53 - 1 in Python and Rust, a `bigint` or a string with a lone surrogate in TypeScript.

- (b) Children's inputs and `continue_as_new`'s are encoded the same way.

- (c) In Python, an input whose dict has a key that is not a string raises `InvalidArgument`.
