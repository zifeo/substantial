# Children, chains and loops

A run's history grows with every step, and "every wakeup reads and replays it" ([Interfaces](../interfaces.md#i-12-loops-and-compression)). So a tree of work lives in many runs, each holding its own work and its children's outcomes, "never the whole tree" ([Interfaces](../interfaces.md#i-11-starting-other-workflows)), and a loop sheds its history by continuing as a new run of its chain. A chain is a run and every run it continued into, under its first run's id. What a continuation creates and carries is the core's ([deycjjrap](core/lifecycle.md#deycjjrap)), and so is how a chain is followed to its last run ([hegd7d4bp](core/lifecycle.md#hegd7d4bp)). Each host decides what a run hands the core when it starts a child and when it continues.

## Children

A child started without an id gets one the core derives from its parent's ([yb61qtt3s](core/children.md#yb61qtt3s)). A child's total cost reaches its parent with its end ([m0vtdttae](core/cost.md#m0vtdttae)), and a start by a client takes its budget the same way ([Cost](cost.md)).

**§ <a id="evdz71nzb">evdz71nzb</a> A child takes the budget given to its `c.start`, else its workflow's, never its parent's.**

- (a) A run a client starts without a budget of its own takes its workflow's, else none.

## Loops

"Only the state crosses a compression": a counter or a list kept beside the loop starts again from its value before the loop, so "what the loop needs belongs in the state" ([Interfaces](../interfaces.md#i-12-loops-and-compression)). No host captures the rest: the layout reserves the field of "The workflow's locals, which no host captures" (`proto/substantial/layout/v1/layout.proto`). Messages the run has not consumed are carried by the core ([deycjjrap](core/lifecycle.md#deycjjrap)), but "A handler's list does not cross a compression" ([Interfaces](../interfaces.md#i-12-loops-and-compression)). The threshold is the worker's `continue_as_new_after` ([the presets](workers.md#presets)).

**§ <a id="hy3f1z5jj">hy3f1z5jj</a> At each boundary of `iterate`, once the core says the history passed `continue_as_new_after`, the run continues as a new run of its chain carrying the loop's state, the new run starts the loop from that state with an empty history, and of the code's values only the state crosses.**

- (a) A state that does not serialize to JSON at a compression fails the run with `NotCompressible`, while the full history still exists.

- (b) Code before the loop runs again in each run of the chain, with the input of the run that continued.

## Continuing by hand

A tail call, a workflow whose last act is to run itself again, is `c.continue_as_new` ([Interfaces](../interfaces.md#i-11-starting-other-workflows)). The same workflow keeps the run's version, and another runs with the version it was declared with ([deycjjrap](core/lifecycle.md#deycjjrap), (a)).

**§ <a id="gjnefe4w6">gjnefe4w6</a> `c.continue_as_new(input, workflow=)` ends the run by raising a continuation that holds this input and, for another workflow, that workflow's name and the version it was declared with, or none.**
