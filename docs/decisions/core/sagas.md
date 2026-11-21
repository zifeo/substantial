# Undos, rollbacks and sagas

A step's `undo=` says how it is undone; a rollback undoes the steps of its run so far, and a saga only the steps inside it (I-10). The host runs the undos the core lists, in order, and raises the error; which undos, in which order, and what leaves the stack is the core's.

**§ <a id="ej5k8q6w5">ej5k8q6w5</a> A rollback lists the undos of every step of the run still on the stack, most recent first.**

- (a) The earlier runs of the chain stay as they ended.

**§ <a id="sft633bnn">sft633bnn</a> A saga that ends on an error lists the undos of its own steps only, most recent first, and those steps leave the stack, so a later rollback does not undo them twice.**

- (a) A saga that succeeds hands its steps to the saga around it, or to a later rollback.

- (b) A step called in a saga that ended is refused as `INVALID`.
