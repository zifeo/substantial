# Steps and waits

A run executes the workflow function from its first line every time it wakes up, and a step already recorded returns its recorded value at once, without running (I-1). A crash loses only the steps running at that moment, so they run again (I-1). The host runs each attempt and reports its outcome; which attempt comes next, and when, is the core's.

## Attempts

**§ <a id="ftq7bbgff">ftq7bbgff</a> Steps run at least once: a step attempt whose outcome was not committed runs again on the worker that takes the run, after its effect may already have happened, and a recorded step never runs again.**

- (a) An attempt runs again when its worker crashed, paused past its claim, or shut down past its grace before its outcome was committed.

**§ <a id="a03w2yqkz">a03w2yqkz</a> A step without a retry policy has one attempt, and one with a policy backs off exponentially between attempts, from `initial`, times `multiplier`, up to `max`, for at most `max_attempts`.**

- (a) A failure that is not retryable fails the step at once with `StepFailed`.

**§ <a id="dzbz2dz7v">dzbz2dz7v</a> An undo runs as the step `<step>.undo`, at the occurrence of the step it undoes.**

## Waits

A sleep's end and a wait's timeout are fixed when the call first runs, so a replay never sleeps again and a loop that waits again after each message gets a new timeout from each call, which makes a debounce (I-8; [Integration](../../integration.md#a-durable-debounce)).

**§ <a id="eqpe8dchq">eqpe8dchq</a> A sleep's end is fixed when it first runs, and its firing is recorded, so a replay never sleeps again and a worker with a slow clock replays it the same.**

**§ <a id="j6dvbbdrq">j6dvbbdrq</a> A `wait_until`'s timeout is fixed when the call first runs, and a wait whose timeout passes before its predicate held resolves false.**

- (a) A `receive` with a timeout is a `wait_until` on the messages of its name, its timeout fixed when it first runs.
