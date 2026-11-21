# Cost and charges in each host

On S3 a run costs its requests, and a step may cost money of its own ([Interfaces](../interfaces.md#i-27-cost-and-charges)). Which requests each verb makes is the core's, and the per-verb tables are its contract ([qashr34zn](core/cost.md#qashr34zn)). The core counts a run's requests from the answers of the operations it makes anyway, prices them with each worker's own prices, sums the steps' charges into the chain's cost and checks budgets at live points ([the core's cost](core/cost.md)). Each host decides what reaches the core: the price tables, the charges of each attempt, the shape of a budget and which budget a start takes, and the money `observe` reports.

Amounts are integers in every host's API, nano-units, never strings. Only the JSON the core gives writes them as decimal strings, "as they may pass 2^53" (`python/substantial/cost.py`, `typescript/src/cost.ts`): past it a JSON number read in TypeScript, where "a TypeScript number is already a double" ([Interfaces](../interfaces.md#i-17-starting-and-following-runs)), is no longer exact, and as strings "no host rounds them" ([Interfaces](../interfaces.md#i-27-cost-and-charges)).

## Amounts

**§ <a id="hzh3dr6yh">hzh3dr6yh</a> Amounts are integers in nano-units of their currency in every host, a Python `int`, a TypeScript `bigint` and a Rust `u64`, read from the decimal strings of the core's JSON.**

## Charges

A code outside ISO 4217, or a charge that would take the chain's total past 2^63 - 1, fails the step at once, a check the core makes ([ap5tfnrja](core/cost.md#ap5tfnrja)).

**§ <a id="cvnf47vy8">cvnf47vy8</a> `s.charge(currency, nano)` inside a step adds to the spend of the attempt running, which the host hands the core with that attempt's outcome.**

- (a) A negative amount, or one past 2^63 - 1, raises `InvalidArgument` at the call, and in TypeScript so does a `number`.

## Prices

Each shipped table cites its source, the AWS price list of its date, in a comment in each host, and its values are in the reference (I-27). "Each worker prices the requests it counts with its own prices, so totals never go down when workers' prices differ" ([Interfaces](../interfaces.md#i-27-cost-and-charges)); a currency outside ISO 4217 is refused when a worker starts, and what else `prices` refuses is in the same section.

How `prices` is given, a named table whose fields may be overridden or every field, is in the same section.

`observe` reports the money of each store operation as the core counts it: "a request costing its share rounded up" ([Interfaces](../interfaces.md#i-27-cost-and-charges), [ap5tfnrja.b](core/cost.md#ap5tfnrja)).

**§ <a id="vg9yp9xsf">vg9yp9xsf</a> A `StoreOp`'s `cost` prices each of its `attempts` as one request of its kind, the price per 1,000 divided by 1,000 and rounded up, in the currency of the prices of the `Substantial` that made it, and is none without `prices`.**

## Budgets

The core compares the chain's cost with its budget at live points ([j2xzy4akb](core/cost.md#j2xzy4akb)), and a run past its budget raises `BudgetExceeded` ([fkgsx3gvk](core/cost.md#fkgsx3gvk)), while "steps already running finish, their charges recorded" ([Interfaces](../interfaces.md#i-28-budgets)). A child's budget is in [Chains](chains.md).

What a budget's meters and limits may be, and what is refused, is in the reference (I-28). Which budget a start takes is in [Chains](chains.md#evdz71nzb).
