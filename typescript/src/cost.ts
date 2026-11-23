// What runs cost (I-27, I-28): the store's prices, the chain's cost as `c.cost()` returns it, and
// budgets.
import { create } from "@bufbuild/protobuf";
import { ConfigError, InvalidArgument, onlyOptions, ProtocolError } from "./errors.ts";
import { layout, run } from "./proto.ts";

/** The largest amount or limit, as the core counts them. */
// § one cvnf47vy8.a@t86v9
export const MAX: bigint = 2n ** 63n - 1n;

const KINDS = ["create", "cas", "get", "list", "delete"] as const;
const REQUESTS = [...KINDS, "requests"] as const;
const FIELDS = ["currency", ...KINDS];

/** A store request kind, which a price and a store operation name. */
export type Kind = (typeof KINDS)[number];

/** The fields of `Prices`: integer nano-units of `currency` per 1,000 requests of each kind. */
export interface PriceFields {
  readonly currency: string;
  readonly create: bigint;
  readonly cas: bigint;
  readonly get: bigint;
  readonly list: bigint;
  readonly delete: bigint;
}

const TABLES: ReadonlyMap<string, PriceFields> = new Map([
  // Source: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json,
  // price list version 20261109154310 (published 2026-11-09), read 2026-11-15: S3 Standard, US
  // East (N. Virginia), "$0.005 per 1,000 PUT, COPY, POST, or LIST requests", "$0.004 per 10,000
  // GET and all other requests"; DELETE is free (https://aws.amazon.com/s3/pricing/, "DELETE and
  // CANCEL requests are free").
  [
    "aws-s3-standard-us-east-1-2026-11-09",
    {
      currency: "USD",
      create: 5_000_000n,
      cas: 5_000_000n,
      get: 400_000n,
      list: 5_000_000n,
      delete: 0n,
    },
  ],
]);

export function isCurrency(code: unknown): code is string {
  return typeof code === "string" && /^[A-Z]{3}$/.test(code);
}

function isAmount(value: unknown): value is bigint {
  return typeof value === "bigint" && value >= 0n && value <= MAX;
}

function shown(value: unknown): string {
  return typeof value === "bigint"
    ? `${value}n`
    : typeof value === "string"
      ? `"${value}"`
      : String(value);
}

/** An integer amount of nano-units of a currency. */
export class Amount {
  readonly currency: string;
  readonly nano: bigint;

  constructor(currency: string, nano: bigint) {
    this.currency = currency;
    this.nano = nano;
  }
}

/** What the store charges: integer nano-units of `currency` per 1,000 requests of each kind
 * (`create` and `cas` are S3's PUT class, `get` its GET class, `list` its LIST class). A request
 * costs its share rounded up.
 *
 * `new Prices("aws-s3-standard-us-east-1-2026-11-09", { get: 500_000n })` is a shipped table, whose
 * fields the overrides replace; `new Prices({ currency, create, cas, get, list, delete })` gives every
 * field. */
export class Prices implements PriceFields {
  readonly currency: string;
  readonly create: bigint;
  readonly cas: bigint;
  readonly get: bigint;
  readonly list: bigint;
  readonly delete: bigint;

  constructor(name: string, overrides?: Partial<PriceFields>);
  constructor(fields: PriceFields);
  constructor(named: string | PriceFields, overrides: Partial<PriceFields> = {}) {
    let fields: Record<string, unknown>;
    if (typeof named === "string") {
      const table = TABLES.get(named);
      if (table === undefined) throw new ConfigError(`no price table named "${named}"`);
      onlyOptions(overrides, FIELDS, "prices", ConfigError);
      const given = Object.entries(overrides).filter(([, v]) => v !== undefined);
      fields = { ...table, ...Object.fromEntries(given) };
    } else if (typeof named === "object" && named !== null) {
      onlyOptions(named, FIELDS, "prices", ConfigError);
      const all: Record<string, unknown> = named as unknown as Record<string, unknown>;
      const missing = FIELDS.filter((k) => all[k] === undefined);
      if (missing.length > 0) {
        throw new ConfigError(`prices without a table name need ${missing.join(", ")}`);
      }
      fields = all;
    } else {
      throw new ConfigError(`prices take a table name or their fields, not ${String(named)}`);
    }
    const { currency } = fields;
    if (!isCurrency(currency)) {
      throw new ConfigError(
        `prices: the currency ${shown(currency)} is not three uppercase ASCII letters`,
      );
    }
    for (const kind of KINDS) {
      if (!isAmount(fields[kind])) {
        throw new ConfigError(
          `prices: ${kind} is a bigint from 0n to ${MAX}n, not ${shown(fields[kind])}`,
        );
      }
    }
    this.currency = currency;
    this.create = fields.create as bigint;
    this.cas = fields.cas as bigint;
    this.get = fields.get as bigint;
    this.list = fields.list as bigint;
    this.delete = fields.delete as bigint;
  }
}

/** What `attempts` requests of the store operation `op` cost at `prices`. */
export function priceOf(prices: Prices, op: Kind, attempts: number): Amount {
  // § vg9yp9xsf@eg5b2
  const per1000 = prices[op];
  return new Amount(prices.currency, ((per1000 + 999n) / 1000n) * BigInt(attempts));
}

export function pricesProto(prices: Prices): run.Prices {
  const { currency, create: c, cas, get, list, delete: d } = prices;
  return create(run.PricesSchema, { currency, create: c, cas, get, list, delete: d });
}

/** Store requests by kind, their total, and amounts by currency in nano-units. */
export class Meters {
  readonly create: number;
  readonly cas: number;
  readonly get: number;
  readonly list: number;
  readonly delete: number;
  readonly requests: number;
  readonly amounts: Record<string, bigint>;

  constructor(
    create: number,
    cas: number,
    get: number,
    list: number,
    del: number,
    requests: number,
    amounts: Record<string, bigint>,
  ) {
    this.create = create;
    this.cas = cas;
    this.get = get;
    this.list = list;
    this.delete = del;
    this.requests = requests;
    this.amounts = amounts;
  }
}

/** The chain's cost so far: its own store requests (`store`), its steps' charges by step name
 * (`steps`) and its children's totals (`children`); the top-level fields are the total of the
 * three. */
export class Cost {
  readonly create: number;
  readonly cas: number;
  readonly get: number;
  readonly list: number;
  readonly delete: number;
  readonly requests: number;
  readonly amounts: Record<string, bigint>;
  readonly store: Meters;
  readonly steps: Record<string, Record<string, bigint>>;
  readonly children: Meters;

  constructor(
    create: number,
    cas: number,
    get: number,
    list: number,
    del: number,
    requests: number,
    amounts: Record<string, bigint>,
    store: Meters,
    steps: Record<string, Record<string, bigint>>,
    children: Meters,
  ) {
    this.create = create;
    this.cas = cas;
    this.get = get;
    this.list = list;
    this.delete = del;
    this.requests = requests;
    this.amounts = amounts;
    this.store = store;
    this.steps = steps;
    this.children = children;
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Amounts as the core renders them, decimal strings, as they may pass 2^53. */
function amountsOf(value: unknown, what: string): Record<string, bigint> {
  if (!isObject(value)) throw new ProtocolError(`${what} ${JSON.stringify(value)}`);
  const out: Record<string, bigint> = {};
  // § one hzh3dr6yh@sb015
  for (const [currency, nano] of Object.entries(value)) {
    if (typeof nano !== "string" || !/^[0-9]+$/.test(nano)) {
      throw new ProtocolError(`${what} ${JSON.stringify(value)}`);
    }
    out[currency] = BigInt(nano);
  }
  return out;
}

function metersOf(value: unknown, what: string): Meters {
  if (!isObject(value)) throw new ProtocolError(`${what} ${JSON.stringify(value)}`);
  const [c, x, g, l, d, r] = REQUESTS.map((k) => {
    const n = value[k];
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 0) {
      throw new ProtocolError(`${what} ${JSON.stringify(value)}`);
    }
    return n;
  }) as [number, number, number, number, number, number];
  return new Meters(c, x, g, l, d, r, amountsOf(value.amounts, `${what} amounts`));
}

/** A `Cost` from the JSON the core resolves `c.cost()` with. */
export function costOf(value: unknown): Cost {
  const total = metersOf(value, "cost");
  const steps = (value as Record<string, unknown>).steps;
  if (!isObject(steps)) throw new ProtocolError(`cost steps ${JSON.stringify(steps)}`);
  const byStep: Record<string, Record<string, bigint>> = {};
  for (const [step, amounts] of Object.entries(steps)) {
    byStep[step] = amountsOf(amounts, `cost of step ${step}`);
  }
  const { store, children } = value as Record<string, unknown>;
  return new Cost(
    total.create,
    total.cas,
    total.get,
    total.list,
    total.delete,
    total.requests,
    total.amounts,
    metersOf(store, "cost store"),
    byStep,
    metersOf(children, "cost children"),
  );
}

/** Limits on a run's cost, by meter: `create`, `cas`, `get`, `list`, `delete`, `requests`, or a
 * currency code (`USD`), each a bigint from 0n to 2n ** 63n - 1n. */
export type Budget = Readonly<Record<string, bigint>>;

/** A budget as the core takes it, its limits sorted by meter; `InvalidArgument` for a malformed
 * meter or limit. */
export function budgetProto(budget: unknown): layout.Budget {
  if (!isObject(budget) || ![Object.prototype, null].includes(Object.getPrototypeOf(budget))) {
    throw new InvalidArgument(`a budget is an object of meters to limits, not ${String(budget)}`);
  }
  const limits = Object.entries(budget).map(([meter, limit]) => {
    if (!(REQUESTS as readonly string[]).includes(meter) && !isCurrency(meter)) {
      throw new InvalidArgument(
        `the budget meter "${meter}" is none of create, cas, get, list, delete, requests or a ` +
          "currency of three uppercase letters",
      );
    }
    if (!isAmount(limit)) {
      throw new InvalidArgument(
        `the budget limit of ${meter} is a bigint from 0n to ${MAX}n, not ${shown(limit)}`,
      );
    }
    return { meter, limit };
  });
  limits.sort((a, b) => (a.meter < b.meter ? -1 : a.meter > b.meter ? 1 : 0));
  return create(layout.BudgetSchema, { limits });
}

/** The budget of a start: its own, else its workflow's, else none. */
export function effectiveBudget(
  budget: Budget | undefined,
  declared: Budget | undefined,
): layout.Budget | undefined {
  // § evdz71nzb.a@rvtf3
  if (budget !== undefined) return budgetProto(budget);
  if (declared !== undefined) return budgetProto(declared);
  return undefined;
}
