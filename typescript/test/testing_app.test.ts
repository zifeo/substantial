// The test environment of docs/testing.md: a web application given `env.client()`, whose start
// opens its own worker and whose stop closes its client.
import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import * as s from "../src/mod.ts";
import { T0 } from "./testing_helpers.ts";

const charged: string[] = [];

beforeEach(() => {
  charged.length = 0;
});

const fulfil = s.workflow("fulfil", async (c, { order }: { order: string }) => {
  return await c.step("capture", async () => {
    charged.push(order);
    return `cap-${order}`;
  });
});

interface App {
  start(): Promise<void>;
  stop(): Promise<void>;
  fetch(request: Request): Promise<Response>;
}

/** The shop: POST /orders starts a run, `?wait=1` waits 5 s for its result; GET /orders/<id>
 * reads its status. */
function createApp(openEngine: () => Promise<s.Substantial>): App {
  let stack: AsyncDisposableStack | undefined;
  let sub: s.Substantial | undefined;
  return {
    async start() {
      stack = new AsyncDisposableStack();
      sub = stack.use(await openEngine());
      stack.use(sub.worker([fulfil], { preset: "service" }));
    },
    async stop() {
      await stack?.disposeAsync();
    },
    async fetch(request) {
      if (sub === undefined) throw new Error("the app is not started");
      const url = new URL(request.url);
      if (request.method === "POST") {
        const body = (await request.json()) as { order: string };
        const run = await sub.start(fulfil, body, { id: body.order });
        if (url.search !== "?wait=1") return Response.json(null, { status: 202 });
        try {
          return Response.json(await run.result({ timeout: s.seconds(5) }));
        } catch (e) {
          if (e instanceof s.WaitTimeout) return Response.json(null, { status: 202 });
          throw e;
        }
      }
      const order = url.pathname.split("/").at(-1) as string;
      return Response.json({ status: await sub.run(fulfil, order).status() });
    },
  };
}

async function request(app: App, method: string, path: string, body?: unknown) {
  const init = body === undefined ? { method } : { method, body: JSON.stringify(body) };
  const response = await app.fetch(new Request(`http://shop.test${path}`, init));
  return [response.status, await response.json()];
}

/** The app's start and stop, as a server runs them around its requests. */
async function running(app: App): Promise<AsyncDisposable> {
  await app.start();
  return { [Symbol.asyncDispose]: () => app.stop() };
}

test("an asgi app given a client", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  const app = createApp(() => env.client());
  {
    await using _running = await running(app);
    for (let i = 0; i < 2; i++) {
      assert.deepEqual(await request(app, "POST", "/orders", { order: "o-1" }), [202, null]);
    }
    assert.deepEqual(await request(app, "GET", "/orders/o-1"), [200, { status: "pending" }]);
    await env.settle();
    assert.deepEqual(await request(app, "GET", "/orders/o-1"), [200, { status: "completed" }]);
    assert.equal(await env.run(fulfil, "o-1").result(), "cap-o-1");
  }
  assert.deepEqual(charged, ["o-1"]);
  await env.check();
});

test("an endpoint that waits answers at once when the step is slow", async () => {
  await using env = await s.testing.Env.open({ now: T0 });
  const app = createApp(() => env.client());
  env.hold("capture");
  {
    await using _running = await running(app);
    assert.deepEqual(await request(app, "POST", "/orders?wait=1", { order: "o-1" }), [202, null]);
    assert.deepEqual(env.now, T0);
  }
  await env.check();
});
