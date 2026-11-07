// Opening an S3 or a Redis store without `@aws-sdk/client-s3` or `@redis/client`, optional peer
// dependencies; its own file, so the process that runs it never loads the packages.
import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { test } from "node:test";
import { ConfigError, Credentials } from "../src/mod.ts";
import { openStore } from "../src/stores/open.ts";

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "@aws-sdk/client-s3" || specifier === "@redis/client") {
      throw Object.assign(new Error(`Cannot find package '${specifier}'`), {
        code: "ERR_MODULE_NOT_FOUND",
      });
    }
    return nextResolve(specifier, context);
  },
});

test("an S3 store without the SDK names the package to install", async () => {
  const credentials = new Credentials({ accessKeyId: "id", secretAccessKey: "secret" });
  await assert.rejects(
    openStore("h3+http://127.0.0.1:1/b/ns?region=r", credentials),
    (e) => e instanceof ConfigError && e.message.includes("npm install @aws-sdk/client-s3"),
  );
  await openStore("memory://no-sdk", undefined);
});

test("redis urls without the extra name it", async () => {
  await assert.rejects(
    openStore("redis://127.0.0.1:1/ns?db=0&replicas=0", undefined),
    (e) => e instanceof ConfigError && e.message.includes("npm install @redis/client"),
  );
});
