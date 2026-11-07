// The core: the module, its instances, and the boundary ABI (I-23, I-25).
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { ModuleError, ProtocolError, Trap } from "./errors.ts";

export const WASM_NAME = "substantial.wasm";
// The repository keeps the only copy in wasm/, and the npm tarball and JSR ship it at that path:
// typescript/src and typescript/dist sit at the same depth.
const WASM_URL = new URL("../../wasm/substantial.wasm", import.meta.url);
const SUMS_URL = new URL("../../wasm/SHA256SUMS", import.meta.url);
const U32 = 0xffff_ffff;

export const Kind = { RUN: 1, AGENT: 2, PLAN: 3 } as const;
export type Kind = (typeof Kind)[keyof typeof Kind];

export async function read(url: URL): Promise<Uint8Array> {
  try {
    if (url.protocol === "file:") {
      // A path, not the URL: under Deno the global URL is not the one node:fs declares.
      return new Uint8Array(await readFile(fileURLToPath(url.href)));
    }
    // A package served over HTTPS, as JSR modules are to Deno.
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return new Uint8Array(await response.arrayBuffer());
  } catch (e) {
    throw new ModuleError(`cannot read ${url}: ${e instanceof Error ? e.message : e}`);
  }
}

/** The module and its checksums, as the package ships them. */
export async function shippedFiles(): Promise<{ wasm: Uint8Array; sums: string }> {
  const [wasm, sums] = await Promise.all([read(WASM_URL), read(SUMS_URL)]);
  return { wasm, sums: new TextDecoder().decode(sums) };
}

function hex(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** Checks that the sha256 of `wasm` is the one the `substantial.wasm` line of `sums` names. */
export async function verify(wasm: Uint8Array, sums: string): Promise<void> {
  let expected: string | undefined;
  for (const line of sums.split("\n")) {
    const match = /^(\S+)\s+\**(.+?)\s*$/.exec(line);
    if (match?.[2] === WASM_NAME) {
      expected = match[1]?.toLowerCase();
      break;
    }
  }
  if (expected === undefined) {
    throw new ModuleError(`no ${WASM_NAME} line in SHA256SUMS`);
  }
  const actual = hex(await crypto.subtle.digest("SHA-256", wasm as Uint8Array<ArrayBuffer>));
  if (actual !== expected) {
    throw new ModuleError(`${WASM_NAME} sha256 is ${actual}, SHA256SUMS expects ${expected}`);
  }
}

let shipped: Promise<CoreModule> | undefined;

/** The compiled core module, shared by every instance of the process. */
export class CoreModule {
  readonly #module: WebAssembly.Module;

  private constructor(module: WebAssembly.Module) {
    this.#module = module;
  }

  static async fromBytes(wasm: Uint8Array, sums: string): Promise<CoreModule> {
    await verify(wasm, sums);
    let module: WebAssembly.Module;
    try {
      module = await WebAssembly.compile(wasm as Uint8Array<ArrayBuffer>);
    } catch (e) {
      throw new ModuleError(`cannot compile ${WASM_NAME}: ${e instanceof Error ? e.message : e}`);
    }
    if (WebAssembly.Module.imports(module).length > 0) {
      throw new ModuleError("the core must import nothing");
    }
    return new CoreModule(module);
  }

  /** The shipped module, verified and compiled once; a failure is tried again next time. */
  static shipped(): Promise<CoreModule> {
    if (shipped === undefined) {
      const loading = shippedFiles().then(({ wasm, sums }) => CoreModule.fromBytes(wasm, sums));
      shipped = loading;
      loading.catch(() => {
        if (shipped === loading) shipped = undefined;
      });
    }
    return shipped;
  }

  instantiate(): Core {
    let instance: WebAssembly.Instance;
    try {
      instance = new WebAssembly.Instance(this.#module, {});
    } catch (e) {
      throw new ModuleError(
        `cannot instantiate ${WASM_NAME}: ${e instanceof Error ? e.message : e}`,
      );
    }
    return new Core(instance);
  }
}

type Fn = (...args: number[]) => unknown;

/** One instance of the core and the machines in it; every boundary call is synchronous. */
export class Core {
  readonly #memory: WebAssembly.Memory;
  readonly #alloc: Fn;
  readonly #free: Fn;
  readonly #new: Fn;
  readonly #call: Fn;
  readonly #drop: Fn;
  #trapped: string | undefined;

  constructor(instance: WebAssembly.Instance) {
    const exports = instance.exports;
    const fn = (name: string): Fn => {
      const f = exports[name];
      if (typeof f !== "function") {
        throw new ModuleError(`missing export ${name}`);
      }
      return f as Fn;
    };
    const memory = exports.memory;
    if (!(memory instanceof WebAssembly.Memory)) {
      throw new ModuleError("missing export memory");
    }
    this.#memory = memory;
    this.#alloc = fn("sub_alloc");
    this.#free = fn("sub_free");
    this.#new = fn("sub_new");
    this.#call = fn("sub_call");
    this.#drop = fn("sub_drop");
  }

  #trap(message: string): Trap {
    this.#trapped = message;
    return new Trap(message);
  }

  #guard(f: Fn, ...args: number[]): unknown {
    if (this.#trapped !== undefined) {
      throw new Trap(this.#trapped);
    }
    try {
      return f(...args);
    } catch (e) {
      throw this.#trap(e instanceof Error ? e.message : String(e));
    }
  }

  /** Copies a request into the module, which frees it when it takes it. */
  #write(data: Uint8Array): number {
    if (data.length > U32) {
      throw new ProtocolError("request too large");
    }
    const ptr = (this.#guard(this.#alloc, data.length) as number) >>> 0;
    const memory = new Uint8Array(this.#memory.buffer);
    if (ptr + data.length > memory.length) {
      throw this.#trap("request out of bounds");
    }
    memory.set(data, ptr);
    return ptr;
  }

  newMachine(kind: number, data: Uint8Array): number {
    const ptr = this.#write(data);
    return (this.#guard(this.#new, kind, ptr, data.length) as number) >>> 0;
  }

  call(machine: number, request: Uint8Array): Uint8Array {
    const ptr = this.#write(request);
    const packed = BigInt.asUintN(
      64,
      this.#guard(this.#call, machine, ptr, request.length) as bigint,
    );
    const out = Number(packed >> 32n);
    const length = Number(packed & 0xffff_ffffn);
    const memory = new Uint8Array(this.#memory.buffer);
    if (out + length > memory.length) {
      throw this.#trap("response out of bounds");
    }
    const response = memory.slice(out, out + length);
    this.#guard(this.#free, out, length);
    return response;
  }

  dropMachine(machine: number): void {
    if (this.#trapped === undefined) {
      this.#guard(this.#drop, machine);
    }
  }
}
