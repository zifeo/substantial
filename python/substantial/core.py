"""The core: the module, its instances, and the boundary ABI (I-23, I-25)."""

from __future__ import annotations

import functools
import hashlib
import importlib.metadata
from enum import IntEnum

import wasmtime

from substantial.errors import ModuleError, ProtocolError, Trap

WASM_NAME = "substantial.wasm"
_SHIPPED = ("substantial/wasm/substantial.wasm", "substantial/wasm/SHA256SUMS")
_U32 = 0xFFFF_FFFF
_U64 = 0xFFFF_FFFF_FFFF_FFFF


class Kind(IntEnum):
    RUN = 1
    AGENT = 2
    PLAN = 3


def shipped_files() -> tuple[bytes, str]:
    """The module and its checksums, as the installed distribution ships them."""
    try:
        dist = importlib.metadata.distribution("substantial")
    except importlib.metadata.PackageNotFoundError as e:
        raise ModuleError(
            "the substantial distribution, which ships the core, is not installed"
        ) from e
    wasm_path, sums_path = (dist.locate_file(path) for path in _SHIPPED)
    try:
        return wasm_path.read_bytes(), sums_path.read_text()
    except OSError as e:
        raise ModuleError(f"the core is missing from the installed distribution: {e}") from e


def verify(wasm: bytes, sums: str) -> None:
    """Checks that the sha256 of `wasm` is the one the `substantial.wasm` line of `sums` names."""
    expected = None
    for line in sums.splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and parts[1].lstrip(" *") == WASM_NAME:
            expected = parts[0].lower()
            break
    if expected is None:
        raise ModuleError(f"no {WASM_NAME} line in SHA256SUMS")
    actual = hashlib.sha256(wasm).hexdigest()
    if actual != expected:
        raise ModuleError(f"{WASM_NAME} sha256 is {actual}, SHA256SUMS expects {expected}")


class CoreModule:
    """The compiled core module, shared by every instance of the process."""

    def __init__(self, wasm: bytes, sums: str) -> None:
        verify(wasm, sums)
        self._engine = wasmtime.Engine()
        try:
            self._module = wasmtime.Module(self._engine, wasm)
        except wasmtime.WasmtimeError as e:
            raise ModuleError(str(e)) from e
        if self._module.imports:
            raise ModuleError("the core must import nothing")

    @staticmethod
    @functools.cache
    def shipped() -> CoreModule:
        return CoreModule(*shipped_files())

    def instantiate(self) -> Core:
        return Core(self._engine, self._module)


def _i32(value: int) -> int:
    """A u32 as the signed i32 wasmtime takes."""
    return value - (1 << 32) if value >= 1 << 31 else value


class Core:
    """One instance of the core and the machines in it.

    Every boundary call is short and synchronous, made from the event loop's thread only.
    """

    def __init__(self, engine: wasmtime.Engine, module: wasmtime.Module) -> None:
        self._store = wasmtime.Store(engine)
        self._trapped: str | None = None
        try:
            instance = wasmtime.Instance(self._store, module, [])
        except (wasmtime.WasmtimeError, wasmtime.Trap) as e:
            raise ModuleError(str(e)) from e
        exports = instance.exports(self._store)

        def export(name: str, kind: type) -> object:
            item = exports.get(name)
            if not isinstance(item, kind):
                raise ModuleError(f"missing export {name}")
            return item

        self._memory = export("memory", wasmtime.Memory)
        self._alloc = export("sub_alloc", wasmtime.Func)
        self._free = export("sub_free", wasmtime.Func)
        self._new = export("sub_new", wasmtime.Func)
        self._call = export("sub_call", wasmtime.Func)
        self._drop = export("sub_drop", wasmtime.Func)

    @staticmethod
    def new() -> Core:
        """An instance of the shipped module."""
        return CoreModule.shipped().instantiate()

    def _guard(self, func: object, *args: int) -> int:
        if self._trapped is not None:
            raise Trap(self._trapped)
        try:
            return func(self._store, *args)  # type: ignore[operator]
        except (wasmtime.Trap, wasmtime.WasmtimeError) as e:
            self._trapped = str(e)
            raise Trap(self._trapped) from e

    def _write(self, data: bytes) -> tuple[int, int]:
        """Copies a request into the module, which frees it when it takes it."""
        ptr = self._guard(self._alloc, _i32(len(data))) & _U32
        try:
            self._memory.write(self._store, data, ptr)  # type: ignore[attr-defined]
        except (IndexError, ValueError, wasmtime.WasmtimeError) as e:
            self._trapped = f"request out of bounds: {e}"
            raise Trap(self._trapped) from e
        return ptr, len(data)

    def _take(self, ptr: int, length: int) -> bytes:
        """Copies a response out of the module and frees it."""
        try:
            out = bytes(self._memory.read(self._store, ptr, ptr + length))  # type: ignore[attr-defined]
        except (IndexError, ValueError, wasmtime.WasmtimeError) as e:
            self._trapped = f"response out of bounds: {e}"
            raise Trap(self._trapped) from e
        if len(out) != length:
            self._trapped = "response out of bounds"
            raise Trap(self._trapped)
        self._guard(self._free, _i32(ptr), _i32(length))
        return out

    def new_machine(self, kind: int, data: bytes) -> int:
        if len(data) > _U32:
            raise ProtocolError("request too large")
        ptr, length = self._write(data)
        return self._guard(self._new, _i32(kind), _i32(ptr), _i32(length)) & _U32

    def call(self, machine: int, request: bytes) -> bytes:
        if len(request) > _U32:
            raise ProtocolError("request too large")
        ptr, length = self._write(request)
        packed = self._guard(self._call, _i32(machine), _i32(ptr), _i32(length)) & _U64
        return self._take(packed >> 32, packed & _U32)

    def drop_machine(self, machine: int) -> None:
        if self._trapped is None:
            self._guard(self._drop, _i32(machine))
