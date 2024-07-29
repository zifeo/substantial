set -eux

BTYPE=debug
TARGET=wasm32-unknown-unknown
RAW_OUTPUT=target/$TARGET/$BTYPE/common.wasm
OUTPUT=target/$TARGET/$BTYPE/substantial.wasm

# make sure the rust plugin is added using setup.sh
protoc -I . --rust_out=substantial/common/src/protocol protocol/*

cargo build --target $TARGET

wasm-tools component new $RAW_OUTPUT -o $OUTPUT

rm -Rf ./substantial/deno/gen
mkdir ./substantial/deno/gen
jco transpile $OUTPUT -o ./substantial/deno/gen --map metatype:substantial/host=../host/host.mjs
deno run -A dev/fix-declarations.ts

python -m wasmtime.bindgen $OUTPUT --out-dir ./substantial/python/gen
