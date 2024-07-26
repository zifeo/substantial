set -eux

BTYPE=debug
TARGET=wasm32-unknown-unknown
RAW_OUTPUT=./substantial/common/target/$TARGET/$BTYPE/common.wasm
OUTPUT=./substantial/common/target/$TARGET/$BTYPE/common-component.wasm

# make sure the rust plugin is added using setup.sh
protoc -I . --rust_out=./substantial/common/src/protocol protocol/*

cd substantial/common
cargo build --target $TARGET
cd ../..

wasm-tools component new $RAW_OUTPUT -o $OUTPUT

jco transpile $OUTPUT -o ./substantial/deno/gen
python -m wasmtime.bindgen $OUTPUT --out-dir ./substantial/python/gen
