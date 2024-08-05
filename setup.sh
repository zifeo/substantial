# linux
sudo apt install python3-dev
# windows
# => will require the DLL, work by default as long as it's in the PATH

# protobuf + rust codegen plugin for the system
sudo apt install protobuf-compiler
cargo install protobuf-codegen
# lib for the generated code
cargo install protobuf

protoc -I . --rust_out=substantial/src/protocol protocol/*
