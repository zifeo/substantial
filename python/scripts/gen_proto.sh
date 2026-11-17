#!/bin/sh
# Regenerates the protobuf modules of python/substantial from proto/.
#
#   mise exec protoc@21.1 -- python/scripts/gen_proto.sh
#
# Generated code runs on a protobuf runtime at least as new as the protoc that wrote it, so the
# oldest protoc that can write it sets the floor of the `protobuf` dependency in pyproject.toml:
# protoc 21.1 is the oldest whose `--pyi_out` supports proto3 `optional`, and its code runs on
# protobuf 4.21.1 and every later runtime (4.21.0 is yanked). A newer protoc would raise that
# floor, and with it the protobuf versions an application can pin.
set -eu

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

expected="libprotoc 3.21.1"
if [ "$(protoc --version)" != "$expected" ]; then
  echo "protoc $(protoc --version) is not $expected: run it with mise exec protoc@21.1" >&2
  exit 1
fi

protoc --proto_path=proto --python_out=python --pyi_out=python \
  substantial/layout/v1/layout.proto \
  substantial/boundary/v1/boundary.proto \
  substantial/boundary/v1/run.proto \
  substantial/boundary/v1/agent.proto \
  substantial/boundary/v1/plan.proto
