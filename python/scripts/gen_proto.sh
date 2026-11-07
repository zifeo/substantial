#!/bin/sh
# Regenerates the protobuf modules of python/substantial from proto/.
#
#   python/scripts/gen_proto.sh
#
# The generated code checks the protobuf runtime against the protoc that wrote it, so protoc must
# be of the release the `protobuf` dependency of pyproject.toml names (protoc 36.x for 7.36.x).
set -eu

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"

expected="libprotoc 36."
case $(protoc --version) in
  "$expected"*) ;;
  *) echo "protoc $(protoc --version) is not $expected*" >&2; exit 1 ;;
esac

protoc --proto_path=proto --python_out=python --pyi_out=python \
  substantial/layout/v1/layout.proto \
  substantial/boundary/v1/boundary.proto \
  substantial/boundary/v1/run.proto \
  substantial/boundary/v1/agent.proto \
  substantial/boundary/v1/plan.proto
