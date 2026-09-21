#!/usr/bin/env bash

set -euo pipefail

out=$1
manifest_path=$2

cargo llvm-cov nextest \
  --manifest-path "$manifest_path" \
  --workspace \
  --no-report \
  --remap-path-prefix

mkdir -p "$out"

cargo llvm-cov report \
  --manifest-path "$manifest_path" \
  --lcov \
  --remap-path-prefix \
  --output-path "$out/coverage.lcov"

test -s "$out/coverage.lcov"
