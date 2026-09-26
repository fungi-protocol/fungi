#!/usr/bin/env bash

set -euo pipefail

out=$1
manifest_path=$2

cargo llvm-cov nextest \
  --manifest-path "$manifest_path" \
  --workspace \
  --all-features \
  --no-report \
  --remap-path-prefix

mkdir -p "$out"

cargo llvm-cov report \
  --manifest-path "$manifest_path" \
  --lcov \
  --ignore-filename-regex '(^|/)target/.*/out/channel_capnp\.rs$' \
  --remap-path-prefix \
  --output-path "$out/coverage.lcov"

test -s "$out/coverage.lcov"
