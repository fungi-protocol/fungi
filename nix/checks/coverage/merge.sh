#!/usr/bin/env bash

# Usage: merge.sh OUT [WORKSPACE_DIR=REPORT]...
#
# Combines the coverage reports of every workspace into one lcov tracefile
# and one Cobertura report with paths relative to the repository root.

set -euo pipefail

out=$1
shift

mkdir -p "$out"

if (($# == 0)); then
  echo "no workspace registered a coverage check; nothing to merge"
  exit 0
fi

tracefiles=()
for entry in "$@"; do
  workspace_dir=${entry%%=*}
  report=${entry#*=}
  tracefile=$(mktemp)
  # cargo-llvm-cov writes paths relative to the Cargo workspace.
  if [[ $workspace_dir == . ]]; then
    cp "$report/coverage.lcov" "$tracefile"
  else
    sed "s|^SF:|SF:$workspace_dir/|" "$report/coverage.lcov" >"$tracefile"
  fi
  tracefiles+=(--add-tracefile "$tracefile")
done

lcov "${tracefiles[@]}" --output-file "$out/coverage.lcov"
lcov_cobertura "$out/coverage.lcov" --base-dir . --output "$out/cobertura.xml"
# lcov_cobertura stamps the current time; keep the report reproducible.
sed -i 's/ timestamp="[0-9]*"/ timestamp="0"/' "$out/cobertura.xml"
