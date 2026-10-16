#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TAUT_SOURCE:?Set TAUT_SOURCE to a clean taut checkout at a7720b961e0240ecf28e6fd699270dc926c85e57}"
test "$(git -C "$TAUT_SOURCE" rev-parse HEAD)" = a7720b961e0240ecf28e6fd699270dc926c85e57
test -z "$(git -C "$TAUT_SOURCE" status --porcelain -- src/crc32c.cc include)"
mkdir -p docs/engine
em++ -std=c++20 -O2 -fexceptions wasm/public.cc src/store.cc src/records.cc src/wal.cc src/wire.cc \
  "$TAUT_SOURCE/src/crc32c.cc" -Isrc -I"$TAUT_SOURCE/include" \
  -sMODULARIZE=1 -sEXPORT_ES6=1 -sEXPORT_NAME=createStore -sALLOW_MEMORY_GROWTH=1 -sSTACK_SIZE=1048576 \
  -sENVIRONMENT=web,worker -sEXPORTED_FUNCTIONS='["_run_store"]' \
  -sEXPORTED_RUNTIME_METHODS='["ccall"]' -o docs/engine/store.js
