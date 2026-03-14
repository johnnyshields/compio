#!/usr/bin/env bash
# Run unit tests under Miri to detect memory leaks, undefined behavior,
# and aliasing violations in pure logic (HPACK, frames, codec).
#
# Miri cannot handle real I/O (io_uring, IOCP), so tests that create a
# compio-runtime are excluded. The safe modules are:
#   - hpack::    (Huffman, dynamic table, encoder/decoder)
#   - frame::    (frame encode/decode)
#   - error::    (error types)
#
# Some proto:: and codec:: tests also touch the runtime and are skipped.
#
# Usage:
#   ./scripts/memleak/miri.sh              # run all compatible tests
#   ./scripts/memleak/miri.sh hpack        # filter by test name

set -euo pipefail

# Miri flags:
#   -Zmiri-tree-borrows   use Tree Borrows model (more permissive than
#                         Stacked Borrows, fewer false positives)
export MIRIFLAGS="${MIRIFLAGS:--Zmiri-tree-borrows}"

echo "=== Miri: detecting UB, leaks, and aliasing violations ==="
echo "MIRIFLAGS=$MIRIFLAGS"
echo ""

# If user passes arguments, use them as test filter directly
if [[ $# -gt 0 ]]; then
    echo "--- compio-h2 (filter: $*) ---"
    cargo +nightly miri test -p compio-h2 --lib -- "$@"
    exit $?
fi

# Run each Miri-safe module separately.
# Modules that touch compio-runtime (codec::reader, some proto::connection
# tests) are excluded — they hit io_uring syscalls Miri can't emulate.
MODULES=("hpack::" "frame::" "error::")

total=0
for mod in "${MODULES[@]}"; do
    echo "--- compio-h2 $mod ---"
    output=$(cargo +nightly miri test -p compio-h2 --lib -- "$mod" 2>&1)
    echo "$output" | tail -3
    passed=$(echo "$output" | grep "test result:" | grep -oP '\d+ passed' | grep -oP '\d+' || echo 0)
    total=$((total + passed))
    echo ""
done

echo "=== Miri complete: $total tests passed ==="
