#!/usr/bin/env bash
# Run compio-driver tests with AddressSanitizer (ASan) + LeakSanitizer (LSan) to
# detect memory leaks, use-after-free, buffer overflows, and stack overflows.
#
# This exercises the full io_uring / poll driver path including:
#   - Key allocation (ThinCell<RawOp>) via into_raw/from_raw
#   - In-flight operation lifecycle (push → completion → free)
#   - Driver drop cleanup of uncompleted operations
#
# Requires: nightly toolchain with rust-src component
#   rustup component add rust-src --toolchain nightly
#
# Usage:
#   ./compio-driver/scripts/memleak/asan.sh <target>                # run all tests
#   ./compio-driver/scripts/memleak/asan.sh <target> -- timeout     # filter by test name

set -euo pipefail

TARGET="${1:?Usage: $0 <target-triple> [-- test-filter]}"
shift

# ASan + LSan: detect memory errors and leaks at process exit.
export RUSTFLAGS="${RUSTFLAGS:-} -Zsanitizer=address"
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=0}"

EXTRA_ARGS=(-p compio-driver "$@")

echo "=== ASan + LSan: detecting memory errors and leaks (compio-driver) ==="
echo "Target: $TARGET"
echo "RUSTFLAGS=$RUSTFLAGS"
echo "ASAN_OPTIONS=$ASAN_OPTIONS"
echo ""

# -Zbuild-std instruments std with ASan (prevents false positives and
# compilation errors in deps like zerocopy that depend on std internals).
cargo +nightly test -Zbuild-std --target "$TARGET" "${EXTRA_ARGS[@]}"
