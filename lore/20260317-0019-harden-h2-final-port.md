# Harden: compio-h2-final port

Hardening pass on the 9 commits ported from `feat-compio-h2` onto `compio-h2` (direct state architecture).

## Effort/Impact Table

| # | Opportunity | Effort | Impact | Action |
|---|-------------|--------|--------|--------|
| 1 | Extract `WINDOW_UPDATE_THRESHOLD_RATIO` constant | Quick | Medium | Implemented |
| 2 | Fix clippy nested-if warnings (5 instances) | Quick | Low | Implemented |
| 3 | Fix clippy else-if collapse (2 instances) | Quick | Low | Implemented |

## Items assessed as fine (no action needed)

- TLS module: well-implemented, ALPN validation correct
- flow_controlled_len: properly used in all call sites, tests cover padded/non-padded
- Zerocopy threshold: consistent in write() and write_vectored(), well-documented
- Cancel-safety docs: accurate for direct-state architecture
- Test coverage: 31 tests passing, good coverage for ported features
- No dead code or unused imports introduced by the port
- h2spec: 147/147 passing

## Changes made

### 1. WINDOW_UPDATE_THRESHOLD_RATIO constant
- Added `pub(crate) const WINDOW_UPDATE_THRESHOLD_RATIO: i32 = 2` in `proto/streams.rs`
- Used in `streams_needing_window_update()`, `apply_release()`, and `encode_window_updates()`
- Removed the `threshold_ratio` parameter from `streams_needing_window_update()` — it was always called with `2` and having it as a parameter while `apply_release()` hardcoded `/2` was a desync risk

### 2-3. Clippy fixes
- Collapsed 5 nested `if let ... { if ... }` into combined conditions in server.rs, state.rs, connection.rs
- Collapsed 2 `else { if ... }` into `else if` in connection.rs
