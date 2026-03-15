---
date: 2026-03-15
scope: compio-h2
type: hardening
---

# Harden: compio-h2 partial send + benchmark changes

Hardening pass after partial send support in `encode_data` and RPC benchmark window reconfiguration.

## Findings

Codebase is clean: clippy clean, 356 tests passing, comprehensive flow control coverage.

| # | Opportunity | Effort | Impact | Action |
|---|-------------|--------|--------|--------|
| 1 | Handle ignored error in `flush_pending_sends` full-send path | Quick | Low | Implemented |

### #1: Handle ignored error in flush_pending_sends

`connection.rs:305` had `let _result = encode_data_frames(...)` silently ignoring errors, while the partial-send path at line 314 properly checks errors. Fixed to check the result and wake the sender on error, consistent with the partial-send error handling.
