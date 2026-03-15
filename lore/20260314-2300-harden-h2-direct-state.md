# Harden: compio-h2 direct state architecture

Date: 2026-03-14

## Changes

### 1. Remove dead code & fix all compiler warnings
- Removed unused `ConnConfig` struct from state.rs
- Removed unused `deliver_data`, `deliver_trailers`, `deliver_response_headers` methods from state.rs (frame handlers use direct field access instead)
- Removed unused imports: `DecodedHeader` (server.rs), `FrameWriter` (codec/mod.rs), `ConnConfig`/`ConnExtra` (connection.rs), `StreamState` (state.rs)
- Fixed unused vars: `e` → `_err` (server.rs), removed `settings_local` (connection.rs), removed `slot_for_pending` (share.rs)

### 2. Deduplicate DATA frame encoding
- `encode_data_frames()` in connection.rs now delegates to `ConnShared::encode_data()` instead of duplicating the flow control + frame writing logic (was ~50 lines of near-identical code)

### 3. Remove dead `result` field on PendingSend
- `PendingSend.result` was write-only: set by `flush_pending_sends` but the completed item was removed from the queue before the poll_fn could read it
- Removed `result` field, removed `result_slot`/`slot_for_pending` variables from share.rs
- Simplified the pending send poll_fn to just check item presence (absence = sent)

### 4. Enforce max_send_buffer_size
- Added check in `SendStream::send_data()` to reject when `pending_send_bytes + data.len() > max_send_buffer_size`
- Previously the field was set but never checked — config was silently ignored
- Added integration test `max_send_buffer_size_enforced`

### 5. Delete codec/writer.rs
- Entire `FrameWriter` struct was dead code — writing now done directly from `ConnShared.write_buf`
- Moved the 3 roundtrip tests to codec/reader.rs (rewritten to use `Frame::encode()` directly)
- Deleted 185 lines of unused code

## Verification
- 0 compio-h2 compiler warnings
- 311/311 tests pass (200 unit + 110 integration + 1 doc test)
