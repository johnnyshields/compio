# Harden: compio-h2 TLS, flow control, cancel safety

Review of commit `5e36463` (feat-compio-h2 branch).

## Effort/Impact Table

| # | Opportunity | Effort | Impact | Action |
|---|-------------|--------|--------|--------|
| 1 | Fix umbrella crate feature forwarding for native-tls/rustls | Quick | High | Auto-fix |
| 2 | Fix TLS example `required-features` to gate on `native-tls` not `tls` | Quick | Medium | Auto-fix |
| 3 | Fix server TLS example doc header (says PEM but code uses PKCS#12) | Quick | Low | Auto-fix |
| 4 | Add non-padded `flow_controlled_len` test case | Quick | Low | Auto-fix |

## Opportunity Details

### 1. Fix umbrella crate feature forwarding

**What**: `compio/Cargo.toml` lines 86 and 91 forward `compio-h2?/tls` for both `native-tls` and `rustls` features. Since compio-h2 now has its own `native-tls` and `rustls` sub-features, the umbrella should forward those instead.

**Where**: `compio/Cargo.toml`

**Why**: Without this, `cargo build -p compio --features native-tls,h2` enables compio-h2's `tls` feature (pulling in `compio-tls` dep) but doesn't activate any TLS backend in compio-h2. The `compio_h2::tls::native_tls` re-export won't be available.

**Change**:
- Line 86: `compio-h2?/tls` → `compio-h2?/native-tls`
- Line 91: `compio-h2?/tls` → `compio-h2?/rustls`

### 2. Fix TLS example required-features

**What**: Both TLS examples use `compio_h2::tls::native_tls::*` which is gated on `#[cfg(feature = "native-tls")]`. But Cargo.toml gates examples on `required-features = ["tls"]`. Building with `--features rustls` (without native-tls) would fail.

**Where**: `compio-h2/Cargo.toml` example entries for `h2-client-tls` and `h2-server-tls`

**Change**: `required-features = ["tls"]` → `required-features = ["native-tls"]`

### 3. Fix server TLS example doc header

**What**: The doc comment says `H2_CERT_PATH` / `H2_KEY_PATH` (PEM) but the code uses `H2_IDENTITY_PATH` / `H2_IDENTITY_PASS` (PKCS#12).

**Where**: `compio-h2/examples/h2-server-tls.rs` lines 8-9

### 4. Add non-padded flow_controlled_len test

**What**: Add a test verifying `flow_controlled_len()` equals `payload().len()` for non-padded DATA frames. Currently only padded frames are tested.

**Where**: `compio-h2/src/frame/data.rs` test module

## Execution Protocol
**DO NOT implement any changes without user approval.**
For EACH opportunity, use `AskUserQuestion`.
Options: "Implement" / "Skip (add to TODO.md)" / "Do not implement"
Ask all questions before beginning any implementation work
(do NOT do alternating ask then implement, ask then implement, etc.)
After all items resolved, run: `cargo check -p compio-h2` and `cargo test -p compio-h2`
