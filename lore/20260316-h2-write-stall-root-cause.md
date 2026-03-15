# H2 Write Stall: Complete Root Cause Analysis

## Summary

64KB H2 echo roundtrip: **median 74µs, but periodic 44ms spikes** drag mean to ~11ms (vs h2 crate's 103µs mean). Root cause is compio-runtime single-threaded task scheduling, not io_uring or TCP.

## Evidence chain

### 1. The stall is periodic, not on every request

```
65536B: mean=10592µs  median=74µs  min=54µs  max=44363µs
```

Median is close to h2 crate. The 44ms spike occurs every ~8 requests (when WINDOW_UPDATE threshold is crossed: 8 × 64KB = 512KB = 50% of 1MB window).

### 2. The TCP write itself is fast

`probe_uring` benchmark (compio-h2/examples/probe_uring.rs):

| Path | 13B after 64KB |
|------|---------------|
| io_uring SEND_ZC | median 3µs, max 6µs |
| Blocking write(2) | median 0µs, max 53µs |
| SEND_ZC CQE2 (buf release) | 0µs (arrives with CQE1) |

No stall at the syscall level. The zerocopy 2-CQE path adds negligible overhead.

### 3. The stall is in runtime task scheduling

`uring_submit_bug` Test 6 ("reader wakes writer") reproduces the exact pattern:

1. Writer sends 64KB → echo comes back
2. Reader receives echo → adds 13B to write_buf → calls `wake()`
3. Writer runs `write_all(13B)` — **this takes 2-10ms**

But `write_all(13B)` at the socket level takes 3µs. The delay is between `wake()` and when the writer task actually gets scheduled by the runtime.

### 4. Why the scheduling delay exists

On compio's single-threaded runtime:

1. Reader task processes DATA frame, calls `wake_io()`
2. Reader immediately loops to `read_frame()` → `read_exact(9)` → submits RECV to io_uring
3. If data is in the socket buffer, `read_exact` completes instantly → reader keeps running
4. Reader processes more frames without yielding
5. Only when reader's next `read_exact` finds no data → returns `Pending` → runtime regains control
6. Runtime calls `submit_and_wait(1)` to submit queued ops and wait for a CQE
7. io_flush_loop finally gets scheduled, encodes WINDOW_UPDATE, calls `write_all(13B)`
8. `write_all` submits SEND_ZC but now must wait for another `submit_and_wait()` round

The delay accumulates across steps 2-6. When the server sends multiple frames back-to-back (4 DATA frames for 64KB), the reader processes all of them in rapid succession. The woken io_flush_loop is starved until the reader finally blocks on an empty socket.

## Fixes applied

### In compio-h2 (this branch)

1. **Stream WINDOW_UPDATE threshold** (streams.rs): Only send when released ≥ 50% of initial window. Reduced stall frequency from every frame to every ~8 requests.

2. **Lazy IO wake on release_capacity** (share.rs): Only wake the io_flush_loop when the threshold is actually crossed, not on every `release_capacity()` call. Reduces unnecessary IO loop iterations.

### What would fix it fully

**compio-runtime level** (not in this branch):

- Before calling `submit_and_wait()`, poll all woken tasks first. This would let the io_flush_loop run and queue its SEND_ZC before the runtime blocks waiting for CQEs.
- Or: submit all queued SQEs eagerly in `push_raw()` instead of deferring to the event loop poll. This would let the WINDOW_UPDATE write go out immediately when queued, without waiting for the next event loop iteration.

**H2 protocol level** (alternative):

- Encode WINDOW_UPDATEs directly in the reader task (into write_buf) instead of waking the io_flush_loop. The next io_flush_loop iteration triggered by user data would then flush them along with the real write. But this creates ownership complexity since the reader and io_flush_loop both modify write_buf.

## Diagnostic tools

- `compio-h2/examples/read_diag.rs` — H2 roundtrip latency profiler (min/median/max/p99)
- `compio-h2/examples/probe_uring.rs` — Isolates write latency: io_uring vs blocking, zerocopy vs regular, CQE1 vs CQE2
- `compio-h2/examples/uring_submit_bug.rs` — Reproduces task scheduling patterns (Tests 1-7)

## Current performance

| Payload | compio-h2 median | compio-h2 mean | h2 crate mean |
|---------|-----------------|---------------|--------------|
| 100B | 22µs | 22µs | ~56µs |
| 1KB | 22µs | 22µs | ~71µs |
| 64KB | 74µs | ~11ms | ~103µs |

Median is competitive. Mean is dragged by periodic 44ms WINDOW_UPDATE scheduling stalls.
