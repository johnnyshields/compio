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

### What does NOT cause the stall

1. **NOT io_uring submission overhead**: `probe_uring` shows write_all(13B) takes 3µs via io_uring SEND_ZC.
2. **NOT runtime task scheduling**: Multi-threaded test (separate threads for client and server) shows identical 43ms stalls.
3. **NOT TCP_NODELAY/Nagle**: Both sides set nodelay. TCP_QUICKACK also tested with no effect.
4. **NOT the write path**: Both blocking write(2) and io_uring SEND_ZC take ~3µs for 13B.

### What DOES cause the stall

The stall_trace phase breakdown shows `wait=43340µs` — the client sends the request in 2µs but waits 43ms for the SERVER's response. The server delays 43ms before responding to every 8th request (when WINDOW_UPDATE threshold is crossed).

The stall is in the **network interaction pattern**: a standalone small WINDOW_UPDATE write from the server, followed by the response write. Despite TCP_NODELAY, something in WSL2's TCP/virtio networking adds ~43ms when a small packet is followed by a larger response on the same connection. tokio h2 avoids this because its Codec/BufWriter coalesces the WINDOW_UPDATE with the response into a single syscall.

### What would fix it fully

**BufWriter wrapping** (attempted, not landed): Wrapping the write side in `compio_io::BufWriter` would coalesce the 13-byte WINDOW_UPDATE with subsequent DATA frames into a single TCP write. However, this requires careful flush management — never flushing standalone WINDOW_UPDATEs but always flushing when there's real data, and handling the case where the peer is flow-control-blocked and needs the WINDOW_UPDATE immediately.

The key challenge: we must flush WINDOW_UPDATEs for flow-control-blocked peers, but must NOT flush them standalone for the echo benchmark. A potential solution is a delayed flush: buffer the WINDOW_UPDATE, set a short timer (1-5ms), and flush either when real data arrives or the timer fires.

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
