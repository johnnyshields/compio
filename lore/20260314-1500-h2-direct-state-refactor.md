# Fix H2 roundtrip latency: Direct state machine access (compio-quic pattern)

## Problem
H2 roundtrip: **88ms** (44ms send + 44ms recv). Raw TCP: 14µs. Flume channel: 6µs.
Root cause: every user operation (send_data, send_request, etc.) round-trips through a spawned event loop via flume channels. The channel hop + task scheduling adds massive latency.

## Architecture change
Replace channel-based command pattern with compio-quic's **direct state machine access** pattern:

| | Current (channels) | Target (direct access) |
|--|-----|------|
| **State** | Owned by spawned event loop task | Behind `Rc<RefCell<ConnState>>` shared by all handles |
| **Send ops** | `send_async(Command) → recv_async(response)` | `state.borrow_mut() → encode frame → wake IO task` |
| **Recv ops** | Data delivered via per-stream flume channels | `state.borrow() → check buffer → store Waker if empty` |
| **IO task** | Processes ALL commands + frames + flushes | Only reads TCP → delivers frames → flushes write buffer |
| **Wakeups** | Flume channel wakers | `HashMap<StreamId, Waker>` (readable/writable) + poller Waker |

### compio-quic reference pattern (from `compio-quic/src/connection.rs`)
```
ConnectionInner {
    state: Mutex<ConnectionState>,   // All protocol state
    socket: Socket,                   // Network IO
}

ConnectionState {
    conn: quinn_proto::Connection,    // Protocol state machine
    poller: Option<Waker>,            // Wakes IO driver
    writable: HashMap<StreamId, Waker>,
    readable: HashMap<StreamId, Waker>,
}

// User write path (send_stream.rs:158-189):
fn execute_poll_write() {
    let mut state = self.conn.state();     // Lock
    match state.conn.send_stream(id).write_chunks(data) {
        Ok(n) => { state.wake(); Poll::Ready(Ok(n)) }  // Wake IO task
        Err(blocked) => { state.writable.insert(id, waker); Poll::Pending }
    }
}

// IO driver loop (connection.rs:181-292):
select! {
    _ = poller => { /* woken by user write */ }
    _ = timer => { handle_timeout() }
    events = event_stream => { handle_events() }
    buf = transmit_fut => { /* send completed */ }
}
// After any wake: poll_transmit() → send to network
// After events: poll() → wake readable/writable streams
```

## H2 adaptation: Shared state design

### New `ConnShared` (replaces `ConnState` + `Command` + channels)
```rust
// Use Rc<RefCell<>> — compio is single-threaded, no Send/Sync needed
pub(crate) struct ConnShared {
    // Protocol state (moved from ConnState)
    pub(crate) streams: StreamStore,
    pub(crate) hpack_encoder: HpackEncoder,
    pub(crate) hpack_decoder: HpackDecoder,
    pub(crate) settings: ConnSettings,
    pub(crate) conn_send_flow: FlowControl,
    pub(crate) conn_recv_flow: FlowControl,
    pub(crate) conn_recv_consumed: u32,
    pub(crate) is_client: bool,
    pub(crate) going_away: bool,
    pub(crate) last_peer_stream_id: StreamId,
    pub(crate) next_local_stream_id: StreamId,
    pub(crate) pending_send_bytes: usize,
    pub(crate) max_send_buffer_size: usize,
    pub(crate) pending_sends: Vec<PendingSend>,
    pub(crate) pending_capacity: Vec<PendingCapacity>,
    pub(crate) reset_tracker: ResetTracker,
    pub(crate) continuation_bytes: usize,

    // Write buffer: frames encoded by user ops, flushed by IO task
    pub(crate) write_buf: Vec<u8>,

    // Waker maps (compio-quic pattern)
    pub(crate) poller: Option<Waker>,     // IO task waker
    pub(crate) writable: HashMap<StreamId, Waker>,
    pub(crate) readable: HashMap<StreamId, Waker>,
    pub(crate) new_streams: VecDeque<Waker>,  // server accept() waiters
    pub(crate) ready_waiters: VecDeque<Waker>, // client ready() waiters

    // Error state
    pub(crate) error: Option<H2Error>,
}

impl ConnShared {
    /// Wake the IO task to flush write buffer / send WINDOW_UPDATEs
    pub(crate) fn wake_io(&mut self) {
        if let Some(waker) = self.poller.take() { waker.wake(); }
    }
}

type SharedState = Rc<RefCell<ConnShared>>;
```

## Files to modify

### 1. `src/state.rs` (NEW)
- `ConnShared` struct definition (above)
- `SharedState` type alias = `Rc<RefCell<ConnShared>>`
- Helper methods: `wake_io()`, `wake_stream_readable()`, `wake_stream_writable()`, `terminate()`
- Frame encoding helpers that write directly to `write_buf`:
  - `encode_headers()` — HPACK encode + write HEADERS/CONTINUATION frames to buf
  - `encode_data()` — write DATA frame to buf (split on max_frame_size)
  - `encode_rst_stream()`, `encode_goaway()`, `encode_window_update()`, `encode_settings()`

### 2. `src/proto/connection.rs` (MAJOR REFACTOR)
- Remove: `Command` enum, `InternalMsg`, `CommandSender`, `handle_command()`, `dispatch_msg()`
- Keep: `handle_frame()` logic (moved to `ConnShared` methods)
- IO driver becomes simple loop (like compio-quic):
  ```rust
  async fn run_io_driver(state: SharedState, reader: FrameReader<R>, writer_io: W) {
      let mut poller = poll_fn(|cx| { /* register poller waker */ });
      loop {
          select! {
              _ = poller => { /* user wrote frames, flush write_buf */ }
              frame = reader.read_frame() => { /* lock state, process frame, wake streams */ }
              _ = ping_timer => { /* send keepalive ping */ }
          }
          // Flush write_buf to TCP
          let mut state = state.borrow_mut();
          flush_write_buf(&mut state, &mut writer).await;
          // Send WINDOW_UPDATEs, flush pending sends
      }
  }
  ```

### 3. `src/share.rs` (REFACTOR send/recv)
- `SendStream` holds `SharedState` + `StreamId` (not `CommandSender`)
- `send_data()` becomes:
  ```rust
  pub fn poll_send_data(cx, state, stream_id, data, end_stream) -> Poll<Result<()>> {
      let mut s = state.borrow_mut();
      // Check flow control
      let avail = min(s.conn_send_flow.available(), stream_send_avail);
      if avail >= data.len() {
          s.encode_data(stream_id, data, end_stream);
          s.wake_io();  // Signal IO task to flush
          Poll::Ready(Ok(()))
      } else {
          s.writable.insert(stream_id, cx.waker().clone());
          Poll::Pending
      }
  }
  ```
- `RecvStream::data()` — lock state, check stream's data buffer, return or store waker
- `RecvFlowControl::release_capacity()` — lock state, update counters, wake IO if threshold met

### 4. `src/client.rs` (REFACTOR)
- `SendRequest` holds `SharedState`
- `send_request()` — lock state, allocate stream ID, encode HEADERS, wake IO
- `ResponseFuture::await_response()` — poll-based: lock state, check if headers arrived, or store waker
- `ClientConnection::run()` — calls `run_io_driver()`

### 5. `src/server.rs` (REFACTOR)
- `ServerConnection` holds `SharedState`
- `accept()` — poll-based: lock state, check `new_streams` queue, or store waker
- `SendResponse::send_response()` — lock state, encode HEADERS, wake IO
- Server's `run()` — calls `run_io_driver()`

### 6. `src/codec/writer.rs` (SIMPLIFY)
- `FrameWriter` becomes thinner: just writes raw bytes from `write_buf` to TCP
- Remove buffering logic (write_buf IS the buffer now, owned by ConnShared)

### 7. Remove: `src/codec/reader.rs` reader task spawning
- Reader task still exists (reads frames from TCP) but delivers via state lock, not channel:
  ```rust
  // In IO driver loop, after reading a frame:
  let mut state = state.borrow_mut();
  state.handle_frame(frame)?;  // Process frame, update streams, wake waiters
  ```

## Operations conversion summary

| Operation | Current | Target |
|-----------|---------|--------|
| `send_request()` | Channel roundtrip | Lock → encode HEADERS → wake IO → return stream handles |
| `send_data()` | Channel roundtrip | Lock → check flow control → encode DATA → wake IO |
| `send_response()` | Channel roundtrip | Lock → encode HEADERS → wake IO |
| `send_trailers()` | Channel roundtrip | Lock → encode HEADERS+END_STREAM → wake IO |
| `send_reset()` | Channel roundtrip | Lock → encode RST_STREAM → wake IO |
| `shutdown()` | Channel roundtrip | Lock → encode GOAWAY → wake IO |
| `reserve_capacity()` | Channel roundtrip | Lock → check windows → return or store Waker |
| `release_capacity()` | Channel send (fire&forget) | Lock → update counter → wake IO if threshold |
| `ready()` | Channel roundtrip | Lock → check can_accept_stream → return or store Waker |
| `data()` | flume recv_async | Lock → check stream data buffer → return or store Waker |
| `trailers()` | flume recv_async | Lock → check stream trailers → return or store Waker |
| `await_response()` | flume recv_async | Lock → check stream response → return or store Waker |
| `accept()` | flume recv_async | Lock → check incoming queue → return or store Waker |

## Stream data delivery (inbound)
Currently: per-stream `data_tx`/`data_rx` and `trailers_tx`/`trailers_rx` flume channels.
Target: buffered in `StreamStore` entry, waker in `readable` HashMap.
```rust
// In handle_data_frame():
state.streams.get_mut(id).data_buf.push_back(payload);
if let Some(waker) = state.readable.remove(&id) { waker.wake(); }

// In RecvStream::data():
let state = self.state.borrow();
if let Some(data) = state.streams.get(id).data_buf.pop_front() {
    Poll::Ready(Some(Ok(data)))
} else if stream_closed {
    Poll::Ready(None)
} else {
    state.readable.insert(id, cx.waker().clone());
    Poll::Pending
}
```

## Verification
```
cargo test -p compio-h2
cargo run --release -p compio-h2 --example diag_roundtrip  # target: <100µs
cargo bench -p compio-h2 --bench rpc -- "h2_request"       # target: comparable to h2 crate
```
