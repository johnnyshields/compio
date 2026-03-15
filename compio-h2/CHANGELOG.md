# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Initial release

### Added

- Full HTTP/2 frame codec: DATA, HEADERS, CONTINUATION, SETTINGS, PING, GOAWAY, RST_STREAM, PRIORITY, WINDOW_UPDATE with RFC 9113 stream-ID validation
- Unknown frame types silently ignored per RFC 9113 §4.1
- HEADERS/CONTINUATION splitting on write and assembly on read with flood protection
- HPACK header compression (RFC 7541) with compile-time Huffman decode table via build.rs
- HPACK encoder with O(1) FNV hash-based dynamic table lookup, two-phase size updates, skip indexing for volatile headers
- HPACK decoder with 5-byte integer continuation limit, table size validation against SETTINGS, header list size enforcement
- Build.rs-generated static table lookup using length-bucketed match dispatch
- Per-stream and connection-level flow control with configurable initial window sizes
- Send-side flow control backpressure with pending data queue
- Connection-level WINDOW_UPDATE sent when receive window half-consumed
- Stream-level WINDOW_UPDATE driven by application `release_capacity()` calls
- INITIAL_WINDOW_SIZE delta applied to all open streams on SETTINGS change
- MAX_FRAME_SIZE and MAX_CONCURRENT_STREAMS enforcement (REFUSED_STREAM on limit)
- `ClientBuilder` and `ServerBuilder` with fluent API for all HTTP/2 settings
- Client `SendRequest` handle with `send_request()`, `ready()`, and `shutdown()`
- Client `ResponseFuture` for awaiting response headers
- Server `ServerConnection` with `accept()`, `shutdown()`, and `abrupt_shutdown()`
- `SendStream` with data sending, trailer support, capacity reservation, and RST_STREAM
- `RecvStream` with cancel-safe `data()` and `trailers()` methods
- `RecvFlowControl` for application-managed receive window updates
- Configurable PING/PONG keepalive with interval and timeout settings
- Two-phase GOAWAY graceful shutdown with connection lifecycle tracking (Open/Closing/Closed)
- RST_STREAM flood protection with configurable rate limits and GOAWAY(EnhanceYourCalm)
- Max send buffer size limit (default 400 KiB) to prevent unbounded memory growth
- Settings state machine (Synced/WaitingAck) with queued changes
- Pseudo-header validation: ordering, required fields, CONNECT exemption, no duplicates
- Structured `H2Error` type with `reason()`, `is_io()`, `is_reset()`, `is_connection()`, `is_go_away()`, `is_remote()`, `is_library()` helpers
- `FrameError` and `HpackError` for granular error reporting
- Write buffer batching with persistent Vec and read buffer reuse with persistent BytesMut
- TLS support via `tls` feature gate with `connect`/`accept` wrappers, ALPN `h2` validation, and `native-tls`/`rustls` sub-features forwarding to `compio-tls`
- Cancel safety documentation on all public async methods
- h2spec conformance: passes all 147/147 h2spec tests in strict mode
- 356 tests (246 unit + 110 integration), 4 fuzz targets, 6 conformance/load test scripts
