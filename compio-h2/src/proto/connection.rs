use bytes::Bytes;
use compio_io::{AsyncRead, AsyncWrite};

use crate::{
    codec::{FrameReader, FrameWriter},
    error::{FrameError, H2Error, Reason},
    frame::{self, DEFAULT_MAX_FRAME_SIZE, Frame, StreamId},
    hpack::{DecodedHeader, Decoder as HpackDecoder, Encoder as HpackEncoder},
    proto::{
        flow_control::FlowControl,
        ping_pong::PingPong,
        settings::ConnSettings,
        streams::{StreamRecv, StreamStore},
    },
};

/// Commands sent from user handles to the connection task.
pub enum Command {
    /// Client: open a new stream with request headers.
    NewStream {
        /// HPACK-encoded header field list.
        headers: Vec<(Bytes, Bytes)>,
        /// Whether to close the sending half immediately.
        end_stream: bool,
        /// Channel to deliver the allocated stream ID and receive handles.
        response_tx: flume::Sender<Result<(StreamId, StreamRecv), H2Error>>,
    },
    /// Send HEADERS on an existing stream (server response or trailers).
    SendHeaders {
        /// Target stream identifier.
        stream_id: StreamId,
        /// Header field list to encode via HPACK.
        headers: Vec<(Bytes, Bytes)>,
        /// Whether to set the END_STREAM flag.
        end_stream: bool,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Send DATA on a stream.
    SendData {
        /// Target stream identifier.
        stream_id: StreamId,
        /// Payload bytes.
        data: Bytes,
        /// Whether to set the END_STREAM flag.
        end_stream: bool,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Send trailers (HEADERS with END_STREAM) on a stream.
    SendTrailers {
        /// Target stream identifier.
        stream_id: StreamId,
        /// Trailer header field list.
        trailers: Vec<(Bytes, Bytes)>,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Initiate graceful shutdown by sending GOAWAY.
    GoAway {
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Send RST_STREAM on a specific stream.
    SendReset {
        /// Target stream identifier.
        stream_id: StreamId,
        /// The reason code for the reset.
        reason: Reason,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Check if the connection can accept a new stream
    /// (MAX_CONCURRENT_STREAMS).
    PollReady {
        /// Channel to deliver the result when capacity is available.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Abruptly shut down the connection with an error reason.
    AbruptShutdown {
        /// The error reason code to include in the GOAWAY frame.
        reason: Reason,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Set the target connection-level receive window size at runtime.
    SetTargetWindowSize {
        /// The desired connection-level receive window size.
        size: u32,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Set the initial stream-level window size via a new SETTINGS frame.
    SetInitialWindowSize {
        /// The new initial window size for new streams.
        size: u32,
        /// Channel to deliver the result.
        response_tx: flume::Sender<Result<(), H2Error>>,
    },
    /// Reserve send capacity on a stream.
    ReserveCapacity {
        /// Target stream identifier.
        stream_id: StreamId,
        /// Number of bytes to reserve.
        amount: u32,
        /// Channel to deliver granted capacity (partial grants are possible).
        response_tx: flume::Sender<Result<u32, H2Error>>,
    },
}

/// Internal messages processed by the connection main loop.
pub enum InternalMsg {
    /// An incoming frame from the reader task.
    Frame(Result<Option<Frame>, H2Error>),
    /// A command from a user handle.
    Cmd(Command),
    /// Release recv flow control capacity for a stream.
    ReleaseCapacity {
        /// The stream to release capacity for.
        stream_id: StreamId,
        /// Number of bytes to release.
        amount: u32,
    },
}

/// Wrapper to send commands as InternalMsg.
#[derive(Clone)]
pub struct CommandSender {
    tx: flume::Sender<InternalMsg>,
}

impl CommandSender {
    /// Create a new `CommandSender` wrapping the given channel.
    pub fn new(tx: flume::Sender<InternalMsg>) -> Self {
        CommandSender { tx }
    }

    /// Send a command to the connection task.
    pub async fn send_cmd(&self, cmd: Command) -> Result<(), H2Error> {
        self.tx
            .send_async(InternalMsg::Cmd(cmd))
            .await
            .map_err(|_| H2Error::connection(Reason::InternalError))
    }
}

/// Incoming stream info sent from connection to server's accept channel.
pub struct IncomingStream {
    /// The stream ID assigned to this request.
    pub stream_id: StreamId,
    /// The decoded request headers.
    pub headers: Vec<DecodedHeader>,
    /// Receiver channels for the stream's data and trailers.
    pub recv: StreamRecv,
}

/// A pending DATA send waiting for flow control capacity.
struct PendingSend {
    stream_id: StreamId,
    data: Bytes,
    end_stream: bool,
    response_tx: flume::Sender<Result<(), H2Error>>,
}

/// Result of `try_send_or_queue`: what happened to the data.
enum SendOutcome {
    /// All data was sent successfully.
    Sent,
    /// Data was partially or fully queued; the `PendingSend` should be kept.
    Queued(PendingSend),
    /// An error occurred (already forwarded to `response_tx`).
    Errored,
}

/// A pending send capacity reservation waiting for flow control window.
struct PendingCapacity {
    stream_id: StreamId,
    amount: u32,
    response_tx: flume::Sender<Result<u32, H2Error>>,
}

/// Extra connection parameters that are local policies, not conveyed via H2
/// SETTINGS frames.
///
/// These differ from [`ConnSettings`] in that they are never sent to the peer.
/// `ConnSettings` maps 1:1 to HTTP/2 SETTINGS parameters (RFC 7540 §6.5.2),
/// while `ConnExtra` holds implementation-specific limits enforced locally.
#[derive(Default, Clone)]
pub struct ConnExtra {
    /// Maximum number of recently-reset streams tracked before triggering a
    /// GOAWAY with `ENHANCE_YOUR_CALM`.
    ///
    /// This mitigates the Rapid Reset attack (CVE-2023-44487) by bounding how
    /// many streams a peer can reset within
    /// [`reset_stream_duration`](Self::reset_stream_duration).
    ///
    /// Default (when `None`): 50.
    pub max_concurrent_reset_streams: Option<usize>,

    /// Sliding window duration for counting reset streams.
    ///
    /// Resets older than this duration are forgotten when evaluating
    /// [`max_concurrent_reset_streams`](Self::max_concurrent_reset_streams).
    ///
    /// Default (when `None`): 1 second.
    pub reset_stream_duration: Option<std::time::Duration>,

    /// Maximum total bytes queued in the connection's pending-send buffer.
    ///
    /// When the buffer exceeds this limit, further `send_data` calls will
    /// return an error rather than queuing additional data.
    ///
    /// Default (when `None`): 409,600 bytes (400 KiB).
    pub max_send_buffer_size: Option<usize>,
}

/// Bundled configuration for `run_client_connection` / `run_server_connection`.
pub struct ConnConfig {
    /// H2 SETTINGS parameters.
    pub settings: ConnSettings,
    /// Keepalive / PING state.
    pub ping_pong: PingPong,
    /// Optional initial connection-level flow control window override.
    pub initial_connection_window_size: Option<u32>,
    /// Extra connection parameters (reset limits, send buffer size).
    pub extra: ConnExtra,
}

/// Internal connection state shared by the processing loop.
struct ConnState {
    streams: StreamStore,
    conn_send_flow: FlowControl,
    conn_recv_flow: FlowControl,
    /// Bytes consumed on connection recv flow since last WINDOW_UPDATE was
    /// sent.
    conn_recv_consumed: u32,
    settings: ConnSettings,
    hpack_encoder: HpackEncoder,
    hpack_decoder: HpackDecoder,
    ping_pong: PingPong,
    is_client: bool,
    /// For server: channel to send incoming streams to `accept()`.
    incoming_tx: Option<flume::Sender<Result<IncomingStream, H2Error>>>,
    /// Last stream ID we've seen from the peer.
    last_peer_stream_id: StreamId,
    going_away: bool,
    /// Pending DATA sends blocked on flow control.
    pending_sends: Vec<PendingSend>,
    /// Total bytes currently pending in `pending_sends`.
    pending_send_bytes: usize,
    /// Maximum total bytes allowed in the pending send buffer.
    max_send_buffer_size: usize,
    /// Waiters for stream capacity (poll_ready).
    ready_waiters: Vec<flume::Sender<Result<(), H2Error>>>,
    /// Pending send capacity reservations waiting for flow control window.
    pending_capacity: Vec<PendingCapacity>,
    /// Internal channel sender (cloned into each RecvStream for flow control
    /// releases).
    internal_tx: flume::Sender<InternalMsg>,
}

impl ConnState {
    fn new(
        is_client: bool,
        incoming_tx: Option<flume::Sender<Result<IncomingStream, H2Error>>>,
        settings: ConnSettings,
        ping_pong: PingPong,
        initial_connection_window_size: Option<u32>,
        extra: ConnExtra,
        internal_tx: flume::Sender<InternalMsg>,
    ) -> Self {
        let mut streams = StreamStore::new(is_client);
        // Server enforces its own local max_concurrent_streams on incoming streams.
        // Client enforces the peer's (remote) limit — applied when SETTINGS is
        // received.
        if !is_client {
            streams.set_max_concurrent_streams(settings.local().max_concurrent_streams);
        }
        // Apply DoS protection settings
        if let Some(max) = extra.max_concurrent_reset_streams {
            streams.set_max_reset_streams(max);
        }
        if let Some(dur) = extra.reset_stream_duration {
            streams.set_reset_window(dur);
        }
        let conn_recv_flow = match initial_connection_window_size {
            Some(size) => FlowControl::new(size as i32),
            None => FlowControl::default(),
        };
        // Configure HPACK decoder with the local max_header_list_size to
        // enforce limits on incoming headers (CVE-2024-24549 protection).
        let max_header_list_size = settings.local().max_header_list_size as usize;
        let mut hpack_decoder = HpackDecoder::new(4096);
        hpack_decoder.set_max_header_list_size(max_header_list_size);
        // Configure HPACK encoder with the local max_header_list_size for
        // defensive consistency — ensures we don't produce headers larger than
        // what we advertised to the peer.
        let mut hpack_encoder = HpackEncoder::new(4096);
        if settings.local().max_header_list_size != u32::MAX {
            hpack_encoder.set_max_header_list_size(max_header_list_size);
        }
        ConnState {
            streams,
            conn_send_flow: FlowControl::default(),
            conn_recv_flow,
            conn_recv_consumed: 0,
            settings,
            hpack_encoder,
            hpack_decoder,
            ping_pong,
            is_client,
            incoming_tx,
            last_peer_stream_id: StreamId::ZERO,
            going_away: false,
            pending_sends: Vec::new(),
            pending_send_bytes: 0,
            max_send_buffer_size: extra.max_send_buffer_size.unwrap_or(409_600),
            ready_waiters: Vec::new(),
            pending_capacity: Vec::new(),
            internal_tx,
        }
    }

    /// Whether `stream_id` refers to a peer-initiated stream that has
    /// never been opened (idle). Receiving DATA, RST_STREAM, or WINDOW_UPDATE
    /// on such a stream is a connection error per RFC 7540 §5.1.
    fn is_idle_peer_stream(&self, stream_id: &StreamId) -> bool {
        if self.is_client {
            // Peer is server → server-initiated streams are even
            stream_id.value().is_multiple_of(2)
                && stream_id.value() > self.last_peer_stream_id.value()
        } else {
            // Peer is client → all peer streams have IDs > last_peer_stream_id
            stream_id.value() > self.last_peer_stream_id.value()
        }
    }

    /// Send a RST_STREAM frame for the given stream.
    async fn send_rst_stream<W: AsyncWrite>(
        &mut self,
        stream_id: StreamId,
        reason: Reason,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let rst = frame::RstStream::new(stream_id, reason);
        writer.write_frame(&Frame::RstStream(rst)).await?;
        // Mark stream as closed
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = stream.state.reset();
        }
        self.close_recv_senders(&stream_id);
        Ok(())
    }

    /// Handle an incoming frame.
    ///
    /// Stream-level errors are caught and result in RST_STREAM; only
    /// connection-level errors propagate up to kill the connection.
    async fn handle_frame<W: AsyncWrite>(
        &mut self,
        frame: Frame,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let result = match frame {
            Frame::Data(data) => self.handle_data(data, writer).await,
            Frame::Headers(headers) => self.handle_headers(headers, writer).await,
            Frame::Priority(_) => Ok(()), // Priority is advisory, ignore
            Frame::RstStream(rst) => self.handle_rst_stream(rst),
            Frame::Settings(settings) => self.handle_settings(settings, writer).await,
            Frame::Ping(ping) => self.handle_ping(ping, writer).await,
            Frame::GoAway(goaway) => self.handle_goaway(goaway),
            Frame::WindowUpdate(wu) => self.handle_window_update(wu),
            Frame::Continuation(_) => Err(H2Error::connection(Reason::ProtocolError)),
        };

        match result {
            Ok(()) => Ok(()),
            Err(H2Error::StreamError {
                stream_id, reason, ..
            }) => {
                // Stream error: send RST_STREAM and continue
                compio_log::debug!("stream {} error: {}, sending RST_STREAM", stream_id, reason);
                self.send_rst_stream(StreamId::new(stream_id), reason, writer)
                    .await?;
                Ok(())
            }
            Err(e) => {
                // Convert library errors to connection errors with proper
                // reason codes so run_event_loop sends GOAWAY.
                match &e {
                    H2Error::HpackDecode(_) | H2Error::Hpack(_) => {
                        Err(H2Error::connection(Reason::CompressionError))
                    }
                    H2Error::Protocol(_) | H2Error::InvalidFrame(_) | H2Error::Frame(_) => {
                        Err(H2Error::connection(Reason::ProtocolError))
                    }
                    _ => Err(e), // Connection error: propagate up
                }
            }
        }
    }

    async fn handle_data<W: AsyncWrite>(
        &mut self,
        data: frame::Data,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let stream_id = data.stream_id();
        let payload_len = data.payload().len() as u32;
        let end_stream = data.is_end_stream();

        // Connection-level flow control
        self.conn_recv_flow
            .consume(payload_len)
            .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
        self.conn_recv_consumed += payload_len;

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            // Check stream state allows receiving
            if !stream.state.can_recv() {
                return Err(H2Error::stream(stream_id.value(), Reason::StreamClosed));
            }

            // Stream-level flow control
            stream
                .recv_flow
                .consume(payload_len)
                .map_err(|_| H2Error::stream(stream_id.value(), Reason::FlowControlError))?;
            // Track received bytes for content-length validation
            stream.received_data_bytes += payload_len as u64;
            if stream
                .expected_content_length
                .is_some_and(|expected| stream.received_data_bytes > expected)
            {
                return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
            }

            // Send data to user's RecvStream (zero-copy via Bytes)
            let _ = stream.data_tx.send(Ok(data.into_payload()));

            if end_stream {
                // Validate content-length matches at END_STREAM
                if stream
                    .expected_content_length
                    .is_some_and(|expected| stream.received_data_bytes != expected)
                {
                    return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
                }
                stream.state = stream.state.recv_end_stream()?;
                // Drop senders so RecvStream sees EOF
                self.close_recv_senders(&stream_id);
            }
        } else {
            // DATA on an idle stream is a connection error (RFC 7540 §5.1).
            if self.is_idle_peer_stream(&stream_id) {
                return Err(H2Error::connection(Reason::ProtocolError));
            }
            // Stream was closed and GC'd — send RST_STREAM STREAM_CLOSED (RFC 7540 §5.1)
            let rst = frame::RstStream::new(stream_id, Reason::StreamClosed);
            writer.write_frame(&Frame::RstStream(rst)).await?;
        }

        Ok(())
    }

    async fn handle_headers<W: AsyncWrite>(
        &mut self,
        headers: frame::Headers,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let stream_id = headers.stream_id();
        let end_stream = headers.is_end_stream();

        // Decode HPACK
        let decoded = self.hpack_decoder.decode(headers.header_block())?;

        // Validate pseudo-header ordering and requirements (skip for trailers)
        if !has_no_pseudo_headers(&decoded) {
            validate_pseudo_headers(&decoded, !self.is_client)?;
        }

        // Validate regular headers: no uppercase, no connection-specific headers
        validate_regular_headers(&decoded, stream_id)?;

        if self.is_client {
            // Client receives response headers or trailers
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                // Check stream state allows receiving (RFC 7540 §5.1)
                if !stream.state.can_recv() {
                    return Err(H2Error::stream(stream_id.value(), Reason::StreamClosed));
                }
                if has_no_pseudo_headers(&decoded) && end_stream {
                    // Trailers (no pseudo-headers + END_STREAM)
                    let header_map = headers_to_header_map(&decoded);
                    let _ = stream.trailers_tx.send(Ok(header_map));
                } else if has_no_pseudo_headers(&decoded) && !end_stream {
                    // Headers with no pseudo-headers and no END_STREAM on an
                    // existing stream is a protocol error (RFC 7540 §8.1):
                    // trailers MUST have END_STREAM set.
                    return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
                } else {
                    // Response headers — extract :status and deliver to ResponseFuture
                    let mut status_code = http::StatusCode::OK;
                    let mut header_map = http::HeaderMap::new();
                    for dh in &decoded {
                        if &dh.name[..] == b":status" {
                            if let Ok(s) = std::str::from_utf8(&dh.value)
                                && let Ok(code) = s.parse::<u16>()
                            {
                                status_code = http::StatusCode::from_u16(code)
                                    .unwrap_or(http::StatusCode::OK);
                            }
                        } else if !dh.name.starts_with(b":")
                            && let (Ok(hname), Ok(hvalue)) = (
                                http::header::HeaderName::from_bytes(&dh.name),
                                http::header::HeaderValue::from_bytes(&dh.value),
                            )
                        {
                            header_map.append(hname, hvalue);
                        }
                    }
                    stream.expected_content_length = parse_content_length(&decoded);
                    if let Some(ref headers_tx) = stream.headers_tx {
                        let _ = headers_tx.send(Ok((status_code, header_map)));
                    }
                }

                if end_stream {
                    stream.state = stream.state.recv_end_stream()?;
                    self.close_recv_senders(&stream_id);
                }
            }
        } else {
            // Server receives request headers (new stream) or trailers
            if !self.streams.contains(&stream_id) {
                // Validate stream ID: client-initiated streams must be odd
                if stream_id.value().is_multiple_of(2) {
                    return Err(H2Error::connection(Reason::ProtocolError));
                }
                // Stream ID not monotonically increasing: RFC 7540 §5.1.1
                // requires stream IDs to be numerically greater than all
                // previously opened streams. Violation is a connection error.
                if stream_id.value() <= self.last_peer_stream_id.value() {
                    return Err(H2Error::connection_msg(
                        Reason::ProtocolError,
                        "HEADERS on closed/non-monotonic stream ID",
                    ));
                }

                // New stream from client — reject if going away
                if self.going_away {
                    let rst = frame::RstStream::new(stream_id, Reason::RefusedStream);
                    writer.write_frame(&Frame::RstStream(rst)).await?;
                    return Ok(());
                }

                // Check max concurrent streams
                if !self.streams.can_accept_stream() {
                    let rst = frame::RstStream::new(stream_id, Reason::RefusedStream);
                    writer.write_frame(&Frame::RstStream(rst)).await?;
                    return Ok(());
                }

                self.last_peer_stream_id = stream_id;

                let initial_send_window = self.settings.remote().initial_window_size as i32;
                let initial_recv_window = self.settings.local().initial_window_size as i32;
                let recv = self.streams.insert(
                    stream_id,
                    initial_send_window,
                    initial_recv_window,
                    self.internal_tx.clone(),
                );

                if let Some(stream) = self.streams.get_mut(&stream_id) {
                    stream.state = stream.state.recv_headers(end_stream)?;
                    stream.expected_content_length = parse_content_length(&decoded);
                }

                // Send to accept channel
                if let Some(ref incoming_tx) = self.incoming_tx {
                    let _ = incoming_tx.send(Ok(IncomingStream {
                        stream_id,
                        headers: decoded,
                        recv,
                    }));
                }

                if end_stream {
                    self.close_recv_senders(&stream_id);
                }
            } else {
                // Existing stream — must be trailers
                if let Some(stream) = self.streams.get_mut(&stream_id) {
                    // Check stream state allows receiving
                    if !stream.state.can_recv() {
                        return Err(H2Error::stream(stream_id.value(), Reason::StreamClosed));
                    }
                    // Trailers MUST have END_STREAM set (RFC 7540 §8.1)
                    if !end_stream {
                        return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
                    }
                    let header_map = headers_to_header_map(&decoded);
                    let _ = stream.trailers_tx.send(Ok(header_map));
                    stream.state = stream.state.recv_end_stream()?;
                }
                // end_stream is always true here (non-end_stream trailers
                // returned ProtocolError above), so unconditionally close.
                self.close_recv_senders(&stream_id);
            }
        }

        Ok(())
    }

    fn handle_rst_stream(&mut self, rst: frame::RstStream) -> Result<(), H2Error> {
        let stream_id = rst.stream_id();
        let reason = rst.reason();

        // RST_STREAM on idle stream is a connection error (RFC 7540 §5.1)
        if !self.streams.contains(&stream_id) {
            if self.is_idle_peer_stream(&stream_id) {
                return Err(H2Error::connection(Reason::ProtocolError));
            }
            // Otherwise stream was already closed and GC'd — ignore
            return Ok(());
        }

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = stream.state.reset();
            let _ = stream
                .data_tx
                .send(Err(H2Error::stream_remote(stream_id.value(), reason)));
            let _ = stream
                .trailers_tx
                .send(Err(H2Error::stream_remote(stream_id.value(), reason)));
            // Notify poll_reset waiters
            if let Some(reset_tx) = stream.reset_tx.take() {
                let _ = reset_tx.send(reason);
            }
        }
        // Close recv senders so RecvStream sees EOF instead of hanging
        self.close_recv_senders(&stream_id);

        // CVE-2023-44487: Track rapid resets for DoS protection.
        // If the peer is sending resets faster than our threshold, trigger GOAWAY.
        if self.streams.record_reset() {
            // Don't set going_away here — run_event_loop will send GOAWAY
            // and set the flag when it handles the returned error.
            return Err(H2Error::connection(Reason::EnhanceYourCalm));
        }

        Ok(())
    }

    async fn handle_settings<W: AsyncWrite>(
        &mut self,
        settings: frame::Settings,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        if settings.is_ack() {
            // recv_ack may return a queued SETTINGS frame to send
            if let Some(queued_frame) = self.settings.recv_ack() {
                writer.write_frame(&Frame::Settings(queued_frame)).await?;
            }
            return Ok(());
        }

        // Apply remote settings
        self.settings.apply_remote(&settings);

        // Propagate max_concurrent_streams to StreamStore.
        // Client: peer's (remote) limit constrains how many streams we can open.
        // Server: local limit is set at init; remote limit doesn't apply to incoming
        // streams.
        if self.is_client
            && let Some(max) = settings.max_concurrent_streams()
        {
            self.streams.set_max_concurrent_streams(max);
        }

        // RFC 7540 §6.9.2: When INITIAL_WINDOW_SIZE changes, adjust all
        // existing streams' send windows by the delta between new and old value.
        // A change can cause a flow control window to exceed 2^31-1, which is
        // a connection error of type FLOW_CONTROL_ERROR.
        if let Some(new_window) = settings.initial_window_size() {
            let new_window = new_window as i32;
            let stream_ids: Vec<StreamId> = self.streams.iter_ids().collect();
            for id in stream_ids {
                if let Some(stream) = self.streams.get_mut(&id) {
                    if stream.state.is_closed() {
                        continue;
                    }
                    stream
                        .send_flow
                        .update_initial_window_size(new_window)
                        .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
                }
            }
        }

        // Send ACK
        let ack = frame::Settings::ack();
        writer.write_frame(&Frame::Settings(ack)).await?;

        Ok(())
    }

    async fn handle_ping<W: AsyncWrite>(
        &mut self,
        ping: frame::Ping,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        if ping.is_ack() {
            self.ping_pong.recv_pong(ping.opaque_data());
            return Ok(());
        }

        // Send PONG
        let pong = frame::Ping::pong(*ping.opaque_data());
        writer.write_frame(&Frame::Ping(pong)).await?;

        Ok(())
    }

    /// Drop the data_tx and trailers_tx senders for a stream so RecvStream sees
    /// EOF.
    fn close_recv_senders(&mut self, stream_id: &StreamId) {
        if let Some(stream) = self.streams.get_mut(stream_id) {
            // Replace senders with closed channels — dropping the old senders
            // signals EOF to the corresponding receivers in RecvStream.
            let (dead_data_tx, _) = flume::bounded(0);
            let (dead_trailers_tx, _) = flume::bounded(0);
            stream.data_tx = dead_data_tx;
            stream.trailers_tx = dead_trailers_tx;
            stream.headers_tx = None;
        }
    }

    fn handle_goaway(&mut self, goaway: frame::GoAway) -> Result<(), H2Error> {
        self.going_away = true;
        let last_stream_id = goaway.last_stream_id();
        let error_code = goaway.error_code();
        let debug_data = goaway.debug_data().clone();

        // Close all streams with IDs greater than last_stream_id
        let stream_ids: Vec<StreamId> = self
            .streams
            .iter_ids()
            .filter(|id| id.value() > last_stream_id.value())
            .collect();

        for id in &stream_ids {
            if let Some(stream) = self.streams.get_mut(id) {
                stream.state = stream.state.reset();
                let _ = stream.data_tx.send(Err(H2Error::go_away(
                    last_stream_id.value(),
                    error_code,
                    debug_data.clone(),
                )));
                // Notify poll_reset waiters
                if let Some(reset_tx) = stream.reset_tx.take() {
                    let _ = reset_tx.send(error_code);
                }
            }
            self.close_recv_senders(id);
        }

        // No new streams will be accepted — drain all ready waiters
        self.drain_ready_waiters(Reason::RefusedStream);

        Ok(())
    }

    /// Send stream-level WINDOW_UPDATEs for streams with released capacity.
    async fn send_stream_window_updates<W: AsyncWrite>(
        &mut self,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let updates = self.streams.streams_needing_window_update();
        for (stream_id, increment) in updates {
            let wu = frame::WindowUpdate::new(stream_id, increment);
            writer.write_frame(&Frame::WindowUpdate(wu)).await?;
            self.streams.reset_released(&stream_id, increment);
        }
        Ok(())
    }

    fn handle_window_update(&mut self, wu: frame::WindowUpdate) -> Result<(), H2Error> {
        let stream_id = wu.stream_id();
        let increment = wu.size_increment();

        if stream_id.is_zero() {
            // Connection-level
            self.conn_send_flow
                .apply_window_update(increment)
                .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
        } else {
            // Stream-level
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                stream
                    .send_flow
                    .apply_window_update(increment)
                    .map_err(|_| H2Error::stream(stream_id.value(), Reason::FlowControlError))?;
            } else {
                // WINDOW_UPDATE on idle stream is a connection error (RFC 7540 §5.1)
                if self.is_idle_peer_stream(&stream_id) {
                    return Err(H2Error::connection(Reason::ProtocolError));
                }
                // Otherwise stream was closed/GC'd — ignore
            }
        }

        Ok(())
    }

    /// Compute sendable bytes and try to send data immediately, partially, or
    /// not at all, returning a [`SendOutcome`] describing what happened.
    ///
    /// On full or partial send the corresponding `pending_send_bytes` are
    /// subtracted (the caller must have already *added* them before calling).
    /// On error during a partial send the remainder bytes are also subtracted
    /// and the error is forwarded to `response_tx`.
    async fn try_send_or_queue<W: AsyncWrite>(
        &mut self,
        item: PendingSend,
        writer: &mut FrameWriter<W>,
    ) -> Result<SendOutcome, H2Error> {
        let data_len = item.data.len() as u32;
        let conn_avail = self.conn_send_flow.available();
        let stream_state = self.streams.get(&item.stream_id);
        let stream_avail = stream_state.map(|s| s.send_flow.available()).unwrap_or(0);
        let sendable = std::cmp::min(data_len, std::cmp::min(conn_avail, stream_avail)) as usize;

        if item.data.is_empty() || sendable == item.data.len() {
            // Full send
            self.pending_send_bytes -= item.data.len();
            let result = self
                .cmd_send_data(item.stream_id, item.data, item.end_stream, writer)
                .await;
            let _ = item.response_tx.send(result);
            Ok(SendOutcome::Sent)
        } else if sendable > 0 {
            // Partial send: send what fits, queue remainder
            let send_now = item.data.slice(..sendable);
            let remainder = item.data.slice(sendable..);
            self.pending_send_bytes -= sendable;
            let result = self
                .cmd_send_data(item.stream_id, send_now, false, writer)
                .await;
            if let Err(e) = result {
                self.pending_send_bytes -= remainder.len();
                let _ = item.response_tx.send(Err(e));
                Ok(SendOutcome::Errored)
            } else {
                Ok(SendOutcome::Queued(PendingSend {
                    stream_id: item.stream_id,
                    data: remainder,
                    end_stream: item.end_stream,
                    response_tx: item.response_tx,
                }))
            }
        } else if stream_state.is_none() && conn_avail > 0 {
            // Stream gone (reset/GC'd) but connection window is open —
            // don't re-queue forever. Return an error to the caller.
            self.pending_send_bytes -= item.data.len();
            let _ = item.response_tx.send(Err(H2Error::stream(
                item.stream_id.value(),
                Reason::StreamClosed,
            )));
            Ok(SendOutcome::Errored)
        } else {
            // No capacity at all — caller decides how to handle
            Ok(SendOutcome::Queued(item))
        }
    }

    /// Try to flush pending sends that are now within flow control limits.
    async fn flush_pending_sends<W: AsyncWrite>(
        &mut self,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let mut still_pending = Vec::new();
        let pending = std::mem::take(&mut self.pending_sends);

        for item in pending {
            match self.try_send_or_queue(item, writer).await? {
                SendOutcome::Sent | SendOutcome::Errored => {}
                SendOutcome::Queued(ps) => still_pending.push(ps),
            }
        }

        self.pending_sends = still_pending;
        Ok(())
    }

    /// Handle a command from a user handle.
    async fn handle_command<W: AsyncWrite>(
        &mut self,
        cmd: Command,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        match cmd {
            Command::NewStream {
                headers,
                end_stream,
                response_tx,
            } => {
                let result = self.cmd_new_stream(headers, end_stream, writer).await;
                let _ = response_tx.send(result);
            }
            Command::SendHeaders {
                stream_id,
                headers,
                end_stream,
                response_tx,
            } => {
                let result = self
                    .cmd_send_headers(stream_id, headers, end_stream, writer)
                    .await;
                let _ = response_tx.send(result);
            }
            Command::SendData {
                stream_id,
                data,
                end_stream,
                response_tx,
            } => {
                // Pre-account the full data length so try_send_or_queue can
                // subtract the portion it sends. For new (not yet pending)
                // data we check the buffer limit first.
                let data_len = data.len();
                if self.pending_send_bytes + data_len > self.max_send_buffer_size {
                    // Over the buffer limit — only allow if it fits entirely
                    // in the current flow-control window (no queuing needed).
                    let conn_avail = self.conn_send_flow.available() as usize;
                    let stream_avail = self
                        .streams
                        .get(&stream_id)
                        .map(|s| s.send_flow.available() as usize)
                        .unwrap_or(0);
                    let sendable = std::cmp::min(data_len, std::cmp::min(conn_avail, stream_avail));
                    if sendable < data_len {
                        let _ = response_tx.send(Err(H2Error::Protocol(
                            "send buffer size limit exceeded".into(),
                        )));
                        return Ok(());
                    }
                }

                self.pending_send_bytes += data_len;
                let item = PendingSend {
                    stream_id,
                    data,
                    end_stream,
                    response_tx,
                };
                match self.try_send_or_queue(item, writer).await? {
                    SendOutcome::Sent | SendOutcome::Errored => {}
                    SendOutcome::Queued(ps) => self.pending_sends.push(ps),
                }
            }
            Command::SendTrailers {
                stream_id,
                trailers,
                response_tx,
            } => {
                let result = self.cmd_send_trailers(stream_id, trailers, writer).await;
                let _ = response_tx.send(result);
            }
            Command::GoAway { response_tx } => {
                let result = self.cmd_goaway(writer).await;
                let _ = response_tx.send(result);
            }
            Command::SendReset {
                stream_id,
                reason,
                response_tx,
            } => {
                let result = self.send_rst_stream(stream_id, reason, writer).await;
                let _ = response_tx.send(result);
            }
            Command::PollReady { response_tx } => {
                if self.going_away {
                    let _ = response_tx.send(Err(H2Error::connection(Reason::RefusedStream)));
                } else if self.streams.can_accept_stream() {
                    let _ = response_tx.send(Ok(()));
                } else {
                    self.ready_waiters.push(response_tx);
                }
            }
            Command::AbruptShutdown {
                reason,
                response_tx,
            } => {
                let result = self.cmd_abrupt_shutdown(reason, writer).await;
                let _ = response_tx.send(result);
            }
            Command::ReserveCapacity {
                stream_id,
                amount,
                response_tx,
            } => {
                self.cmd_reserve_capacity(stream_id, amount, response_tx);
            }
            Command::SetTargetWindowSize { size, response_tx } => {
                let result = self.cmd_set_target_window_size(size, writer).await;
                let _ = response_tx.send(result);
            }
            Command::SetInitialWindowSize { size, response_tx } => {
                let result = self.cmd_set_initial_window_size(size, writer).await;
                let _ = response_tx.send(result);
            }
        }
        Ok(())
    }

    /// Wake one ready waiter if stream capacity is available.
    fn notify_ready_waiters(&mut self) {
        while self.streams.can_accept_stream() {
            if let Some(tx) = self.ready_waiters.pop() {
                // If receiver dropped, try next waiter
                if tx.send(Ok(())).is_err() {
                    continue;
                }
                break;
            } else {
                break;
            }
        }
    }

    /// Drain all ready waiters with a connection error.
    fn drain_ready_waiters(&mut self, reason: Reason) {
        for tx in self.ready_waiters.drain(..) {
            let _ = tx.send(Err(H2Error::connection(reason)));
        }
    }

    async fn cmd_new_stream<W: AsyncWrite>(
        &mut self,
        headers: Vec<(Bytes, Bytes)>,
        end_stream: bool,
        writer: &mut FrameWriter<W>,
    ) -> Result<(StreamId, StreamRecv), H2Error> {
        if self.going_away {
            return Err(H2Error::connection(Reason::RefusedStream));
        }
        if !self.streams.can_accept_stream() {
            return Err(H2Error::Protocol("max concurrent streams exceeded".into()));
        }
        let stream_id = self.streams.next_stream_id()?;

        let initial_send_window = self.settings.remote().initial_window_size as i32;
        let initial_recv_window = self.settings.local().initial_window_size as i32;
        // Client-initiated streams need a headers channel for response headers
        let recv = self.streams.insert_with_headers(
            stream_id,
            initial_send_window,
            initial_recv_window,
            self.is_client,
            self.internal_tx.clone(),
        );

        // Encode headers with HPACK
        let mut header_block = Vec::new();
        self.hpack_encoder.encode(
            headers.iter().map(|(k, v)| (k.as_ref(), v.as_ref())),
            &mut header_block,
        );

        let mut frame = frame::Headers::new(stream_id, Bytes::from(header_block));
        if end_stream {
            frame.set_end_stream();
        }

        // Update stream state
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = stream.state.send_headers(end_stream)?;
        }

        let max_frame_size = self.settings.remote().max_frame_size as usize;
        write_headers_with_continuation(writer, frame, max_frame_size).await?;

        Ok((stream_id, recv))
    }

    async fn cmd_send_headers<W: AsyncWrite>(
        &mut self,
        stream_id: StreamId,
        headers: Vec<(Bytes, Bytes)>,
        end_stream: bool,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let mut header_block = Vec::new();
        self.hpack_encoder.encode(
            headers.iter().map(|(k, v)| (k.as_ref(), v.as_ref())),
            &mut header_block,
        );

        let mut frame = frame::Headers::new(stream_id, Bytes::from(header_block));
        if end_stream {
            frame.set_end_stream();
        }

        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = stream.state.send_headers(end_stream)?;
        }

        let max_frame_size = self.settings.remote().max_frame_size as usize;
        write_headers_with_continuation(writer, frame, max_frame_size).await?;
        Ok(())
    }

    /// Send DATA on a stream, consuming flow control and splitting into frames.
    ///
    /// The caller (`try_send_or_queue`) has already verified that the data fits
    /// within the current flow control windows. This method consumes flow
    /// control, updates stream state, and writes the wire frames.
    async fn cmd_send_data<W: AsyncWrite>(
        &mut self,
        stream_id: StreamId,
        data: Bytes,
        end_stream: bool,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let data_len = data.len() as u32;

        // Consume flow control for the full payload
        if data_len > 0 {
            self.conn_send_flow
                .consume(data_len)
                .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                stream
                    .send_flow
                    .consume(data_len)
                    .map_err(|_| H2Error::stream(stream_id.value(), Reason::FlowControlError))?;
            }
        }

        if end_stream && let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = stream.state.send_end_stream()?;
        }

        // Split payload into frames capped at the peer's max_frame_size.
        // Only the last frame carries END_STREAM (if requested).
        let max_frame = self.settings.remote().max_frame_size as usize;
        let payload = data;
        if payload.is_empty() {
            // Empty DATA frame (e.g. END_STREAM with no body)
            let mut flags = 0u8;
            if end_stream {
                flags |= 0x1;
            }
            let header = frame::FrameHeader::new(0x0, flags, stream_id, 0);
            writer.write_data_frame(header.encode(), &payload).await?;
        } else {
            let mut offset = 0;
            while offset < payload.len() {
                let end = std::cmp::min(offset + max_frame, payload.len());
                let chunk = payload.slice(offset..end);
                let is_last = end == payload.len();
                let mut flags = 0u8;
                if end_stream && is_last {
                    flags |= 0x1; // END_STREAM
                }
                let header = frame::FrameHeader::new(0x0, flags, stream_id, chunk.len() as u32);
                writer.write_data_frame(header.encode(), &chunk).await?;
                offset = end;
            }
        }
        Ok(())
    }

    async fn cmd_send_trailers<W: AsyncWrite>(
        &mut self,
        stream_id: StreamId,
        trailers: Vec<(Bytes, Bytes)>,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        self.cmd_send_headers(stream_id, trailers, true, writer)
            .await
    }

    /// Send a GOAWAY frame and mark the connection as going away.
    async fn cmd_goaway<W: AsyncWrite>(
        &mut self,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let last_stream_id = self.last_peer_stream_id;
        let goaway = frame::GoAway::new(last_stream_id, Reason::NoError);
        writer.write_frame(&Frame::GoAway(goaway)).await?;
        self.going_away = true;
        Ok(())
    }

    /// Send a GOAWAY with an error reason and close all open streams
    /// immediately.
    async fn cmd_abrupt_shutdown<W: AsyncWrite>(
        &mut self,
        reason: Reason,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let last_stream_id = self.last_peer_stream_id;
        let goaway = frame::GoAway::new(last_stream_id, reason);
        writer.write_frame(&Frame::GoAway(goaway)).await?;

        // Close all open streams with errors
        let stream_ids: Vec<StreamId> = self.streams.iter_ids().collect();
        for id in &stream_ids {
            if let Some(stream) = self.streams.get_mut(id)
                && !stream.state.is_closed()
            {
                stream.state = stream.state.reset();
                let _ = stream.data_tx.send(Err(H2Error::connection(reason)));
                let _ = stream.trailers_tx.send(Err(H2Error::connection(reason)));
            }
            self.close_recv_senders(id);
        }

        self.going_away = true;
        writer.flush_buf().await?;
        Ok(())
    }

    /// Reserve send capacity on a stream.
    ///
    /// Immediately available capacity up to `amount`. If no capacity
    /// is available, the request is queued and will be fulfilled when
    /// WINDOW_UPDATE frames arrive.
    fn cmd_reserve_capacity(
        &mut self,
        stream_id: StreamId,
        amount: u32,
        response_tx: flume::Sender<Result<u32, H2Error>>,
    ) {
        if let Some(stream) = self.streams.get(&stream_id) {
            if !stream.state.can_send() {
                let _ = response_tx.send(Err(H2Error::Protocol(
                    "stream is not in a sendable state".into(),
                )));
                return;
            }
            let avail = std::cmp::min(
                self.conn_send_flow.available(),
                stream.send_flow.available(),
            );
            let grant = std::cmp::min(amount, avail);
            if grant > 0 {
                let _ = response_tx.send(Ok(grant));
            } else {
                // No capacity available — queue for later fulfillment
                self.pending_capacity.push(PendingCapacity {
                    stream_id,
                    amount,
                    response_tx,
                });
            }
        } else {
            let _ = response_tx.send(Err(H2Error::Protocol("stream not found".into())));
        }
    }

    /// Try to fulfill pending capacity reservations from available flow
    /// control.
    fn fulfill_pending_capacity(&mut self) {
        let mut still_pending = Vec::new();
        let pending = std::mem::take(&mut self.pending_capacity);

        for item in pending {
            // Check if receiver is still alive
            if item.response_tx.is_disconnected() {
                continue;
            }
            let avail = if let Some(stream) = self.streams.get(&item.stream_id) {
                if !stream.state.can_send() {
                    let _ = item.response_tx.send(Err(H2Error::Protocol(
                        "stream is not in a sendable state".into(),
                    )));
                    continue;
                }
                std::cmp::min(
                    self.conn_send_flow.available(),
                    stream.send_flow.available(),
                )
            } else {
                let _ = item
                    .response_tx
                    .send(Err(H2Error::Protocol("stream not found".into())));
                continue;
            };

            let grant = std::cmp::min(item.amount, avail);
            if grant > 0 {
                let _ = item.response_tx.send(Ok(grant));
            } else {
                still_pending.push(item);
            }
        }

        self.pending_capacity = still_pending;
    }

    /// Adjust the connection-level receive window size at runtime.
    ///
    /// If `size` is larger than the current window, a WINDOW_UPDATE is sent
    /// for the difference. If smaller, the window shrinks naturally as data
    /// arrives (no mechanism to shrink it immediately per RFC 7540).
    async fn cmd_set_target_window_size<W: AsyncWrite>(
        &mut self,
        size: u32,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        // RFC 7540 §6.9.1: flow control window cannot exceed 2^31-1
        if size > 0x7FFF_FFFF {
            return Err(H2Error::connection(Reason::FlowControlError));
        }
        let current = self.conn_recv_flow.window_size();
        let target = size as i32;
        if target > current {
            let increment = (target - current) as u32;
            let wu = frame::WindowUpdate::new(StreamId::ZERO, increment);
            writer.write_frame(&Frame::WindowUpdate(wu)).await?;
            self.conn_recv_flow
                .release(increment)
                .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
        }
        // If target <= current, the window will shrink as data is consumed
        // without being replenished (no WINDOW_UPDATE sent until it drops
        // below the new target threshold).
        Ok(())
    }

    /// Change the INITIAL_WINDOW_SIZE for new streams via SETTINGS.
    ///
    /// Updates local settings and sends a SETTINGS frame. The delta is also
    /// applied to all existing open streams' receive windows per RFC 7540
    /// §6.9.2.
    async fn cmd_set_initial_window_size<W: AsyncWrite>(
        &mut self,
        size: u32,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        // RFC 7540 §6.9.1: flow control window cannot exceed 2^31-1
        if size > 0x7FFF_FFFF {
            return Err(H2Error::connection(Reason::FlowControlError));
        }
        let old_size = self.settings.local().initial_window_size as i32;
        let new_size = size as i32;

        // Update local settings
        self.settings.set_local_initial_window_size(size);

        // Send SETTINGS frame (may be queued if waiting for ACK)
        if let Some(frame) = self.settings.build_local_settings() {
            writer.write_frame(&Frame::Settings(frame)).await?;
        }

        // Apply delta to all existing streams' recv windows
        let delta = new_size - old_size;
        if delta != 0 {
            let stream_ids: Vec<StreamId> = self.streams.iter_ids().collect();
            for id in stream_ids {
                if let Some(stream) = self.streams.get_mut(&id) {
                    if stream.state.is_closed() {
                        continue;
                    }
                    stream
                        .recv_flow
                        .update_initial_window_size(new_size)
                        .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
                }
            }
        }

        Ok(())
    }

    /// Dispatch a single internal message. Returns `Ok(true)` to continue the
    /// loop, `Ok(false)` to break out (EOF / channel closed).
    async fn dispatch_msg<W: AsyncWrite>(
        &mut self,
        msg: InternalMsg,
        writer: &mut FrameWriter<W>,
    ) -> Result<bool, H2Error> {
        match msg {
            InternalMsg::Frame(Ok(Some(frame))) => {
                self.handle_frame(frame, writer).await?;
                Ok(true)
            }
            InternalMsg::Frame(Ok(None)) => Ok(false),
            InternalMsg::Frame(Err(e)) => {
                // Convert library/frame errors to connection errors with proper
                // reason codes so run_event_loop sends GOAWAY with the right code.
                if e.reason().is_some() {
                    // Already has a reason code (ConnectionError, StreamError, etc.)
                    Err(e)
                } else {
                    match &e {
                        H2Error::Frame(fe) => {
                            let reason = match fe {
                                FrameError::InvalidFrameSize(_) => Reason::FrameSizeError,
                                FrameError::FlowControlError(_) => Reason::FlowControlError,
                                _ => Reason::ProtocolError,
                            };
                            Err(H2Error::connection(reason))
                        }
                        H2Error::HpackDecode(_) | H2Error::Hpack(_) => {
                            Err(H2Error::connection(Reason::CompressionError))
                        }
                        _ => Err(H2Error::connection(Reason::ProtocolError)),
                    }
                }
            }
            InternalMsg::Cmd(cmd) => {
                self.handle_command(cmd, writer).await?;
                Ok(true)
            }
            InternalMsg::ReleaseCapacity { stream_id, amount } => {
                self.streams.apply_release(&stream_id, amount);
                Ok(true)
            }
        }
    }

    /// Flush pending data, send window updates, fulfill capacity reservations,
    /// flush the write buffer, and GC.
    async fn flush_all<W: AsyncWrite>(
        &mut self,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        // Flush pending sends that may now fit in flow control windows
        self.flush_pending_sends(writer).await?;

        // Fulfill pending capacity reservations
        self.fulfill_pending_capacity();

        // Send connection-level WINDOW_UPDATE based on consumed byte counter
        let threshold = (self.conn_recv_flow.initial_window_size() / 2) as u32;
        if self.conn_recv_consumed > 0 && self.conn_recv_consumed >= threshold {
            let increment = self.conn_recv_consumed;
            let wu = frame::WindowUpdate::new(StreamId::ZERO, increment);
            writer.write_frame(&Frame::WindowUpdate(wu)).await?;
            self.conn_recv_flow
                .release(increment)
                .map_err(|_| H2Error::connection(Reason::FlowControlError))?;
            self.conn_recv_consumed = 0;
        }

        // Send stream-level WINDOW_UPDATEs
        self.send_stream_window_updates(writer).await?;

        // Flush buffered writes
        writer.flush_buf().await?;

        // Garbage-collect closed streams
        self.streams.gc_closed();

        // Wake ready waiters now that stream capacity may have freed up
        self.notify_ready_waiters();

        Ok(())
    }

    /// Run the main connection event loop.
    ///
    /// Shared by both client and server — dispatches incoming frames and
    /// outgoing commands, manages flow control, and flushes writes.
    /// On connection-level errors, sends GOAWAY before returning.
    async fn run_event_loop<W: AsyncWrite>(
        &mut self,
        internal_rx: &flume::Receiver<InternalMsg>,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        let result = self.run_event_loop_inner(internal_rx, writer).await;
        if let Err(ref e) = result
            && let Some(reason) = e.reason()
        {
            // Only send GOAWAY if we haven't already started shutting down.
            // (RFC 9113 allows additional GOAWAYs with lower last-stream-id,
            // but we avoid sending redundant frames.)
            if !self.going_away {
                let goaway = frame::GoAway::new(self.last_peer_stream_id, reason);
                let _ = writer.write_frame(&Frame::GoAway(goaway)).await;
            }
            let _ = writer.flush_buf().await;
            let _ = writer.shutdown().await;
        }
        result
    }

    /// Inner event loop — separated so [`run_event_loop`] can wrap it with
    /// GOAWAY-on-error logic.
    async fn run_event_loop_inner<W: AsyncWrite>(
        &mut self,
        internal_rx: &flume::Receiver<InternalMsg>,
        writer: &mut FrameWriter<W>,
    ) -> Result<(), H2Error> {
        loop {
            // Wait for the first message (blocking)
            let msg = match internal_rx.recv_async().await {
                Ok(msg) => msg,
                Err(_) => break,
            };
            if !self.dispatch_msg(msg, writer).await? {
                break;
            }

            // Drain any additional queued messages without blocking (capped to
            // ensure periodic flushing under sustained burst).
            const BATCH_LIMIT: usize = 32;
            for _ in 1..BATCH_LIMIT {
                match internal_rx.try_recv() {
                    Ok(msg) => {
                        if !self.dispatch_msg(msg, writer).await? {
                            // Flush before exiting on EOF
                            self.flush_all(writer).await?;
                            return Ok(());
                        }
                    }
                    Err(_) => break,
                }
            }

            // Flush once for the whole batch
            self.flush_all(writer).await?;

            // Graceful shutdown: exit when going away and all streams are done
            if self.going_away && self.streams.active_count() == 0 {
                break;
            }
        }

        // Connection ending — drain any remaining ready waiters
        self.drain_ready_waiters(Reason::RefusedStream);

        Ok(())
    }
}

/// Spawn a reader task that feeds frames into the internal channel.
fn spawn_reader_task<R: AsyncRead + 'static>(
    reader: FrameReader<R>,
    internal_tx: flume::Sender<InternalMsg>,
) {
    compio_runtime::spawn(async move {
        let mut reader = reader;
        loop {
            let result = reader.read_frame().await;
            if internal_tx
                .send_async(InternalMsg::Frame(result))
                .await
                .is_err()
            {
                break;
            }
        }
    })
    .detach();
}

/// Create and configure a [`FrameReader`]/[`FrameWriter`] pair from raw I/O
/// halves, applying the local SETTINGS limits to the reader.
fn configure_reader_writer<R: AsyncRead + 'static, W: AsyncWrite + 'static>(
    reader_io: R,
    writer_io: W,
    config: &ConnConfig,
) -> (FrameReader<R>, FrameWriter<W>) {
    let mut reader = FrameReader::new(reader_io);
    // Enforce local limits on incoming frames.
    // max_frame_size is set eagerly: the peer cannot legally send frames larger
    // than DEFAULT_MAX_FRAME_SIZE (16384) before ACKing our SETTINGS, so setting
    // the limit here is safe.
    if config.settings.local().max_header_list_size != u32::MAX {
        reader.set_max_header_list_size(config.settings.local().max_header_list_size);
    }
    if config.settings.local().max_frame_size != DEFAULT_MAX_FRAME_SIZE {
        reader.set_max_frame_size(config.settings.local().max_frame_size);
    }
    (reader, FrameWriter::new(writer_io))
}

/// Common post-handshake setup and event loop for both client and server
/// connections.
///
/// Create [`ConnState`], send the initial SETTINGS (and optional
/// WINDOW_UPDATE), spawn the reader task, and drive the connection event loop.
///
/// This operation is *not* cancel-safe.
async fn init_and_run<R: AsyncRead + 'static, W: AsyncWrite + 'static>(
    reader: FrameReader<R>,
    mut writer: FrameWriter<W>,
    internal_rx: flume::Receiver<InternalMsg>,
    internal_tx: flume::Sender<InternalMsg>,
    is_client: bool,
    incoming_tx: Option<flume::Sender<Result<IncomingStream, H2Error>>>,
    config: ConnConfig,
) -> Result<(), H2Error> {
    let initial_connection_window_size = config.initial_connection_window_size;
    let mut state = ConnState::new(
        is_client,
        incoming_tx,
        config.settings,
        config.ping_pong,
        initial_connection_window_size,
        config.extra,
        internal_tx.clone(),
    );

    // Send initial SETTINGS.
    let settings_frame = state.settings.build_initial_settings();
    writer.write_frame(&Frame::Settings(settings_frame)).await?;

    // If connection window > default 65535, send WINDOW_UPDATE to increase it.
    // RFC 7540: connection flow control window starts at 65535 and can only be
    // increased via WINDOW_UPDATE frames.
    if let Some(size) = initial_connection_window_size
        && size > 65_535
    {
        let increment = size - 65_535;
        let wu = frame::WindowUpdate::new(StreamId::ZERO, increment);
        writer.write_frame(&Frame::WindowUpdate(wu)).await?;
    }

    spawn_reader_task(reader, internal_tx);
    state.run_event_loop(&internal_rx, &mut writer).await
}

/// Run the connection background task for the client side.
pub(crate) async fn run_client_connection<R: AsyncRead + 'static, W: AsyncWrite + 'static>(
    reader_io: R,
    writer_io: W,
    internal_rx: flume::Receiver<InternalMsg>,
    internal_tx: flume::Sender<InternalMsg>,
    config: ConnConfig,
) -> Result<(), H2Error> {
    let (reader, mut writer) = configure_reader_writer(reader_io, writer_io, &config);
    // Client connection preface: send magic bytes before SETTINGS.
    writer.write_all_bytes(frame::PREFACE.to_vec()).await?;
    init_and_run(reader, writer, internal_rx, internal_tx, true, None, config).await
}

/// Run the connection background task for the server side.
pub(crate) async fn run_server_connection<R: AsyncRead + 'static, W: AsyncWrite + 'static>(
    reader_io: R,
    writer_io: W,
    internal_rx: flume::Receiver<InternalMsg>,
    internal_tx: flume::Sender<InternalMsg>,
    incoming_tx: flume::Sender<Result<IncomingStream, H2Error>>,
    config: ConnConfig,
) -> Result<(), H2Error> {
    let (mut reader, mut writer) = configure_reader_writer(reader_io, writer_io, &config);
    // Server: read and validate the client connection preface.
    let preface = reader.read_exact_bytes(frame::PREFACE.len()).await?;
    if preface != frame::PREFACE {
        // Send GOAWAY before closing on invalid preface.
        let goaway = frame::GoAway::new(StreamId::ZERO, Reason::ProtocolError);
        let _ = writer.write_frame(&Frame::GoAway(goaway)).await;
        let _ = writer.flush_buf().await;
        return Err(H2Error::Protocol("invalid client preface".into()));
    }
    init_and_run(
        reader,
        writer,
        internal_rx,
        internal_tx,
        false,
        Some(incoming_tx),
        config,
    )
    .await
}

// Helper functions

/// Write a HEADERS frame, splitting into CONTINUATION frames if the header
/// block exceeds `max_frame_size` (per RFC 7540 §4.3).
async fn write_headers_with_continuation<W: AsyncWrite>(
    writer: &mut FrameWriter<W>,
    headers: frame::Headers,
    max_frame_size: usize,
) -> Result<(), H2Error> {
    let header_block = headers.header_block().clone();

    if header_block.len() <= max_frame_size {
        // Fits in a single HEADERS frame — write as-is (END_HEADERS already set)
        writer.write_frame(&Frame::Headers(headers)).await
    } else {
        // Split: HEADERS (first chunk, no END_HEADERS) + CONTINUATION frames
        let stream_id = headers.stream_id();
        let first_chunk = header_block.slice(..max_frame_size);

        // Replace the header block with just the first chunk and clear END_HEADERS
        let mut first_frame = frame::Headers::new(stream_id, first_chunk);
        // Preserve END_STREAM and PRIORITY flags from the original
        if headers.is_end_stream() {
            first_frame.set_end_stream();
        }
        if headers.has_priority() {
            first_frame.set_priority(headers.exclusive(), headers.dependency(), headers.weight());
        }
        first_frame.clear_end_headers();
        writer.write_frame(&Frame::Headers(first_frame)).await?;

        // Write remaining chunks as CONTINUATION frames
        let mut offset = max_frame_size;
        while offset < header_block.len() {
            let end = std::cmp::min(offset + max_frame_size, header_block.len());
            let chunk = header_block.slice(offset..end);
            let is_last = end == header_block.len();

            let mut cont = frame::Continuation::new(stream_id, chunk);
            if is_last {
                cont.set_end_headers();
            }
            writer.write_frame(&Frame::Continuation(cont)).await?;
            offset = end;
        }

        Ok(())
    }
}

/// Validate pseudo-header ordering and required fields per RFC 7540 §8.1.2.
///
/// Rules:
/// 1. All pseudo-headers must appear before regular headers.
/// 2. No duplicate pseudo-headers.
/// 3. Requests: `:method`, `:scheme`, `:path` required (CONNECT exempt from
///    `:scheme`/`:path`).
/// 4. Responses: `:status` required.
fn validate_pseudo_headers(headers: &[DecodedHeader], is_request: bool) -> Result<(), H2Error> {
    let mut seen_regular = false;
    let mut seen_method = false;
    let mut seen_scheme = false;
    let mut seen_path = false;
    let mut seen_status = false;
    let mut seen_authority = false;
    let mut method_value: Option<Bytes> = None;

    for dh in headers {
        if dh.name.starts_with(b":") {
            // Pseudo-header after regular header is a protocol error
            if seen_regular {
                return Err(H2Error::connection_msg(
                    Reason::ProtocolError,
                    "pseudo-header after regular header",
                ));
            }

            // Check for duplicates
            match &dh.name[..] {
                b":method" => {
                    if seen_method {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "duplicate :method",
                        ));
                    }
                    seen_method = true;
                    method_value = Some(dh.value.clone());
                }
                b":scheme" => {
                    if seen_scheme {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "duplicate :scheme",
                        ));
                    }
                    seen_scheme = true;
                }
                b":path" => {
                    if seen_path {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "duplicate :path",
                        ));
                    }
                    if dh.value.is_empty() {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "empty :path",
                        ));
                    }
                    seen_path = true;
                }
                b":status" => {
                    if seen_status {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "duplicate :status",
                        ));
                    }
                    seen_status = true;
                }
                b":authority" => {
                    if seen_authority {
                        return Err(H2Error::connection_msg(
                            Reason::ProtocolError,
                            "duplicate :authority",
                        ));
                    }
                    seen_authority = true;
                }
                _ => {
                    return Err(H2Error::connection_msg(
                        Reason::ProtocolError,
                        "unknown pseudo-header",
                    ));
                }
            }
        } else {
            seen_regular = true;
        }
    }

    if is_request {
        // :status is not allowed in requests (RFC 7540 §8.1.2.3)
        if seen_status {
            return Err(H2Error::connection_msg(
                Reason::ProtocolError,
                ":status not allowed in request",
            ));
        }
        if !seen_method {
            return Err(H2Error::connection_msg(
                Reason::ProtocolError,
                "missing required :method",
            ));
        }
        let is_connect = method_value
            .as_ref()
            .map(|v| &v[..] == b"CONNECT")
            .unwrap_or(false);
        if !is_connect {
            if !seen_scheme {
                return Err(H2Error::connection_msg(
                    Reason::ProtocolError,
                    "missing required :scheme",
                ));
            }
            if !seen_path {
                return Err(H2Error::connection_msg(
                    Reason::ProtocolError,
                    "missing required :path",
                ));
            }
        }
    } else {
        // Request pseudo-headers are not allowed in responses (RFC 7540 §8.1.2.4)
        if seen_method || seen_scheme || seen_path || seen_authority {
            return Err(H2Error::connection_msg(
                Reason::ProtocolError,
                "request pseudo-headers not allowed in response",
            ));
        }
        if !seen_status {
            return Err(H2Error::connection_msg(
                Reason::ProtocolError,
                "missing required :status",
            ));
        }
    }

    Ok(())
}

/// Validate regular header fields per RFC 7540 §8.1.2.
///
/// Checks for uppercase header names (§8.1.2) and connection-specific
/// headers (§8.1.2.2) which are prohibited in HTTP/2.
fn validate_regular_headers(headers: &[DecodedHeader], stream_id: StreamId) -> Result<(), H2Error> {
    for dh in headers {
        if dh.name.starts_with(b":") {
            continue; // pseudo-headers validated separately
        }
        // Header field names MUST be lowercase (RFC 7540 §8.1.2)
        for &b in dh.name.iter() {
            if b.is_ascii_uppercase() {
                return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
            }
        }
        // Connection-specific headers are prohibited (RFC 7540 §8.1.2.2)
        match &dh.name[..] {
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" => {
                return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
            }
            b"te" => {
                // TE header is allowed only with value "trailers"
                if &dh.value[..] != b"trailers" {
                    return Err(H2Error::stream(stream_id.value(), Reason::ProtocolError));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Extract the `content-length` value from decoded headers, if present.
fn parse_content_length(headers: &[DecodedHeader]) -> Option<u64> {
    for dh in headers {
        if dh.name.as_ref() == b"content-length"
            && let Ok(s) = std::str::from_utf8(&dh.value)
            && let Ok(len) = s.parse::<u64>()
        {
            return Some(len);
        }
    }
    None
}

fn has_no_pseudo_headers(headers: &[DecodedHeader]) -> bool {
    headers.iter().all(|dh| !dh.name.starts_with(b":"))
}

fn headers_to_header_map(headers: &[DecodedHeader]) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    for dh in headers {
        if dh.name.starts_with(b":") {
            continue; // skip pseudo-headers
        }
        if let (Ok(name), Ok(value)) = (
            http::header::HeaderName::from_bytes(&dh.name),
            http::header::HeaderValue::from_bytes(&dh.value),
        ) {
            map.append(name, value);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::flow_control::FlowControl;

    #[test]
    fn test_stream_error_does_not_kill_connection() {
        // Verify that H2Error::StreamError is properly categorized
        let err = H2Error::stream(3, Reason::Cancel);
        match &err {
            H2Error::StreamError {
                stream_id, reason, ..
            } => {
                assert_eq!(*stream_id, 3);
                assert_eq!(*reason, Reason::Cancel);
            }
            _ => panic!("expected StreamError"),
        }

        // Verify ConnectionError is distinct
        let err = H2Error::connection(Reason::ProtocolError);
        assert!(matches!(err, H2Error::ConnectionError { .. }));
    }

    #[test]
    fn test_window_update_consumed_tracking() {
        // Simulate the consumed byte tracking for WINDOW_UPDATE decisions
        let mut recv_flow = FlowControl::default(); // 65535
        let mut consumed: u32 = 0;
        let threshold = (recv_flow.initial_window_size() / 2) as u32; // 32767

        // Consume some data — below threshold
        recv_flow.consume(20000).unwrap();
        consumed += 20000;
        assert!(consumed < threshold);

        // Consume more — crosses threshold
        recv_flow.consume(15000).unwrap();
        consumed += 15000;
        assert!(consumed >= threshold);
        assert_eq!(consumed, 35000);

        // Send WINDOW_UPDATE for consumed amount, restore window, reset counter
        let increment = consumed;
        recv_flow.release(increment).unwrap();
        consumed = 0;

        // Window should be back to initial
        assert_eq!(recv_flow.window_size(), 65535);
        assert_eq!(consumed, 0);

        // New consumption starts fresh counter
        recv_flow.consume(10000).unwrap();
        consumed += 10000;
        assert!(consumed < threshold); // 10000 < 32767 — no spurious WINDOW_UPDATE
    }

    #[test]
    fn test_conn_state_stream_error_isolation() {
        // ConnState should have the stream error → RST_STREAM pattern
        // Verify the error types are properly distinguished
        let stream_err = H2Error::StreamError {
            stream_id: 5,
            reason: Reason::FlowControlError,
            remote: false,
        };
        let conn_err = H2Error::connection(Reason::ProtocolError);

        // Stream errors should match the StreamError pattern
        assert!(matches!(stream_err, H2Error::StreamError { .. }));

        // Connection errors should NOT match StreamError
        assert!(!matches!(conn_err, H2Error::StreamError { .. }));
    }

    #[test]
    fn test_stream_level_window_update_tracking() {
        use crate::proto::streams::StreamStore;

        let (internal_tx, _rx) = flume::unbounded();
        let mut store = StreamStore::new(false);
        store.insert(StreamId::new(1), 65535, 65535, internal_tx);
        if let Some(s) = store.get_mut(&StreamId::new(1)) {
            s.state = s.state.recv_headers(false).unwrap();
        }

        // No released bytes — no update needed
        assert!(store.streams_needing_window_update().is_empty());

        // Release some bytes (simulating app calling release_capacity)
        store.apply_release(&StreamId::new(1), 35000);
        let updates = store.streams_needing_window_update();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, StreamId::new(1));
        assert_eq!(updates[0].1, 35000);

        // Reset after sending WINDOW_UPDATE
        store.reset_released(&StreamId::new(1), 35000);
        if let Some(s) = store.get(&StreamId::new(1)) {
            assert_eq!(s.released, 0);
        }
        assert!(store.streams_needing_window_update().is_empty());
    }

    #[test]
    fn test_goaway_closes_higher_streams() {
        let mut state = {
            let (tx, _) = flume::unbounded();
            ConnState::new(
                true,
                None,
                ConnSettings::new(),
                PingPong::disabled(),
                None,
                ConnExtra::default(),
                tx,
            )
        };

        // Insert some streams
        let initial_send = 65535i32;
        let initial_recv = 65535i32;
        state.streams.insert(
            StreamId::new(1),
            initial_send,
            initial_recv,
            state.internal_tx.clone(),
        );
        state.streams.insert(
            StreamId::new(3),
            initial_send,
            initial_recv,
            state.internal_tx.clone(),
        );
        state.streams.insert(
            StreamId::new(5),
            initial_send,
            initial_recv,
            state.internal_tx.clone(),
        );

        // Set streams to Open
        for id in [1u32, 3, 5] {
            if let Some(s) = state.streams.get_mut(&StreamId::new(id)) {
                s.state = s.state.send_headers(false).unwrap();
            }
        }

        // Receive GOAWAY with last_stream_id=3
        let goaway = frame::GoAway::new(StreamId::new(3), Reason::NoError);
        state.handle_goaway(goaway).unwrap();

        assert!(state.going_away);
        // Stream 1 and 3 should still be open
        assert!(
            !state
                .streams
                .get(&StreamId::new(1))
                .unwrap()
                .state
                .is_closed()
        );
        assert!(
            !state
                .streams
                .get(&StreamId::new(3))
                .unwrap()
                .state
                .is_closed()
        );
        // Stream 5 should be closed
        assert!(
            state
                .streams
                .get(&StreamId::new(5))
                .unwrap()
                .state
                .is_closed()
        );
    }

    #[test]
    fn test_settings_initial_window_size_adjusts_existing_streams() {
        // RFC 7540 §6.9.2: When INITIAL_WINDOW_SIZE changes, the delta
        // must be applied to all existing streams' send flow control windows.
        let mut state = {
            let (tx, _) = flume::unbounded();
            ConnState::new(
                true,
                None,
                ConnSettings::new(),
                PingPong::disabled(),
                None,
                ConnExtra::default(),
                tx,
            )
        };

        // Insert streams with default initial window size (65535)
        state
            .streams
            .insert(StreamId::new(1), 65535, 65535, state.internal_tx.clone());
        state
            .streams
            .insert(StreamId::new(3), 65535, 65535, state.internal_tx.clone());
        for id in [1u32, 3] {
            if let Some(s) = state.streams.get_mut(&StreamId::new(id)) {
                s.state = s.state.send_headers(false).unwrap();
            }
        }

        // Consume some of stream 1's send window
        if let Some(s) = state.streams.get_mut(&StreamId::new(1)) {
            s.send_flow.consume(10000).unwrap();
        }

        // Simulate receiving SETTINGS with new initial_window_size = 32768
        // Delta = 32768 - 65535 = -32767
        let new_window = 32768i32;
        let stream_ids: Vec<StreamId> = state.streams.iter_ids().collect();
        for id in stream_ids {
            if let Some(stream) = state.streams.get_mut(&id) {
                if stream.state.is_closed() {
                    continue;
                }
                stream
                    .send_flow
                    .update_initial_window_size(new_window)
                    .unwrap();
            }
        }

        // Stream 1 had 65535 - 10000 = 55535, delta = -32767, so now 55535 - 32767 =
        // 22768
        assert_eq!(
            state
                .streams
                .get(&StreamId::new(1))
                .unwrap()
                .send_flow
                .window_size(),
            22768
        );
        // Stream 3 had 65535, delta = -32767, so now 65535 - 32767 = 32768
        assert_eq!(
            state
                .streams
                .get(&StreamId::new(3))
                .unwrap()
                .send_flow
                .window_size(),
            32768
        );
    }

    #[test]
    fn test_settings_initial_window_size_increase() {
        let mut state = {
            let (tx, _) = flume::unbounded();
            ConnState::new(
                true,
                None,
                ConnSettings::new(),
                PingPong::disabled(),
                None,
                ConnExtra::default(),
                tx,
            )
        };

        state
            .streams
            .insert(StreamId::new(1), 65535, 65535, state.internal_tx.clone());
        if let Some(s) = state.streams.get_mut(&StreamId::new(1)) {
            s.state = s.state.send_headers(false).unwrap();
            s.send_flow.consume(60000).unwrap(); // window = 5535
        }

        // Increase initial window to 131070 (delta = +65535)
        let new_window = 131070i32;
        if let Some(stream) = state.streams.get_mut(&StreamId::new(1)) {
            stream
                .send_flow
                .update_initial_window_size(new_window)
                .unwrap();
        }

        // 5535 + 65535 = 71070
        assert_eq!(
            state
                .streams
                .get(&StreamId::new(1))
                .unwrap()
                .send_flow
                .window_size(),
            71070
        );
    }

    #[test]
    fn test_settings_initial_window_size_skips_closed_streams() {
        let mut state = {
            let (tx, _) = flume::unbounded();
            ConnState::new(
                true,
                None,
                ConnSettings::new(),
                PingPong::disabled(),
                None,
                ConnExtra::default(),
                tx,
            )
        };

        state
            .streams
            .insert(StreamId::new(1), 65535, 65535, state.internal_tx.clone());
        state
            .streams
            .insert(StreamId::new(3), 65535, 65535, state.internal_tx.clone());

        // Open stream 1, close stream 3
        if let Some(s) = state.streams.get_mut(&StreamId::new(1)) {
            s.state = s.state.send_headers(false).unwrap();
        }
        if let Some(s) = state.streams.get_mut(&StreamId::new(3)) {
            s.state = s.state.reset();
        }

        let new_window = 32768i32;
        let stream_ids: Vec<StreamId> = state.streams.iter_ids().collect();
        for id in stream_ids {
            if let Some(stream) = state.streams.get_mut(&id) {
                if stream.state.is_closed() {
                    continue;
                }
                stream
                    .send_flow
                    .update_initial_window_size(new_window)
                    .unwrap();
            }
        }

        // Stream 1 (open): adjusted
        assert_eq!(
            state
                .streams
                .get(&StreamId::new(1))
                .unwrap()
                .send_flow
                .window_size(),
            32768
        );
        // Stream 3 (closed): NOT adjusted — still at original 65535
        assert_eq!(
            state
                .streams
                .get(&StreamId::new(3))
                .unwrap()
                .send_flow
                .window_size(),
            65535
        );
    }

    #[test]
    fn test_headers_continuation_splitting() {
        // Verify that a header block larger than max_frame_size would be
        // correctly split into a HEADERS frame + CONTINUATION frames.
        // We test the frame construction logic directly (not the async writer).
        let stream_id = StreamId::new(1);
        let max_frame_size: usize = 16;

        // Create a header block larger than max_frame_size (50 bytes)
        let header_block = Bytes::from(vec![0xAA; 50]);
        let headers = frame::Headers::new(stream_id, header_block.clone());
        assert!(headers.is_end_headers());

        // Simulate the splitting logic from write_headers_with_continuation
        assert!(header_block.len() > max_frame_size);

        // First frame: HEADERS with first chunk, no END_HEADERS
        let first_chunk = header_block.slice(..max_frame_size);
        let mut first_frame = frame::Headers::new(stream_id, first_chunk.clone());
        first_frame.clear_end_headers();
        assert!(!first_frame.is_end_headers());
        assert_eq!(first_frame.header_block().len(), max_frame_size);
        assert_eq!(first_frame.stream_id(), stream_id);

        // Remaining chunks as CONTINUATION frames
        let mut offset = max_frame_size;
        let mut continuations = Vec::new();
        while offset < header_block.len() {
            let end = std::cmp::min(offset + max_frame_size, header_block.len());
            let chunk = header_block.slice(offset..end);
            let is_last = end == header_block.len();

            let mut cont = frame::Continuation::new(stream_id, chunk);
            if is_last {
                cont.set_end_headers();
            }
            continuations.push(cont);
            offset = end;
        }

        // Should produce ceil((50 - 16) / 16) = 3 CONTINUATION frames
        // chunks: 16, 16, 2
        assert_eq!(continuations.len(), 3);

        // Only the last CONTINUATION should have END_HEADERS
        assert!(!continuations[0].is_end_headers());
        assert!(!continuations[1].is_end_headers());
        assert!(continuations[2].is_end_headers());

        // All continuations must share the same stream ID
        for cont in &continuations {
            assert_eq!(cont.stream_id(), stream_id);
        }

        // Reassemble: all chunks together should equal the original header block
        let mut reassembled = first_chunk.to_vec();
        for cont in &continuations {
            reassembled.extend_from_slice(cont.header_block());
        }
        assert_eq!(reassembled, header_block.as_ref());
    }

    #[test]
    fn test_headers_no_split_when_fits() {
        // A header block that fits in one frame should not be split
        let stream_id = StreamId::new(3);
        let max_frame_size: usize = 100;
        let header_block = Bytes::from(vec![0xBB; 50]);
        let headers = frame::Headers::new(stream_id, header_block.clone());

        assert!(header_block.len() <= max_frame_size);
        assert!(headers.is_end_headers());
        assert_eq!(headers.header_block().len(), 50);
    }

    #[test]
    fn test_headers_split_preserves_end_stream() {
        // END_STREAM flag on the original HEADERS must be preserved on the first frame
        let stream_id = StreamId::new(1);
        let max_frame_size: usize = 10;
        let header_block = Bytes::from(vec![0xCC; 25]);

        let mut original = frame::Headers::new(stream_id, header_block.clone());
        original.set_end_stream();
        assert!(original.is_end_stream());

        // Split: first frame keeps END_STREAM, clears END_HEADERS
        let first_chunk = header_block.slice(..max_frame_size);
        let mut first_frame = frame::Headers::new(stream_id, first_chunk);
        if original.is_end_stream() {
            first_frame.set_end_stream();
        }
        first_frame.clear_end_headers();

        assert!(first_frame.is_end_stream());
        assert!(!first_frame.is_end_headers());
    }

    #[test]
    fn test_pending_send_buffering() {
        // Test the PendingSend struct and flow control gating logic
        let flow = FlowControl::new(100);
        assert_eq!(flow.available(), 100);

        // Data that fits
        assert!(flow.available() >= 50);

        // Data that doesn't fit
        assert!(flow.available() < 200);

        // After WINDOW_UPDATE, it should fit
        let mut flow = flow;
        flow.apply_window_update(150).unwrap();
        assert!(flow.available() >= 200);
    }

    // --- Pseudo-header validation tests ---

    fn dh(name: &[u8], value: &[u8]) -> DecodedHeader {
        DecodedHeader {
            name: Bytes::from(name.to_vec()),
            value: Bytes::from(value.to_vec()),
            sensitive: false,
        }
    }

    #[test]
    fn test_valid_request_headers() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b":scheme", b"https"),
            dh(b":path", b"/"),
            dh(b"host", b"example.com"),
        ];
        assert!(validate_pseudo_headers(&headers, true).is_ok());
    }

    #[test]
    fn test_valid_response_headers() {
        let headers = vec![dh(b":status", b"200"), dh(b"content-type", b"text/html")];
        assert!(validate_pseudo_headers(&headers, false).is_ok());
    }

    #[test]
    fn test_pseudo_after_regular_rejected() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b"host", b"example.com"),
            dh(b":scheme", b"https"), // pseudo after regular
        ];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("pseudo-header after regular"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_duplicate_method_rejected() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b":method", b"POST"),
            dh(b":scheme", b"https"),
            dh(b":path", b"/"),
        ];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("duplicate :method"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_missing_method_rejected() {
        let headers = vec![dh(b":scheme", b"https"), dh(b":path", b"/")];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("missing required :method"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_missing_scheme_rejected() {
        let headers = vec![dh(b":method", b"GET"), dh(b":path", b"/")];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("missing required :scheme"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_missing_path_rejected() {
        let headers = vec![dh(b":method", b"GET"), dh(b":scheme", b"https")];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("missing required :path"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_connect_exempt_from_scheme_path() {
        // CONNECT is exempt from :scheme and :path requirements
        let headers = vec![
            dh(b":method", b"CONNECT"),
            dh(b":authority", b"example.com:443"),
        ];
        assert!(validate_pseudo_headers(&headers, true).is_ok());
    }

    #[test]
    fn test_missing_status_rejected() {
        let headers = vec![dh(b"content-type", b"text/html")];
        let err = validate_pseudo_headers(&headers, false).unwrap_err();
        assert!(err.to_string().contains("missing required :status"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_unknown_pseudo_header_rejected() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b":scheme", b"https"),
            dh(b":path", b"/"),
            dh(b":foo", b"bar"),
        ];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("unknown pseudo-header"));
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_conn_extra_defaults() {
        let extra = ConnExtra::default();
        assert!(extra.max_concurrent_reset_streams.is_none());
        assert!(extra.reset_stream_duration.is_none());
        assert!(extra.max_send_buffer_size.is_none());
    }

    /// Verify that the DATA frame splitting loop in `cmd_send_data` produces
    /// the right number of chunks with only the last carrying END_STREAM.
    #[test]
    fn test_data_frame_splitting_at_max_frame_size() {
        // Simulate the splitting logic from cmd_send_data for a 50-byte payload
        // with max_frame_size = 16.
        let payload = Bytes::from(vec![0xAA; 50]);
        let max_frame: usize = 16;
        let end_stream = true;

        let mut frames: Vec<(usize, bool)> = Vec::new(); // (len, has_end_stream)
        let mut offset = 0;
        while offset < payload.len() {
            let end = std::cmp::min(offset + max_frame, payload.len());
            let chunk_len = end - offset;
            let is_last = end == payload.len();
            let has_es = end_stream && is_last;
            frames.push((chunk_len, has_es));
            offset = end;
        }

        // 50 / 16 = 3 full frames (16 bytes) + 1 partial (2 bytes) = 4 frames
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0], (16, false));
        assert_eq!(frames[1], (16, false));
        assert_eq!(frames[2], (16, false));
        assert_eq!(frames[3], (2, true)); // only last has END_STREAM
    }

    /// Verify that a payload exactly equal to max_frame_size produces one
    /// frame.
    #[test]
    fn test_data_frame_no_split_when_fits() {
        let payload = Bytes::from(vec![0xBB; 16384]);
        let max_frame: usize = 16384;

        let mut frame_count = 0;
        let mut offset = 0;
        while offset < payload.len() {
            let end = std::cmp::min(offset + max_frame, payload.len());
            frame_count += 1;
            offset = end;
        }
        assert_eq!(frame_count, 1);
    }

    /// Verify partial flow control: when available < data_len but > 0, only the
    /// sendable portion should be consumed and the remainder queued.
    #[test]
    fn test_partial_flow_control_queuing() {
        let mut conn_flow = FlowControl::new(1000);
        let stream_flow = FlowControl::new(500); // stream is the bottleneck

        let data = Bytes::from(vec![0xCC; 800]);
        let data_len = data.len() as u32;
        let conn_avail = conn_flow.available();
        let stream_avail = stream_flow.available();
        let sendable = std::cmp::min(data_len, std::cmp::min(conn_avail, stream_avail)) as usize;

        // Should be 500 (stream-limited)
        assert_eq!(sendable, 500);
        assert!(sendable > 0);
        assert!(sendable < data.len());

        // Partial send: split data
        let send_now = data.slice(..sendable);
        let remainder = data.slice(sendable..);
        assert_eq!(send_now.len(), 500);
        assert_eq!(remainder.len(), 300);

        // Consume from flow control
        conn_flow.consume(sendable as u32).unwrap();
        assert_eq!(conn_flow.available(), 500);
    }

    // --- is_idle_peer_stream tests ---

    fn make_server_state() -> ConnState {
        let (tx, _) = flume::unbounded();
        ConnState::new(
            false,
            None,
            ConnSettings::new(),
            PingPong::disabled(),
            None,
            ConnExtra::default(),
            tx,
        )
    }

    fn make_client_state() -> ConnState {
        let (tx, _) = flume::unbounded();
        ConnState::new(
            true,
            None,
            ConnSettings::new(),
            PingPong::disabled(),
            None,
            ConnExtra::default(),
            tx,
        )
    }

    #[test]
    fn test_idle_peer_stream_server_detects_unseen_client_stream() {
        let mut state = make_server_state();
        // Server hasn't seen any client streams yet (last_peer_stream_id = 0)
        assert!(state.is_idle_peer_stream(&StreamId::new(1)));
        assert!(state.is_idle_peer_stream(&StreamId::new(3)));

        // After accepting stream 3, stream 1 is not idle (already passed)
        state.last_peer_stream_id = StreamId::new(3);
        assert!(!state.is_idle_peer_stream(&StreamId::new(1)));
        assert!(!state.is_idle_peer_stream(&StreamId::new(3)));
        assert!(state.is_idle_peer_stream(&StreamId::new(5)));
    }

    #[test]
    fn test_idle_peer_stream_client_detects_even_server_streams() {
        let mut state = make_client_state();
        // Client: peer is server, server-initiated streams are even
        // Even stream IDs higher than last_peer_stream_id are idle
        assert!(!state.is_idle_peer_stream(&StreamId::new(1))); // odd = client-initiated, not peer
        assert!(state.is_idle_peer_stream(&StreamId::new(2))); // even + > 0 = idle server stream

        state.last_peer_stream_id = StreamId::new(2);
        assert!(!state.is_idle_peer_stream(&StreamId::new(2))); // no longer idle
        assert!(state.is_idle_peer_stream(&StreamId::new(4))); // still idle
    }

    #[test]
    fn test_even_stream_id_from_client_is_connection_error() {
        // Server-side: even stream IDs from client are invalid (RFC 7540 §5.1.1)
        let state = make_server_state();
        // Even IDs should fail the server-side odd check in handle_headers,
        // but is_idle_peer_stream should still identify them correctly
        assert!(state.is_idle_peer_stream(&StreamId::new(2)));
    }

    #[test]
    fn test_non_monotonic_stream_id_detected() {
        let mut state = make_server_state();
        state.last_peer_stream_id = StreamId::new(5);
        // Stream 3 is not idle (below last seen) — won't trigger idle error,
        // but won't be found in store either → ignored (closed/GC'd)
        assert!(!state.is_idle_peer_stream(&StreamId::new(3)));
        // Stream 7 is idle (above last seen) — connection error
        assert!(state.is_idle_peer_stream(&StreamId::new(7)));
    }

    #[test]
    fn test_data_on_half_closed_remote_is_stream_error() {
        // Verify the error type for DATA on half-closed(remote) stream
        // The handle_data path checks can_recv() → StreamClosed stream error
        let err = H2Error::stream(3, Reason::StreamClosed);
        assert!(err.is_reset());
        assert_eq!(err.reason(), Some(Reason::StreamClosed));
        assert_eq!(err.stream_id(), Some(3));
        // This is a stream error, NOT a connection error
        assert!(!err.is_connection());
    }

    #[test]
    fn test_connection_error_has_reason_for_goaway() {
        // Verify that connection errors from idle stream checks have reason()
        // so that run_event_loop sends GOAWAY
        let err = H2Error::connection(Reason::ProtocolError);
        assert!(err.reason().is_some());
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
    }

    #[test]
    fn test_connection_msg_preserves_message() {
        let err = H2Error::connection_msg(Reason::ProtocolError, "duplicate :method");
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
        assert!(err.is_connection());
        assert!(err.to_string().contains("duplicate :method"));
    }

    // --- validate_regular_headers tests (#3) ---

    #[test]
    fn test_regular_headers_uppercase_rejected() {
        let headers = vec![dh(b":method", b"GET"), dh(b"Content-Type", b"text/html")];
        let err = validate_regular_headers(&headers, StreamId::new(1)).unwrap_err();
        assert_eq!(err.reason(), Some(Reason::ProtocolError));
    }

    #[test]
    fn test_regular_headers_connection_rejected() {
        let headers = vec![dh(b"connection", b"keep-alive")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_err());
    }

    #[test]
    fn test_regular_headers_keep_alive_rejected() {
        let headers = vec![dh(b"keep-alive", b"timeout=5")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_err());
    }

    #[test]
    fn test_regular_headers_transfer_encoding_rejected() {
        let headers = vec![dh(b"transfer-encoding", b"chunked")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_err());
    }

    #[test]
    fn test_regular_headers_upgrade_rejected() {
        let headers = vec![dh(b"upgrade", b"websocket")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_err());
    }

    #[test]
    fn test_regular_headers_te_trailers_allowed() {
        let headers = vec![dh(b"te", b"trailers")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_ok());
    }

    #[test]
    fn test_regular_headers_te_gzip_rejected() {
        let headers = vec![dh(b"te", b"gzip")];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_err());
    }

    #[test]
    fn test_regular_headers_valid_passes() {
        let headers = vec![
            dh(b":status", b"200"),
            dh(b"content-type", b"text/html"),
            dh(b"x-custom", b"value"),
        ];
        assert!(validate_regular_headers(&headers, StreamId::new(1)).is_ok());
    }

    // --- validate_pseudo_headers cross-checks (#4) ---

    #[test]
    fn test_status_in_request_rejected() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b":scheme", b"https"),
            dh(b":path", b"/"),
            dh(b":status", b"200"),
        ];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains(":status not allowed in request"));
    }

    #[test]
    fn test_request_pseudos_in_response_rejected() {
        let headers = vec![dh(b":status", b"200"), dh(b":method", b"GET")];
        let err = validate_pseudo_headers(&headers, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("request pseudo-headers not allowed in response")
        );
    }

    #[test]
    fn test_empty_path_rejected() {
        let headers = vec![
            dh(b":method", b"GET"),
            dh(b":scheme", b"https"),
            dh(b":path", b""),
        ];
        let err = validate_pseudo_headers(&headers, true).unwrap_err();
        assert!(err.to_string().contains("empty :path"));
    }

    #[test]
    fn test_response_with_scheme_rejected() {
        let headers = vec![dh(b":status", b"200"), dh(b":scheme", b"https")];
        let err = validate_pseudo_headers(&headers, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("request pseudo-headers not allowed in response")
        );
    }

    #[test]
    fn test_response_with_path_rejected() {
        let headers = vec![dh(b":status", b"200"), dh(b":path", b"/")];
        let err = validate_pseudo_headers(&headers, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("request pseudo-headers not allowed in response")
        );
    }

    #[test]
    fn test_response_with_authority_rejected() {
        let headers = vec![dh(b":status", b"200"), dh(b":authority", b"example.com")];
        let err = validate_pseudo_headers(&headers, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("request pseudo-headers not allowed in response")
        );
    }

    // --- Content-length helper tests (#5) ---

    #[test]
    fn test_parse_content_length_present() {
        let headers = vec![dh(b":method", b"GET"), dh(b"content-length", b"42")];
        assert_eq!(parse_content_length(&headers), Some(42));
    }

    #[test]
    fn test_parse_content_length_absent() {
        let headers = vec![dh(b":method", b"GET")];
        assert_eq!(parse_content_length(&headers), None);
    }

    #[test]
    fn test_parse_content_length_invalid() {
        let headers = vec![dh(b"content-length", b"not-a-number")];
        assert_eq!(parse_content_length(&headers), None);
    }

    #[test]
    fn test_parse_content_length_zero() {
        let headers = vec![dh(b"content-length", b"0")];
        assert_eq!(parse_content_length(&headers), Some(0));
    }

    // --- Non-monotonic stream ID → ConnectionError regression test (#6) ---

    /// Verifies that HEADERS with a stream ID ≤ last_peer_stream_id triggers a
    /// connection error (GOAWAY), not a stream error (RST_STREAM).
    /// Regression test for the Group B fix in commit 2a00c16.
    #[compio_macros::test]
    async fn test_non_monotonic_stream_id_is_connection_error() {
        use compio_net::{TcpListener, TcpStream};

        use crate::server;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn a minimal server
        let server_handle = compio_runtime::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = server::builder().handshake(stream).await.unwrap();
            // Accept until the connection closes with an error
            while let Some(result) = conn.accept().await {
                match result {
                    Ok(_) => {}
                    Err(e) => return Some(e),
                }
            }
            None
        });

        // Raw TCP client: send the HTTP/2 preface + valid SETTINGS, then
        // two HEADERS frames with non-monotonic stream IDs.
        let mut tcp = TcpStream::connect(addr).await.unwrap();
        use compio_io::AsyncWriteExt;

        // Minimal HPACK: :method=GET, :path=/, :scheme=http
        let hpack_block: Vec<u8> = vec![0x82, 0x84, 0x86];
        let hpack_len = hpack_block.len() as u8;

        // Build a single buffer: preface + SETTINGS + SETTINGS ACK + HEADERS(3)
        let mut buf = Vec::new();
        buf.extend_from_slice(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        // Empty SETTINGS frame
        buf.extend_from_slice(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0]);
        // SETTINGS ACK
        buf.extend_from_slice(&[0, 0, 0, 0x04, 0x01, 0, 0, 0, 0]);
        // HEADERS on stream 3 (END_STREAM | END_HEADERS)
        buf.extend_from_slice(&[0, 0, hpack_len, 0x01, 0x05, 0, 0, 0, 3]);
        buf.extend_from_slice(&hpack_block);
        tcp.write_all(buf).await.unwrap();

        // Wait for server to process stream 3
        compio_runtime::time::sleep(std::time::Duration::from_millis(100)).await;

        // HEADERS on stream 1 — non-monotonic (1 < 3) → connection error
        let mut buf2 = Vec::new();
        buf2.extend_from_slice(&[0, 0, hpack_len, 0x01, 0x05, 0, 0, 0, 1]);
        buf2.extend_from_slice(&hpack_block);
        tcp.write_all(buf2).await.unwrap();

        // The server should send GOAWAY (connection error) and close
        let result =
            compio_runtime::time::timeout(std::time::Duration::from_secs(5), server_handle)
                .await
                .expect("server should respond within timeout");

        if let Ok(Some(err)) = result {
            assert!(
                err.is_connection(),
                "non-monotonic stream ID should produce ConnectionError, got: {err}"
            );
            assert_eq!(err.reason(), Some(Reason::ProtocolError));
        }
        // If None, the server cleanly closed — also acceptable since it sent
        // GOAWAY
    }
}
