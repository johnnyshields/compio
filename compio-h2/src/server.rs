use bytes::Bytes;
use compio_io::{AsyncRead, AsyncWrite, util::Splittable};

use crate::{
    error::{H2Error, Reason},
    frame::StreamId,
    proto::{
        connection::{Command, CommandSender, ConnExtra, IncomingStream, InternalMsg},
        ping_pong::PingPong,
        settings::ConnSettings,
    },
    share::{RecvStream, SendStream},
};

/// Create a new server builder for configuring HTTP/2 connection settings.
pub fn builder() -> crate::builder::ServerBuilder {
    crate::builder::ServerBuilder::new()
}

/// Perform HTTP/2 server handshake with default settings.
///
/// This operation is *not* cancel-safe.
///
/// Returns a `ServerConnection` that can accept incoming streams.
pub async fn handshake<IO>(io: IO) -> Result<ServerConnection, H2Error>
where
    IO: Splittable + 'static,
    IO::ReadHalf: AsyncRead + 'static,
    IO::WriteHalf: AsyncWrite + 'static,
{
    handshake_with_settings(
        io,
        ConnSettings::new(),
        PingPong::disabled(),
        None,
        ConnExtra::default(),
    )
    .await
}

/// Perform HTTP/2 server handshake with explicit settings and keepalive.
///
/// This operation is *not* cancel-safe.
pub async fn handshake_with_settings<IO>(
    io: IO,
    settings: ConnSettings,
    ping_pong: PingPong,
    initial_connection_window_size: Option<u32>,
    extra: ConnExtra,
) -> Result<ServerConnection, H2Error>
where
    IO: Splittable + 'static,
    IO::ReadHalf: AsyncRead + 'static,
    IO::WriteHalf: AsyncWrite + 'static,
{
    let (read_half, write_half) = io.split();
    let (internal_tx, internal_rx) = flume::unbounded::<InternalMsg>();
    let (incoming_tx, incoming_rx) = flume::unbounded();
    let (closed_tx, closed_rx) = flume::bounded::<Result<(), H2Error>>(1);

    let cmd_sender = CommandSender::new(internal_tx.clone());

    // Spawn the server connection background task
    let itx = internal_tx.clone();
    compio_runtime::spawn(async move {
        let result = crate::proto::connection::run_server_connection(
            read_half,
            write_half,
            internal_rx,
            itx,
            incoming_tx,
            crate::proto::connection::ConnConfig {
                settings,
                ping_pong,
                initial_connection_window_size,
                extra,
            },
        )
        .await;
        if let Err(ref _e) = result {
            compio_log::error!("server connection error: {}", _e);
        }
        // Notify poll_closed waiters
        let _ = closed_tx.send(result);
    })
    .detach();

    Ok(ServerConnection {
        incoming_rx,
        cmd_tx: cmd_sender,
        closed_rx,
    })
}

/// Server-side HTTP/2 connection handle.
///
/// # Cancellation
///
/// Unless noted otherwise, methods on this type are *not* cancel-safe:
/// dropping a future after the command has been dispatched may leave the
/// operation completed on the wire without the caller observing the result.
pub struct ServerConnection {
    incoming_rx: flume::Receiver<Result<IncomingStream, H2Error>>,
    cmd_tx: CommandSender,
    closed_rx: flume::Receiver<Result<(), H2Error>>,
}

impl ServerConnection {
    /// Initiate a graceful shutdown by sending a GOAWAY frame.
    ///
    /// After calling this, no new streams will be accepted. Existing streams
    /// will be allowed to complete, and the connection will close once all
    /// active streams are finished.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn shutdown(&self) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::GoAway { response_tx: tx })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))?
    }

    /// Initiate an abrupt shutdown by sending a GOAWAY frame with an error
    /// reason.
    ///
    /// Unlike [`shutdown`](Self::shutdown), this immediately closes all open
    /// streams with errors and terminates the connection without waiting for
    /// in-flight streams to complete.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn abrupt_shutdown(&self, reason: Reason) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::AbruptShutdown {
                reason,
                response_tx: tx,
            })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))?
    }

    /// Wait for the connection background task to complete.
    ///
    /// This operation is cancel-safe.
    ///
    /// Returns `Ok(())` if the connection closed cleanly, or the error that
    /// caused it to terminate.
    pub async fn closed(&mut self) -> Result<(), H2Error> {
        self.closed_rx
            .recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection task dropped".into()))?
    }

    /// Set the target connection-level receive window size at runtime.
    ///
    /// If `size` is larger than the current connection receive window, a
    /// WINDOW_UPDATE frame is sent immediately for the difference. If smaller,
    /// the window shrinks naturally as data arrives.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn set_target_window_size(&self, size: u32) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SetTargetWindowSize {
                size,
                response_tx: tx,
            })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))?
    }

    /// Set the initial stream-level window size via a SETTINGS frame.
    ///
    /// This changes the INITIAL_WINDOW_SIZE for newly created streams and
    /// adjusts the receive windows of all existing open streams by the delta
    /// between the old and new values (per RFC 7540 §6.9.2).
    ///
    /// This operation is *not* cancel-safe.
    pub async fn set_initial_window_size(&self, size: u32) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SetInitialWindowSize {
                size,
                response_tx: tx,
            })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))?
    }

    /// Accept the next incoming request stream.
    ///
    /// This operation is cancel-safe.
    ///
    /// Returns `None` when the connection is closed.
    pub async fn accept(
        &mut self,
    ) -> Option<Result<(http::Request<RecvStream>, SendResponse), H2Error>> {
        match self.incoming_rx.recv_async().await {
            Ok(Ok(incoming)) => {
                let result = self.build_request(incoming);
                Some(result)
            }
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => None, // connection closed
        }
    }

    fn build_request(
        &self,
        incoming: IncomingStream,
    ) -> Result<(http::Request<RecvStream>, SendResponse), H2Error> {
        let recv_stream = RecvStream::new(
            incoming.stream_id,
            incoming.recv.data_rx,
            incoming.recv.trailers_rx,
            incoming.recv.internal_tx,
        );

        // Extract pseudo-headers for the request
        let mut method = None;
        let mut scheme = None;
        let mut path = None;
        let mut authority = None;
        let mut regular_headers = http::HeaderMap::new();

        for dh in &incoming.headers {
            if &dh.name[..] == b":method" {
                method = Some(
                    http::Method::from_bytes(&dh.value)
                        .map_err(|e| H2Error::Protocol(format!("invalid method: {}", e)))?,
                );
            } else if &dh.name[..] == b":scheme" {
                scheme = Some(String::from_utf8_lossy(&dh.value).to_string());
            } else if &dh.name[..] == b":path" {
                path = Some(String::from_utf8_lossy(&dh.value).to_string());
            } else if &dh.name[..] == b":authority" {
                authority = Some(String::from_utf8_lossy(&dh.value).to_string());
            } else {
                if let (Ok(hname), Ok(hvalue)) = (
                    http::header::HeaderName::from_bytes(&dh.name),
                    http::header::HeaderValue::from_bytes(&dh.value),
                ) {
                    regular_headers.append(hname, hvalue);
                }
            }
        }

        let method = method.unwrap_or(http::Method::GET);
        let path = path.unwrap_or_else(|| "/".to_string());

        // Build URI
        let uri_str = if let Some(authority) = &authority {
            let s = scheme.as_deref().unwrap_or("https");
            format!("{}://{}{}", s, authority, path)
        } else {
            path
        };

        let uri: http::Uri = uri_str
            .parse()
            .map_err(|e| H2Error::Protocol(format!("invalid URI: {}", e)))?;

        let mut request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(recv_stream)
            .map_err(|e| H2Error::Protocol(format!("failed to build request: {}", e)))?;

        *request.headers_mut() = regular_headers;

        let send_response = SendResponse {
            stream_id: incoming.stream_id,
            cmd_tx: self.cmd_tx.clone(),
            reset_rx: incoming.recv.reset_rx,
        };

        Ok((request, send_response))
    }
}

/// Handle for sending a response on a server stream.
///
/// If dropped without calling [`send_response`](Self::send_response), no
/// response headers are sent. The peer will eventually observe a timeout or
/// connection close.
///
/// # Cancellation
///
/// Unless noted otherwise, methods on this type are *not* cancel-safe.
pub struct SendResponse {
    stream_id: StreamId,
    cmd_tx: CommandSender,
    reset_rx: flume::Receiver<Reason>,
}

impl SendResponse {
    /// Send the response headers.
    ///
    /// This operation is *not* cancel-safe.
    ///
    /// Returns a `SendStream` if `end_of_stream` is false (for sending response
    /// body).
    pub async fn send_response(
        &mut self,
        response: http::Response<()>,
        end_of_stream: bool,
    ) -> Result<Option<SendStream>, H2Error> {
        // Build response headers
        let mut headers = Vec::new();
        headers.push((
            Bytes::from_static(b":status"),
            Bytes::copy_from_slice(response.status().as_str().as_bytes()),
        ));

        for (name, value) in response.headers() {
            headers.push((
                Bytes::copy_from_slice(name.as_str().as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            ));
        }

        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SendHeaders {
                stream_id: self.stream_id,
                headers,
                end_stream: end_of_stream,
                response_tx: tx,
            })
            .await?;

        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))??;

        if end_of_stream {
            Ok(None)
        } else {
            Ok(Some(SendStream::new(
                self.stream_id,
                self.cmd_tx.clone(),
                self.reset_rx.clone(),
            )))
        }
    }

    /// Wait for the peer to send a RST_STREAM on this stream.
    ///
    /// This operation is cancel-safe.
    ///
    /// The reason code from the RST_STREAM frame. This is useful for
    /// detecting early cancellation by the peer while preparing or sending a
    /// response.
    ///
    /// If the stream closes normally (without RST_STREAM) or the connection
    /// is terminated, this returns an error.
    pub async fn poll_reset(&mut self) -> Result<Reason, H2Error> {
        self.reset_rx
            .recv_async()
            .await
            .map_err(|_| H2Error::Protocol("stream closed without reset".into()))
    }
}
