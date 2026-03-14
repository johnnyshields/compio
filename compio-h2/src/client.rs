use bytes::Bytes;
use compio_io::{AsyncRead, AsyncWrite, util::Splittable};

use crate::{
    error::H2Error,
    frame::StreamId,
    proto::{
        connection::{Command, CommandSender, InternalMsg},
        ping_pong::PingPong,
        settings::ConnSettings,
        streams::StreamRecv,
    },
    share::{RecvStream, SendStream},
};

/// Create a new client builder for configuring HTTP/2 connection settings.
pub fn builder() -> crate::builder::ClientBuilder {
    crate::builder::ClientBuilder::new()
}

/// Perform HTTP/2 client handshake with default settings.
///
/// The connection handle must be spawned as a background task for the client to
/// function:
///
/// ```no_run
/// # compio_runtime::Runtime::new().unwrap().block_on(async {
/// use compio_h2::client;
/// use compio_net::TcpStream;
///
/// let tcp = TcpStream::connect("127.0.0.1:8080").await.unwrap();
/// let (send_request, connection) = client::handshake(tcp).await.unwrap();
/// compio_runtime::spawn(connection.run()).detach();
/// # });
/// ```
///
/// This operation is *not* cancel-safe.
pub async fn handshake<IO>(
    io: IO,
) -> Result<(SendRequest, ClientConnection<IO::ReadHalf, IO::WriteHalf>), H2Error>
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
        crate::proto::connection::ConnExtra::default(),
    )
    .await
}

/// Perform HTTP/2 client handshake with explicit settings and keepalive.
///
/// This operation is *not* cancel-safe.
pub async fn handshake_with_settings<IO>(
    io: IO,
    settings: ConnSettings,
    ping_pong: PingPong,
    initial_connection_window_size: Option<u32>,
    extra: crate::proto::connection::ConnExtra,
) -> Result<(SendRequest, ClientConnection<IO::ReadHalf, IO::WriteHalf>), H2Error>
where
    IO: Splittable + 'static,
    IO::ReadHalf: AsyncRead + 'static,
    IO::WriteHalf: AsyncWrite + 'static,
{
    let (read_half, write_half) = io.split();
    let (internal_tx, internal_rx) = flume::unbounded::<InternalMsg>();

    let cmd_sender = CommandSender::new(internal_tx.clone());

    let conn = ClientConnection {
        internal_rx,
        internal_tx,
        read_half,
        write_half,
        settings,
        ping_pong,
        initial_connection_window_size,
        extra,
    };

    let send_request = SendRequest { cmd_tx: cmd_sender };

    Ok((send_request, conn))
}

/// Handle for sending requests on a client connection.
///
/// # Cancellation
///
/// Unless noted otherwise, methods on this type are *not* cancel-safe:
/// dropping a future after the command has been dispatched may leave the
/// operation completed on the wire without the caller observing the result.
#[derive(Clone)]
pub struct SendRequest {
    cmd_tx: CommandSender,
}

impl SendRequest {
    /// Wait until the connection can accept a new stream.
    ///
    /// Resolves when the number of active streams is below the peer's
    /// `MAX_CONCURRENT_STREAMS` limit, or returns an error if the connection
    /// is closing.
    ///
    /// Use this before [`send_request`](Self::send_request) to avoid getting a
    /// `RefusedStream` error when the limit has been reached.
    ///
    /// This operation is cancel-safe.
    pub async fn ready(&mut self) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::PollReady { response_tx: tx })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))?
    }

    /// Initiate a graceful shutdown by sending a GOAWAY frame.
    ///
    /// After calling this, no new streams can be opened. Existing streams
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

    /// Send an HTTP/2 request.
    ///
    /// The returned [`ResponseFuture`] resolves when the response headers
    /// arrive. If `end_of_stream` is false, a [`SendStream`] is also returned
    /// for sending the request body.
    ///
    /// This operation is *not* cancel-safe; if dropped after dispatch, an
    /// orphaned stream may be opened.
    pub async fn send_request(
        &mut self,
        request: http::Request<()>,
        end_of_stream: bool,
    ) -> Result<(ResponseFuture, Option<SendStream>), H2Error> {
        // Build pseudo-headers + regular headers
        let mut headers = Vec::new();
        headers.push((
            Bytes::from_static(b":method"),
            Bytes::copy_from_slice(request.method().as_str().as_bytes()),
        ));
        headers.push((
            Bytes::from_static(b":scheme"),
            Bytes::copy_from_slice(request.uri().scheme_str().unwrap_or("https").as_bytes()),
        ));
        headers.push((
            Bytes::from_static(b":path"),
            Bytes::copy_from_slice(
                request
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("/")
                    .as_bytes(),
            ),
        ));
        if let Some(authority) = request.uri().authority() {
            headers.push((
                Bytes::from_static(b":authority"),
                Bytes::copy_from_slice(authority.as_str().as_bytes()),
            ));
        }

        // Regular headers
        for (name, value) in request.headers() {
            headers.push((
                Bytes::copy_from_slice(name.as_str().as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            ));
        }

        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::NewStream {
                headers,
                end_stream: end_of_stream,
                response_tx: tx,
            })
            .await?;

        let (stream_id, stream_recv) = rx
            .recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed".into()))??;

        let reset_rx = stream_recv.reset_rx.clone();

        let response_future = ResponseFuture {
            stream_id,
            stream_recv: Some(stream_recv),
        };

        let send_stream = if !end_of_stream {
            Some(SendStream::new(stream_id, self.cmd_tx.clone(), reset_rx))
        } else {
            None
        };

        Ok((response_future, send_stream))
    }
}

/// Future that resolves to an HTTP response.
///
/// # Cancellation
///
/// `await_response` consumes `self` and is *not* cancel-safe.
pub struct ResponseFuture {
    stream_id: StreamId,
    stream_recv: Option<StreamRecv>,
}

impl ResponseFuture {
    /// Wait for the response headers.
    pub async fn await_response(mut self) -> Result<http::Response<RecvStream>, H2Error> {
        let stream_recv = self
            .stream_recv
            .take()
            .ok_or_else(|| H2Error::Protocol("response already consumed".into()))?;

        // Wait for response headers from the connection task
        let (status, headers) = if let Some(headers_rx) = stream_recv.headers_rx {
            headers_rx
                .recv_async()
                .await
                .map_err(|_| H2Error::Protocol("connection closed before response".into()))??
        } else {
            (http::StatusCode::OK, http::HeaderMap::new())
        };

        let recv_stream = RecvStream::new(
            self.stream_id,
            stream_recv.data_rx,
            stream_recv.trailers_rx,
            stream_recv.internal_tx,
        );

        let mut response = http::Response::builder()
            .status(status)
            .body(recv_stream)
            .map_err(|e| H2Error::Protocol(format!("failed to build response: {}", e)))?;

        *response.headers_mut() = headers;

        Ok(response)
    }
}

/// Client connection handle. Must be spawned as a background task.
///
/// # Cancellation
///
/// The [`run`](Self::run) future is *not* cancel-safe. Dropping it
/// terminates the connection immediately.
pub struct ClientConnection<R, W> {
    internal_rx: flume::Receiver<InternalMsg>,
    internal_tx: flume::Sender<InternalMsg>,
    read_half: R,
    write_half: W,
    settings: ConnSettings,
    ping_pong: PingPong,
    initial_connection_window_size: Option<u32>,
    extra: crate::proto::connection::ConnExtra,
}

impl<R: AsyncRead + 'static, W: AsyncWrite + 'static> ClientConnection<R, W> {
    /// Run the client connection. This should be spawned as a background task.
    pub async fn run(self) -> Result<(), H2Error> {
        crate::proto::connection::run_client_connection(
            self.read_half,
            self.write_half,
            self.internal_rx,
            self.internal_tx,
            crate::proto::connection::ConnConfig {
                settings: self.settings,
                ping_pong: self.ping_pong,
                initial_connection_window_size: self.initial_connection_window_size,
                extra: self.extra,
            },
        )
        .await
    }
}
