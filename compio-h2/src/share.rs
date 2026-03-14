use bytes::Bytes;

use crate::{
    error::{H2Error, Reason},
    frame::StreamId,
    proto::connection::{Command, CommandSender, InternalMsg},
};

/// Handle for releasing recv flow control capacity on an HTTP/2 stream.
///
/// When data is received on a [`RecvStream`], the flow control window shrinks.
/// The application must call [`release_capacity`](Self::release_capacity) to
/// indicate that it has processed the data and the window can be replenished
/// via a WINDOW_UPDATE frame to the peer.
///
/// This gives applications back-pressure control: the peer cannot send more
/// data than the application is willing to buffer.
pub struct RecvFlowControl {
    stream_id: StreamId,
    unreleased: u32,
    internal_tx: flume::Sender<InternalMsg>,
}

impl RecvFlowControl {
    pub(crate) fn new(stream_id: StreamId, internal_tx: flume::Sender<InternalMsg>) -> Self {
        RecvFlowControl {
            stream_id,
            unreleased: 0,
            internal_tx,
        }
    }

    /// Bytes received but not yet released.
    pub fn unreleased(&self) -> usize {
        self.unreleased as usize
    }

    /// Release `sz` bytes of flow control capacity back to the peer.
    ///
    /// This signals the connection task to send a WINDOW_UPDATE for this
    /// stream. `sz` must not exceed the number of unreleased bytes.
    pub fn release_capacity(&mut self, sz: usize) -> Result<(), H2Error> {
        let sz = sz as u32;
        if sz > self.unreleased {
            return Err(H2Error::Protocol(format!(
                "release_capacity({}) exceeds unreleased bytes ({})",
                sz, self.unreleased
            )));
        }
        self.unreleased -= sz;
        // Best-effort send — if the connection is gone, the stream is dead anyway
        let _ = self.internal_tx.send(InternalMsg::ReleaseCapacity {
            stream_id: self.stream_id,
            amount: sz,
        });
        Ok(())
    }

    pub(crate) fn add_unreleased(&mut self, amount: u32) {
        self.unreleased += amount;
    }
}

/// Handle for sending data and trailers on an HTTP/2 stream.
///
/// If dropped without sending a final frame with `end_of_stream: true` or
/// calling [`send_trailers`](Self::send_trailers), the stream is left
/// half-open from the peer's perspective. No `RST_STREAM` or `END_STREAM`
/// is sent automatically.
///
/// # Cancellation
///
/// Unless noted otherwise, methods on this type are *not* cancel-safe:
/// dropping a future after the command has been dispatched may leave the
/// operation completed on the wire without the caller observing the result.
pub struct SendStream {
    stream_id: StreamId,
    cmd_tx: CommandSender,
    reset_rx: flume::Receiver<Reason>,
    /// Bytes of send capacity currently reserved by the application.
    reserved: u32,
}

impl SendStream {
    pub(crate) fn new(
        stream_id: StreamId,
        cmd_tx: CommandSender,
        reset_rx: flume::Receiver<Reason>,
    ) -> Self {
        SendStream {
            stream_id,
            cmd_tx,
            reset_rx,
            reserved: 0,
        }
    }

    /// Stream id.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Currently reserved send capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.reserved as usize
    }

    /// Request send capacity on this stream.
    ///
    /// Asks the connection for up to `sz` bytes of send capacity, constrained
    /// by the stream-level and connection-level flow control windows. The
    /// granted capacity (which may be less than requested) is added to the
    /// internal reservation and can be queried via
    /// [`capacity`](Self::capacity).
    ///
    /// If no capacity is currently available, this future will wait until a
    /// WINDOW_UPDATE is received from the peer.
    ///
    /// This is **not additive**: calling `reserve_capacity(100)` then
    /// `reserve_capacity(200)` results in at most 200 reserved bytes, not 300.
    /// The second call replaces the prior reservation target.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn reserve_capacity(&mut self, sz: usize) -> Result<(), H2Error> {
        let target = sz as u32;
        if self.reserved >= target {
            return Ok(());
        }
        let needed = target - self.reserved;

        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::ReserveCapacity {
                stream_id: self.stream_id,
                amount: needed,
                response_tx: tx,
            })
            .await?;

        let granted = rx
            .recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed during reserve_capacity".into()))??;

        self.reserved += granted;
        Ok(())
    }

    /// Wait for send capacity to become available on this stream.
    ///
    /// Send capacity currently reserved after the wait, or `None` if the
    /// stream has been reset or the connection has been closed.
    ///
    /// This is useful in a loop to send data as capacity becomes available,
    /// calling [`send_data`](Self::send_data) with each granted chunk.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn poll_capacity(&mut self) -> Option<Result<usize, H2Error>> {
        if self.reserved > 0 {
            return Some(Ok(self.reserved as usize));
        }

        let (tx, rx) = flume::bounded(1);
        if self
            .cmd_tx
            .send_cmd(Command::ReserveCapacity {
                stream_id: self.stream_id,
                amount: u32::MAX,
                response_tx: tx,
            })
            .await
            .is_err()
        {
            return None;
        }

        match rx.recv_async().await {
            Ok(Ok(granted)) => {
                self.reserved += granted;
                Some(Ok(self.reserved as usize))
            }
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => None,
        }
    }

    /// Send data on this stream.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn send_data(
        &mut self,
        data: impl Into<Bytes>,
        end_of_stream: bool,
    ) -> Result<(), H2Error> {
        let data = data.into();
        let len = data.len() as u32;
        // Consume from reserved capacity (allow sending without reservation too)
        self.reserved = self.reserved.saturating_sub(len);

        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SendData {
                stream_id: self.stream_id,
                data,
                end_stream: end_of_stream,
                response_tx: tx,
            })
            .await?;

        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed during send_data".into()))?
    }

    /// Send a RST_STREAM frame to reset this stream with the given reason.
    ///
    /// This immediately closes the stream in both directions.
    ///
    /// This operation is *not* cancel-safe.
    pub async fn send_reset(&self, reason: Reason) -> Result<(), H2Error> {
        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SendReset {
                stream_id: self.stream_id,
                reason,
                response_tx: tx,
            })
            .await?;
        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed during send_reset".into()))?
    }

    /// Send trailers on this stream (implicitly sets END_STREAM).
    ///
    /// This operation is *not* cancel-safe.
    pub async fn send_trailers(&mut self, trailers: http::HeaderMap) -> Result<(), H2Error> {
        let trailer_vec: Vec<(Bytes, Bytes)> = trailers
            .iter()
            .map(|(k, v)| {
                (
                    Bytes::copy_from_slice(k.as_str().as_bytes()),
                    Bytes::copy_from_slice(v.as_bytes()),
                )
            })
            .collect();

        let (tx, rx) = flume::bounded(1);
        self.cmd_tx
            .send_cmd(Command::SendTrailers {
                stream_id: self.stream_id,
                trailers: trailer_vec,
                response_tx: tx,
            })
            .await?;

        rx.recv_async()
            .await
            .map_err(|_| H2Error::Protocol("connection closed during send_trailers".into()))?
    }

    /// The reason code from a RST_STREAM frame.
    ///
    /// This operation is cancel-safe.
    pub async fn poll_reset(&mut self) -> Result<Reason, H2Error> {
        self.reset_rx
            .recv_async()
            .await
            .map_err(|_| H2Error::Protocol("stream closed without reset".into()))
    }
}

/// Handle for receiving data and trailers on an HTTP/2 stream.
///
/// After receiving data via [`data()`](Self::data), call
/// [`flow_control`](Self::flow_control) then
/// [`release_capacity`](RecvFlowControl::release_capacity) to replenish the
/// peer's send window.
///
/// If dropped before consuming all data, unconsumed frames are silently
/// discarded.
///
/// # Cancellation
///
/// Methods on this type are cancel-safe: dropping a future before
/// completion loses no data.
pub struct RecvStream {
    stream_id: StreamId,
    data_rx: flume::Receiver<Result<Bytes, H2Error>>,
    trailers_rx: flume::Receiver<Result<http::HeaderMap, H2Error>>,
    flow_control: RecvFlowControl,
}

impl RecvStream {
    pub(crate) fn new(
        stream_id: StreamId,
        data_rx: flume::Receiver<Result<Bytes, H2Error>>,
        trailers_rx: flume::Receiver<Result<http::HeaderMap, H2Error>>,
        internal_tx: flume::Sender<InternalMsg>,
    ) -> Self {
        RecvStream {
            stream_id,
            data_rx,
            trailers_rx,
            flow_control: RecvFlowControl::new(stream_id, internal_tx),
        }
    }

    /// The stream identifier.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Mutable reference to the recv flow control handle.
    pub fn flow_control(&mut self) -> &mut RecvFlowControl {
        &mut self.flow_control
    }

    /// Receive the next chunk of data. Returns `None` when the stream ends.
    ///
    /// The received bytes are tracked as unreleased in the flow control handle.
    /// Call [`flow_control`](Self::flow_control) and
    /// [`release_capacity`](RecvFlowControl::release_capacity) after processing
    /// the data.
    ///
    /// This operation is cancel-safe.
    pub async fn data(&mut self) -> Option<Result<Bytes, H2Error>> {
        match self.data_rx.recv_async().await.ok() {
            Some(Ok(bytes)) => {
                self.flow_control.add_unreleased(bytes.len() as u32);
                Some(Ok(bytes))
            }
            other => other,
        }
    }

    /// Receive trailers. Returns `None` if no trailers were sent.
    ///
    /// This operation is cancel-safe.
    pub async fn trailers(&self) -> Option<Result<http::HeaderMap, H2Error>> {
        self.trailers_rx.recv_async().await.ok()
    }
}
