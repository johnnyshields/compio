use bytes::Bytes;
use compio_buf::BufResult;
use compio_io::{AsyncWrite, AsyncWriteExt};

use crate::{error::H2Error, frame::Frame};

/// Threshold at which the write buffer is automatically flushed.
const WRITE_BUFFER_FLUSH_THRESHOLD: usize = 16_384;

/// Writes HTTP/2 frames to an async writer with internal buffering.
///
/// Small control frames (SETTINGS, PING, WINDOW_UPDATE, RST_STREAM, PRIORITY)
/// are serialized into an internal buffer and flushed together. DATA frames
/// flush the buffer first, then write header + payload separately to avoid
/// copying large payloads (see `write_data_frame`).
pub struct FrameWriter<IO> {
    io: IO,
    buf: Vec<u8>,
}

impl<IO: AsyncWrite> FrameWriter<IO> {
    /// Create a new instance.
    pub fn new(io: IO) -> Self {
        FrameWriter {
            io,
            buf: Vec::with_capacity(WRITE_BUFFER_FLUSH_THRESHOLD),
        }
    }

    /// Buffer a frame for writing. Flushes automatically if the buffer
    /// exceeds the threshold.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), H2Error> {
        frame.encode(&mut self.buf);
        if self.buf.len() >= WRITE_BUFFER_FLUSH_THRESHOLD {
            self.flush_buf().await?;
        }
        Ok(())
    }

    /// Write a DATA frame efficiently: flush the buffer first, then write
    /// the 9-byte frame header from the buffer and the payload directly
    /// from the Bytes, avoiding a large copy into the write buffer.
    pub async fn write_data_frame(
        &mut self,
        header_bytes: [u8; 9],
        payload: &Bytes,
    ) -> Result<(), H2Error> {
        // Flush any pending buffered frames first
        self.flush_buf().await?;
        // Write 9-byte frame header
        let BufResult(result, _) = self.io.write_all(header_bytes).await;
        result.map_err(H2Error::Io)?;
        // Write payload directly from Bytes (no copy into buffer)
        if !payload.is_empty() {
            let BufResult(result, _) = self.io.write_all(payload.clone()).await;
            result.map_err(H2Error::Io)?;
        }
        Ok(())
    }

    /// Write raw bytes (used for connection preface).
    pub async fn write_all_bytes(&mut self, data: Vec<u8>) -> Result<(), H2Error> {
        // Flush buffer first so ordering is preserved
        self.flush_buf().await?;
        let BufResult(result, _) = self.io.write_all(data).await;
        result.map_err(H2Error::Io)?;
        Ok(())
    }

    /// Flush the internal write buffer to the underlying IO.
    pub async fn flush_buf(&mut self) -> Result<(), H2Error> {
        if !self.buf.is_empty() {
            let buf = std::mem::replace(
                &mut self.buf,
                Vec::with_capacity(WRITE_BUFFER_FLUSH_THRESHOLD),
            );
            let BufResult(result, _) = self.io.write_all(buf).await;
            result.map_err(H2Error::Io)?;
        }
        Ok(())
    }

    /// Shutdown the writer (flushes buffer first).
    pub async fn shutdown(&mut self) -> Result<(), H2Error> {
        self.flush_buf().await?;
        self.io.shutdown().await.map_err(H2Error::Io)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{
        codec::reader::FrameReader,
        frame::{Data, StreamId},
    };

    #[compio_macros::test]
    async fn test_write_and_read_frame() {
        let mut data_frame = Data::new(StreamId::new(1), Bytes::from_static(b"hello"));
        data_frame.set_end_stream();
        let frame = Frame::Data(data_frame);

        // Write to a Vec
        let mut output = Vec::new();
        let mut writer = FrameWriter::new(&mut output);
        writer.write_frame(&frame).await.unwrap();
        writer.flush_buf().await.unwrap();

        // Read back
        let cursor = Cursor::new(output);
        let mut reader = FrameReader::new(cursor);
        let read_frame = reader.read_frame().await.unwrap().unwrap();

        match read_frame {
            Frame::Data(d) => {
                assert_eq!(d.stream_id().value(), 1);
                assert!(d.is_end_stream());
                assert_eq!(d.payload().as_ref(), b"hello");
            }
            _ => panic!("expected Data frame"),
        }
    }

    #[compio_macros::test]
    async fn test_buffered_multiple_frames() {
        let mut output = Vec::new();
        let mut writer = FrameWriter::new(&mut output);

        // Write multiple small frames
        use crate::frame::{Ping, Settings, WindowUpdate};

        let ping = Frame::Ping(Ping::new([1, 2, 3, 4, 5, 6, 7, 8]));
        let settings = Frame::Settings(Settings::ack());
        let wu = Frame::WindowUpdate(WindowUpdate::new(StreamId::ZERO, 1000));

        writer.write_frame(&ping).await.unwrap();
        writer.write_frame(&settings).await.unwrap();
        writer.write_frame(&wu).await.unwrap();
        writer.flush_buf().await.unwrap();

        // Read them all back
        let cursor = Cursor::new(output);
        let mut reader = FrameReader::new(cursor);

        let f1 = reader.read_frame().await.unwrap().unwrap();
        assert!(matches!(f1, Frame::Ping(_)));
        let f2 = reader.read_frame().await.unwrap().unwrap();
        assert!(matches!(f2, Frame::Settings(_)));
        let f3 = reader.read_frame().await.unwrap().unwrap();
        assert!(matches!(f3, Frame::WindowUpdate(_)));
    }

    #[compio_macros::test]
    async fn test_write_data_frame_vectored() {
        let mut output = Vec::new();
        let mut writer = FrameWriter::new(&mut output);

        let stream_id = StreamId::new(1);
        let payload = Bytes::from_static(b"hello world");
        let len = payload.len() as u32;

        // Build header manually
        let header = crate::frame::FrameHeader::new(0x0, 0x1, stream_id, len);
        writer
            .write_data_frame(header.encode(), &payload)
            .await
            .unwrap();

        // Read back
        let cursor = Cursor::new(output);
        let mut reader = FrameReader::new(cursor);
        let frame = reader.read_frame().await.unwrap().unwrap();
        match frame {
            Frame::Data(d) => {
                assert_eq!(d.stream_id().value(), 1);
                assert!(d.is_end_stream());
                assert_eq!(d.payload().as_ref(), b"hello world");
            }
            _ => panic!("expected Data frame"),
        }
    }
}
