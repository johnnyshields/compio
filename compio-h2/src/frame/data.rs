use bytes::Bytes;

use super::{FRAME_TYPE_DATA, FrameHeader, stream_id::StreamId};
use crate::error::FrameError;

const FLAG_END_STREAM: u8 = 0x1;
const FLAG_PADDED: u8 = 0x8;

/// DATA frame (type=0x0).
#[derive(Debug, Clone)]
pub struct Data {
    stream_id: StreamId,
    payload: Bytes,
    flags: u8,
}

impl Data {
    /// Create a new DATA frame with the given stream ID and payload.
    pub fn new(stream_id: StreamId, payload: Bytes) -> Self {
        Data {
            stream_id,
            payload,
            flags: 0,
        }
    }

    /// The stream identifier for this DATA frame.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// A reference to the frame payload data.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Consume this frame and return the payload data.
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    /// Whether the END_STREAM flag (0x1) is set.
    pub fn is_end_stream(&self) -> bool {
        self.flags & FLAG_END_STREAM != 0
    }

    /// Set the END_STREAM flag, indicating no further data will be sent on
    /// this stream.
    pub fn set_end_stream(&mut self) {
        self.flags |= FLAG_END_STREAM;
    }

    /// Whether the PADDED flag (0x8) is set.
    pub fn is_padded(&self) -> bool {
        self.flags & FLAG_PADDED != 0
    }

    /// The raw flags byte.
    pub fn flags(&self) -> u8 {
        self.flags
    }

    /// Decode a DATA frame from the payload bytes (after the 9-byte header).
    pub fn decode(stream_id: StreamId, flags: u8, payload: Bytes) -> Result<Self, FrameError> {
        if stream_id.is_zero() {
            return Err(FrameError::InvalidStreamId(
                "DATA frame with stream ID 0".into(),
            ));
        }

        let actual_payload = if flags & FLAG_PADDED != 0 {
            if payload.is_empty() {
                return Err(FrameError::InvalidPadding(
                    "padded DATA frame with no pad length".into(),
                ));
            }
            let pad_len = payload[0] as usize;
            if pad_len >= payload.len() {
                return Err(FrameError::InvalidPadding(
                    "pad length exceeds frame payload".into(),
                ));
            }
            payload.slice(1..payload.len() - pad_len)
        } else {
            payload
        };

        Ok(Data {
            stream_id,
            payload: actual_payload,
            flags,
        })
    }

    /// Encode this DATA frame into bytes (header + payload).
    pub fn encode(&self, dst: &mut Vec<u8>) {
        let len = self.payload.len() as u32;
        dst.extend_from_slice(
            &FrameHeader::new(FRAME_TYPE_DATA, self.flags, self.stream_id, len).encode(),
        );
        dst.extend_from_slice(&self.payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_roundtrip() {
        let mut frame = Data::new(StreamId::new(1), Bytes::from_static(b"hello"));
        frame.set_end_stream();

        let mut buf = Vec::new();
        frame.encode(&mut buf);

        // Parse header
        assert_eq!(buf.len(), 9 + 5);
        let len = ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32);
        assert_eq!(len, 5);
        assert_eq!(buf[3], 0x0); // DATA type
        let flags = buf[4];
        let sid = ((buf[5] as u32) << 24)
            | ((buf[6] as u32) << 16)
            | ((buf[7] as u32) << 8)
            | (buf[8] as u32);

        let decoded =
            Data::decode(StreamId::new(sid), flags, Bytes::copy_from_slice(&buf[9..])).unwrap();
        assert_eq!(decoded.stream_id().value(), 1);
        assert!(decoded.is_end_stream());
        assert_eq!(decoded.payload().as_ref(), b"hello");
    }
}
