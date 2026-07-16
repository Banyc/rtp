//! The `FRAME_DATA_TS` wire command — frame delivery's only wire-format
//! footprint.
//!
//! In frame-delivery mode the first packet of each application frame is
//! emitted with the `FRAME_DATA_TS` codec command: the same fields as the
//! stock `DATA_TS` command plus a `u32 frame_len` (the total application
//! frame length in bytes).  Continuation packets of the frame use the stock
//! `DATA_TS` command.  The command is never emitted when frame-delivery mode
//! is off, so stock connections stay byte-for-byte identical on the wire.

use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;

use crate::codec::{
    DecodeError, DecodedDataPkt, EncodeError, data_overhead, wrap_corrupted_err,
    wrap_insufficient_buffer_size_err,
};

/// First packet of a frame in frame-delivery mode.  Same fields as
/// `DATA_TS_CMD` plus a `u32 frame_len` (the total application frame length in
/// bytes).  Continuation packets of the frame use the stock `DATA_TS_CMD`.
/// Never emitted when frame-delivery mode is off.
pub(crate) const FRAME_DATA_TS_CMD: u8 = 5;

/// Per-packet wire overhead of a `FRAME_DATA_TS` (cmd 5) packet: the stock
/// `data_overhead()` plus the `u32 frame_len` field.  Only the first packet of
/// a frame pays this; continuation packets use `DATA_TS` and pay
/// `data_overhead()`.
pub const fn frame_data_overhead() -> usize {
    data_overhead() + std::mem::size_of::<u32>()
}

pub(crate) fn encode_frame_data_ts(
    wtr: &mut io::Cursor<&mut [u8]>,
    seq: u64,
    send_ts: u32,
    frame_len: u32,
    data: &[u8],
) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(seq)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u32::<BigEndian>(send_ts)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u32::<BigEndian>(frame_len)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u16::<BigEndian>(data.len().try_into().unwrap())
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_all(data)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

pub(crate) fn decode_frame_data_ts(
    rdr: &mut io::Cursor<&[u8]>,
) -> Result<DecodedDataPkt, DecodeError> {
    let seq = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let send_ts = rdr.read_u32::<BigEndian>().pipe(wrap_corrupted_err)?;
    let frame_len = rdr.read_u32::<BigEndian>().pipe(wrap_corrupted_err)?;
    let len = rdr.read_u16::<BigEndian>().pipe(wrap_corrupted_err)?;
    let end = usize::try_from(rdr.position()).unwrap() + usize::from(len);
    if rdr.get_ref().len() < end {
        return Err(DecodeError::Corrupted);
    }
    let start = rdr.position() as usize;
    Ok(DecodedDataPkt {
        seq,
        send_ts: Some(send_ts),
        frame_len: Some(frame_len),
        buf_range: start..end,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn frame_data_overhead_is_data_overhead_plus_four() {
        assert_eq!(
            super::frame_data_overhead(),
            crate::codec::data_overhead() + 4
        );
    }

    #[test]
    fn roundtrip_frame_data_ts() {
        use crate::codec::{EncodeData, decode, encode_ack_data};

        let data = EncodeData {
            seq: 7,
            send_ts: Some(99_999),
            frame_len: Some(20_000),
            data: b"frame-body",
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, None, Some(data), &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks).unwrap();
        assert!(acks.is_empty());
        let data = decoded.data.unwrap();
        assert_eq!(data.seq, 7);
        assert_eq!(data.send_ts, Some(99_999));
        assert_eq!(data.frame_len, Some(20_000));
        assert_eq!(&buf[data.buf_range], b"frame-body");
    }
}
