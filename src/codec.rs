use core::num::NonZeroU64;
use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;
use thiserror::Error;

use crate::sack::{AckBall, AckQueue};

const ACK_CMD: u8 = 0;
const DATA_CMD: u8 = 1;
const KILL_CMD: u8 = 2;
const DATA_TS_CMD: u8 = 3;
const ECHO_TS_CMD: u8 = 4;
/// First packet of a frame in frame-delivery mode.  Same fields as
/// `DATA_TS_CMD` plus a `u32 frame_len` (the total application frame length in
/// bytes).  Continuation packets of the frame use the stock `DATA_TS_CMD`.
/// Never emitted when frame-delivery mode is off.
const FRAME_DATA_TS_CMD: u8 = 5;

pub fn in_cmd_space(buf: u8) -> bool {
    matches!(
        buf,
        ACK_CMD | DATA_CMD | KILL_CMD | DATA_TS_CMD | ECHO_TS_CMD | FRAME_DATA_TS_CMD
    )
}

pub fn encode_kill(buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    wtr.write_u8(KILL_CMD)
        .pipe(wrap_insufficient_buffer_size_err)?;
    let pos = wtr.position();
    Ok(pos as usize)
}

pub fn encode_ack_data(
    ack: Option<EncodeAck<'_>>,
    echo_ts: Option<u32>,
    data: Option<EncodeData<'_>>,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    if let Some(EncodeAck {
        queue,
        skip,
        max_take,
    }) = ack
    {
        for ack in queue.balls().skip(skip).take(max_take) {
            wtr.write_u8(ACK_CMD)
                .pipe(wrap_insufficient_buffer_size_err)?;
            encode_ack(&mut wtr, ack)?;
        }
    }
    if let Some(echo_ts) = echo_ts {
        wtr.write_u8(ECHO_TS_CMD)
            .pipe(wrap_insufficient_buffer_size_err)?;
        wtr.write_u32::<BigEndian>(echo_ts)
            .pipe(wrap_insufficient_buffer_size_err)?;
    }
    if let Some(EncodeData {
        seq,
        send_ts,
        frame_len,
        data,
    }) = data
    {
        match (send_ts, frame_len) {
            (Some(send_ts), Some(frame_len)) => {
                wtr.write_u8(FRAME_DATA_TS_CMD)
                    .pipe(wrap_insufficient_buffer_size_err)?;
                encode_frame_data_ts(&mut wtr, seq, send_ts, frame_len, data)?;
            }
            (Some(send_ts), None) => {
                wtr.write_u8(DATA_TS_CMD)
                    .pipe(wrap_insufficient_buffer_size_err)?;
                encode_data_ts(&mut wtr, seq, send_ts, data)?;
            }
            (None, None) => {
                wtr.write_u8(DATA_CMD)
                    .pipe(wrap_insufficient_buffer_size_err)?;
                encode_data(&mut wtr, seq, data)?;
            }
            (None, Some(_)) => {
                // `frame_len` without a timestamp is not a valid wire shape:
                // frame-delivery mode always uses timestamps so the receiver's
                // RTT echo path stays intact.  Treat it as an encode error.
                return Err(EncodeError::InsufficientBufferSize);
            }
        }
    }
    let pos = wtr.position();
    Ok(pos as usize)
}
#[derive(Debug, Clone)]
pub struct EncodeAck<'a> {
    pub queue: &'a AckQueue,
    pub skip: usize,
    pub max_take: usize,
}
impl EncodeAck<'_> {
    pub fn next_page(&self) -> Option<Self> {
        let skip = self.skip + self.max_take;
        if self.queue.balls().count() <= skip {
            return None;
        }
        Some(Self {
            queue: self.queue,
            skip,
            max_take: self.max_take,
        })
    }
}
#[derive(Debug, Clone)]
pub struct EncodeData<'a> {
    pub seq: u64,
    pub send_ts: Option<u32>,
    /// Total application frame length in bytes.  `Some` only for the first
    /// packet of a frame in frame-delivery mode; `None` for all stock packets
    /// and for continuation packets of a frame.  When `Some`, `send_ts` must
    /// also be `Some` (frame-delivery mode always uses timestamps).
    pub frame_len: Option<u32>,
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct Decoded {
    pub data: Option<DecodedDataPkt>,
    pub echo_ts: Option<u32>,
    /// broken pipe
    pub killed: bool,
}
#[derive(Debug, Clone)]
pub struct DecodedDataPkt {
    pub seq: u64,
    pub send_ts: Option<u32>,
    /// Total application frame length in bytes.  `Some` only for the first
    /// packet of a frame in frame-delivery mode (cmd `FRAME_DATA_TS`); `None`
    /// for all stock packets and for continuation packets.
    pub frame_len: Option<u32>,
    pub buf_range: std::ops::Range<usize>,
}
pub fn decode(buf: &[u8], ack: &mut Vec<AckBall>) -> Result<Decoded, DecodeError> {
    let mut killed = false;
    let mut echo_ts = None;
    let mut rdr = io::Cursor::new(buf);
    while let Ok(cmd) = rdr.read_u8() {
        match cmd {
            ACK_CMD => {
                let a = decode_ack(&mut rdr)?;
                ack.push(a);
            }
            DATA_CMD => {
                let data = decode_data(&mut rdr)?;
                return Ok(Decoded {
                    data: Some(data),
                    echo_ts,
                    killed,
                });
            }
            DATA_TS_CMD => {
                let data = decode_data_ts(&mut rdr)?;
                return Ok(Decoded {
                    data: Some(data),
                    echo_ts,
                    killed,
                });
            }
            FRAME_DATA_TS_CMD => {
                let data = decode_frame_data_ts(&mut rdr)?;
                return Ok(Decoded {
                    data: Some(data),
                    echo_ts,
                    killed,
                });
            }
            ECHO_TS_CMD => {
                let t = rdr.read_u32::<BigEndian>().pipe(wrap_corrupted_err)?;
                echo_ts = Some(t);
            }
            KILL_CMD => {
                killed = true;
            }
            _ => return Err(DecodeError::Corrupted),
        }
    }
    Ok(Decoded {
        data: None,
        echo_ts,
        killed,
    })
}

fn encode_ack(wtr: &mut io::Cursor<&mut [u8]>, ack: AckBall) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(ack.start)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u64::<BigEndian>(ack.size.get())
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn decode_ack(rdr: &mut io::Cursor<&[u8]>) -> Result<AckBall, DecodeError> {
    let start = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let size = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let size = NonZeroU64::new(size).ok_or(DecodeError::Corrupted)?;
    Ok(AckBall { start, size })
}

pub const fn data_overhead() -> usize {
    let cmd = std::mem::size_of::<u8>();
    let seq = std::mem::size_of::<u64>();
    let send_ts = std::mem::size_of::<u32>();
    let len = std::mem::size_of::<u16>();
    cmd + seq + send_ts + len
}

/// Per-packet wire overhead of a `FRAME_DATA_TS` (cmd 5) packet: the stock
/// `data_overhead()` plus the `u32 frame_len` field.  Only the first packet of
/// a frame pays this; continuation packets use `DATA_TS` and pay
/// `data_overhead()`.
pub const fn frame_data_overhead() -> usize {
    data_overhead() + std::mem::size_of::<u32>()
}

fn encode_data(wtr: &mut io::Cursor<&mut [u8]>, seq: u64, data: &[u8]) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(seq)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u16::<BigEndian>(data.len().try_into().unwrap())
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_all(data)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn encode_data_ts(
    wtr: &mut io::Cursor<&mut [u8]>,
    seq: u64,
    send_ts: u32,
    data: &[u8],
) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(seq)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u32::<BigEndian>(send_ts)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u16::<BigEndian>(data.len().try_into().unwrap())
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_all(data)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn encode_frame_data_ts(
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

fn decode_data(rdr: &mut io::Cursor<&[u8]>) -> Result<DecodedDataPkt, DecodeError> {
    let seq = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let len = rdr.read_u16::<BigEndian>().pipe(wrap_corrupted_err)?;
    let end = usize::try_from(rdr.position()).unwrap() + usize::from(len);
    if rdr.get_ref().len() < end {
        return Err(DecodeError::Corrupted);
    }
    let start = rdr.position() as usize;
    Ok(DecodedDataPkt {
        seq,
        send_ts: None,
        frame_len: None,
        buf_range: start..end,
    })
}

fn decode_data_ts(rdr: &mut io::Cursor<&[u8]>) -> Result<DecodedDataPkt, DecodeError> {
    let seq = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let send_ts = rdr.read_u32::<BigEndian>().pipe(wrap_corrupted_err)?;
    let len = rdr.read_u16::<BigEndian>().pipe(wrap_corrupted_err)?;
    let end = usize::try_from(rdr.position()).unwrap() + usize::from(len);
    if rdr.get_ref().len() < end {
        return Err(DecodeError::Corrupted);
    }
    let start = rdr.position() as usize;
    Ok(DecodedDataPkt {
        seq,
        send_ts: Some(send_ts),
        frame_len: None,
        buf_range: start..end,
    })
}

fn decode_frame_data_ts(rdr: &mut io::Cursor<&[u8]>) -> Result<DecodedDataPkt, DecodeError> {
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

fn wrap_insufficient_buffer_size_err<T>(res: std::io::Result<T>) -> Result<T, EncodeError> {
    res.map_err(|_| EncodeError::InsufficientBufferSize)
}

fn wrap_corrupted_err<T>(res: std::io::Result<T>) -> Result<T, DecodeError> {
    res.map_err(|_| DecodeError::Corrupted)
}

#[derive(Debug, Clone, Error)]
pub enum EncodeError {
    #[error("insufficient buffer size")]
    InsufficientBufferSize,
}

#[derive(Debug, Clone, Error)]
pub enum DecodeError {
    #[error("corrupted")]
    Corrupted,
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, EncodeAck, EncodeData, decode, encode_ack_data};
    use crate::sack::AckQueue;

    #[test]
    fn roundtrip_ack_echo_data() {
        let mut queue = AckQueue::new();
        for seq in 10..15 {
            queue.insert(seq);
        }
        let ack = EncodeAck {
            queue: &queue,
            skip: 0,
            max_take: 64,
        };
        let data = EncodeData {
            seq: 42,
            send_ts: Some(12_345),
            frame_len: None,
            data: b"hello",
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(Some(ack), Some(0xdead_beef), Some(data), &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks).unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, 10);
        assert_eq!(acks[0].size.get(), 5);
        assert_eq!(decoded.echo_ts, Some(0xdead_beef));
        let data = decoded.data.unwrap();
        assert_eq!(data.seq, 42);
        assert_eq!(data.send_ts, Some(12_345));
        assert_eq!(&buf[data.buf_range], b"hello");
    }

    #[test]
    fn decodes_legacy_data_without_ts() {
        // DATA_CMD: cmd u8 + seq u64 BE + len u16 BE + payload
        let mut buf = Vec::new();
        buf.push(1); // DATA_CMD
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&5u16.to_be_bytes());
        buf.extend_from_slice(b"hello");
        let mut acks = Vec::new();
        let decoded = decode(&buf, &mut acks).unwrap();
        assert!(acks.is_empty());
        assert_eq!(decoded.echo_ts, None);
        let data = decoded.data.unwrap();
        assert_eq!(data.seq, 42);
        assert_eq!(data.send_ts, None);
        assert_eq!(&buf[data.buf_range], b"hello");
    }

    #[test]
    fn unknown_cmd_is_corrupted() {
        let buf = [0xff];
        let mut acks = Vec::new();
        let err = decode(&buf, &mut acks).unwrap_err();
        assert!(matches!(err, DecodeError::Corrupted));
    }

    #[test]
    fn roundtrip_frame_data_ts() {
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

    #[test]
    fn frame_data_overhead_is_data_overhead_plus_four() {
        assert_eq!(super::frame_data_overhead(), super::data_overhead() + 4);
    }
}
