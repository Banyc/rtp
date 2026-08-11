use core::num::NonZeroU64;
use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;
use thiserror::Error;

use crate::delivery::frame::wire::{FRAME_DATA_TS_CMD, decode_frame_data_ts, encode_frame_data_ts};
use crate::sack::{SackBlock, SackIntervals};

const ACK_CMD: u8 = 0;
const DATA_CMD: u8 = 1;
const KILL_CMD: u8 = 2;
const DATA_TS_CMD: u8 = 3;
const ECHO_TS_CMD: u8 = 4;
// cmd 6 (`TAG_CMD`) authenticates control-bearing datagrams after the
// opening handshake has derived a per-connection session tag.
const TAG_CMD: u8 = 6;
// cmd 5 (`FRAME_DATA_TS_CMD`) belongs to frame-delivery mode; see
// `crate::delivery::frame::wire`.

#[cfg(test)]
pub fn in_cmd_space(buf: u8) -> bool {
    matches!(
        buf,
        ACK_CMD | DATA_CMD | KILL_CMD | DATA_TS_CMD | ECHO_TS_CMD | FRAME_DATA_TS_CMD | TAG_CMD
    )
}

pub fn encode_kill(tag: Option<u64>, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    if let Some(tag) = tag {
        write_tag(&mut wtr, tag)?;
    }
    wtr.write_u8(KILL_CMD)
        .pipe(wrap_insufficient_buffer_size_err)?;
    let pos = wtr.position();
    Ok(pos as usize)
}

/// Write the session-tag prefix (`TAG_CMD` + 8-byte tag) that authenticates
/// the control commands in a datagram.  The tag is only emitted for
/// datagrams that carry control commands (ACK / echo-timestamp / kill);
/// data-only datagrams stay untagged so the payload path keeps zero
/// overhead and the MSS math is unchanged.
fn write_tag(wtr: &mut io::Cursor<&mut [u8]>, tag: u64) -> Result<(), EncodeError> {
    wtr.write_u8(TAG_CMD)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u64::<BigEndian>(tag)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

pub fn encode_ack_data(
    tag: Option<u64>,
    ack: Option<EncodeAck<'_>>,
    echo_ts: Option<u32>,
    data: Option<EncodeData<'_>>,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    if let Some(tag) = tag.filter(|_| ack.is_some() || echo_ts.is_some()) {
        write_tag(&mut wtr, tag)?;
    }
    if let Some(EncodeAck {
        queue,
        first_block_index,
        max_blocks,
    }) = ack
    {
        for ack in queue.blocks().skip(first_block_index).take(max_blocks) {
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
    pub queue: &'a SackIntervals,
    pub first_block_index: usize,
    pub max_blocks: usize,
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
/// Decode one datagram.  When `session_tag` is `Some` (the connection was
/// opened with the handshake), control commands (ACK / echo-timestamp /
/// kill) are only honoured when the datagram carries a valid session-tag
/// prefix; an untagged or wrongly-tagged control datagram is rejected as
/// [`DecodeError::Unauthenticated`] and the whole datagram is dropped.  The
/// tag never needs to be guessed: it is derived from the opening-handshake
/// nonce, which an off-path attacker does not see.  Data-only datagrams are
/// accepted with or without a tag.
pub fn decode(
    buf: &[u8],
    ack: &mut Vec<SackBlock>,
    session_tag: Option<u64>,
) -> Result<Decoded, DecodeError> {
    let mut killed = false;
    let mut echo_ts = None;
    let mut tag_seen = false;
    let mut rdr = io::Cursor::new(buf);
    while let Ok(cmd) = rdr.read_u8() {
        match cmd {
            TAG_CMD => {
                let tag = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
                if session_tag != Some(tag) {
                    return Err(DecodeError::Unauthenticated);
                }
                tag_seen = true;
            }
            ACK_CMD => {
                require_tag(session_tag, tag_seen)?;
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
                require_tag(session_tag, tag_seen)?;
                let t = rdr.read_u32::<BigEndian>().pipe(wrap_corrupted_err)?;
                echo_ts = Some(t);
            }
            KILL_CMD => {
                require_tag(session_tag, tag_seen)?;
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

fn encode_ack(wtr: &mut io::Cursor<&mut [u8]>, ack: SackBlock) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(ack.start)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u64::<BigEndian>(ack.size.get())
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn decode_ack(rdr: &mut io::Cursor<&[u8]>) -> Result<SackBlock, DecodeError> {
    let start = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let size = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let size = NonZeroU64::new(size).ok_or(DecodeError::Corrupted)?;
    Ok(SackBlock { start, size })
}

pub const fn data_overhead() -> usize {
    let cmd = std::mem::size_of::<u8>();
    let seq = std::mem::size_of::<u64>();
    let send_ts = std::mem::size_of::<u32>();
    let len = std::mem::size_of::<u16>();
    cmd + seq + send_ts + len
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

pub(crate) fn wrap_insufficient_buffer_size_err<T>(
    res: std::io::Result<T>,
) -> Result<T, EncodeError> {
    res.map_err(|_| EncodeError::InsufficientBufferSize)
}

pub(crate) fn wrap_corrupted_err<T>(res: std::io::Result<T>) -> Result<T, DecodeError> {
    res.map_err(|_| DecodeError::Corrupted)
}

/// Control commands in a handshaked connection must be preceded by a valid
/// session tag.  Without this, a datagram forged at the peer's source
/// address could kill the session (`KILL_CMD`) or release the entire send
/// window (a `{start:0, size:u64::MAX}` ACK block) with no retransmission.
fn require_tag(session_tag: Option<u64>, tag_seen: bool) -> Result<(), DecodeError> {
    if session_tag.is_some() && !tag_seen {
        return Err(DecodeError::Unauthenticated);
    }
    Ok(())
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
    #[error("unauthenticated control command")]
    Unauthenticated,
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, EncodeAck, EncodeData, decode, encode_ack_data, encode_kill};
    use crate::sack::SackIntervals;

    #[test]
    fn roundtrip_ack_echo_data() {
        let mut queue = SackIntervals::new();
        for seq in 10..15 {
            queue.insert(seq);
        }
        let ack = EncodeAck {
            queue: &queue,
            first_block_index: 0,
            max_blocks: 64,
        };
        let data = EncodeData {
            seq: 42,
            send_ts: Some(12_345),
            frame_len: None,
            data: b"hello",
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, Some(ack), Some(0xdead_beef), Some(data), &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, None).unwrap();
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
        let decoded = decode(&buf, &mut acks, None).unwrap();
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
        let err = decode(&buf, &mut acks, None).unwrap_err();
        assert!(matches!(err, DecodeError::Corrupted));
    }

    #[test]
    fn handshaked_control_requires_a_valid_session_tag() {
        let tag = 0x1234_5678_9abc_def0;
        let mut queue = SackIntervals::new();
        for seq in 10..15 {
            queue.insert(seq);
        }
        let ack = EncodeAck {
            queue: &queue,
            first_block_index: 0,
            max_blocks: 64,
        };
        let mut buf = vec![0u8; 256];

        // A control datagram encoded with the session tag round-trips.
        let n = encode_ack_data(Some(tag), Some(ack), None, None, &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, Some(tag)).unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, 10);
        assert!(decoded.data.is_none());
        assert!(!decoded.killed);

        // The same untagged bytes are rejected on a handshaked connection...
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf[..n], &mut acks, None),
            Err(DecodeError::Unauthenticated)
        ));
        // ... and a wrong tag is rejected too.
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf[..n], &mut acks, Some(tag ^ 1)),
            Err(DecodeError::Unauthenticated)
        ));
        // The valid tag is accepted regardless of its position as long as it
        // precedes the first control command.
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, Some(tag)).unwrap();
        assert_eq!(acks.len(), 1);
        assert!(!decoded.killed);
    }

    #[test]
    fn data_only_datagrams_need_no_tag_but_a_wrong_tag_is_rejected() {
        let tag = 0x1234_5678_9abc_def0;
        let data = EncodeData {
            seq: 7,
            send_ts: None,
            frame_len: None,
            data: b"payload",
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, None, None, Some(data.clone()), &mut buf).unwrap();
        // Untagged data-only datagram: accepted on a handshaked connection.
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, Some(tag)).unwrap();
        assert!(acks.is_empty());
        assert_eq!(&buf[decoded.data.unwrap().buf_range], b"payload");
        // A forged tag on a data-only datagram is still rejected.  (The
        // encoder only emits a tag for control-bearing datagrams, so craft
        // the bytes by hand: TAG_CMD 6 + 8-byte tag + DATA_CMD payload.)
        let mut forged = vec![6u8];
        forged.extend_from_slice(&(tag ^ 0xff).to_be_bytes());
        forged.push(1); // DATA_CMD
        forged.extend_from_slice(&7u64.to_be_bytes());
        forged.extend_from_slice(&7u16.to_be_bytes());
        forged.extend_from_slice(b"payload");
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&forged, &mut acks, Some(tag)),
            Err(DecodeError::Unauthenticated)
        ));
        // A valid tag on a data-only datagram is accepted.
        let mut tagged = vec![6u8];
        tagged.extend_from_slice(&tag.to_be_bytes());
        tagged.push(1); // DATA_CMD
        tagged.extend_from_slice(&7u64.to_be_bytes());
        tagged.extend_from_slice(&7u16.to_be_bytes());
        tagged.extend_from_slice(b"payload");
        let mut acks = Vec::new();
        let decoded = decode(&tagged, &mut acks, Some(tag)).unwrap();
        assert_eq!(&tagged[decoded.data.unwrap().buf_range], b"payload");
    }

    #[test]
    fn kill_roundtrip_with_session_tag() {
        let tag = 0xdead_beef_cafe_f00d;
        let mut buf = [0u8; 10];
        let n = encode_kill(Some(tag), &mut buf).unwrap();
        let mut acks = Vec::new();
        assert!(decode(&buf[..n], &mut acks, None).is_err());
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, Some(tag)).unwrap();
        assert!(decoded.killed);
        assert!(decoded.data.is_none());
        // Legacy untagged kill still works on non-handshaked connections.
        let n = encode_kill(None, &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, None).unwrap();
        assert!(decoded.killed);
    }
}
