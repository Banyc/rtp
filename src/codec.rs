use core::num::NonZeroU64;
use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;
use thiserror::Error;

use crate::ack::{AckInterval, EncodeAck, MAX_ACK_BLOCKS};
use crate::delivery::frame::wire::{FRAME_DATA_TS_CMD, decode_frame_data_ts, encode_frame_data_ts};
use crate::sequence::SequenceNumber;

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
    if let Some(ack) = ack {
        // Latest-only ACK layout: one ACK_CMD, then cumulative next, a
        // one-byte bounded count, then that many (start, size) ranges.
        wtr.write_u8(ACK_CMD)
            .pipe(wrap_insufficient_buffer_size_err)?;
        wtr.write_u64::<BigEndian>(ack.next().to_wire())
            .pipe(wrap_insufficient_buffer_size_err)?;
        let count = ack.block_count().min(MAX_ACK_BLOCKS);
        wtr.write_u8(count as u8)
            .pipe(wrap_insufficient_buffer_size_err)?;
        for block in ack.blocks().take(count) {
            encode_ack(&mut wtr, block)?;
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
pub struct EncodeData<'a> {
    pub seq: SequenceNumber,
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
    /// The peer's cumulative ACK next: one past the last sequence it has
    /// received in order.  `Some` only when the datagram carried an ACK
    /// command; a data-only packet must not fabricate an ACK event.
    pub ack_next: Option<SequenceNumber>,
    pub data: Option<DecodedDataPkt>,
    pub echo_ts: Option<u32>,
    /// broken pipe
    pub killed: bool,
}
#[derive(Debug, Clone)]
pub struct DecodedDataPkt {
    pub seq: SequenceNumber,
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
    ack: &mut Vec<AckInterval>,
    session_tag: Option<u64>,
) -> Result<Decoded, DecodeError> {
    let mut killed = false;
    let mut echo_ts = None;
    let mut ack_next = None;
    let mut tag_seen = false;
    let mut ack_cmd_seen = false;
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
                // Latest-only framing: at most one ACK_CMD per datagram. A
                // second ACK_CMD after a complete ACK is the zero-tail
                // padding rule (the fitted-ack fill is zeros): consume the
                // rest of the datagram and require every byte to be 0x00,
                // so a padded ACK decodes with the padding stripped. Any
                // nonzero byte keeps the historical Corrupted verdict.
                if ack_cmd_seen {
                    if buf[rdr.position() as usize..].iter().all(|&b| b == 0) {
                        break;
                    }
                    return Err(DecodeError::Corrupted);
                }
                ack_cmd_seen = true;
                let next = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
                let count = rdr.read_u8().pipe(wrap_corrupted_err)?;
                if MAX_ACK_BLOCKS < count as usize {
                    return Err(DecodeError::Corrupted);
                }
                for _ in 0..count {
                    let a = decode_ack(&mut rdr)?;
                    ack.push(a);
                }
                ack_next = Some(SequenceNumber::from_wire(next));
            }
            DATA_CMD => {
                let data = decode_data(&mut rdr)?;
                return Ok(Decoded {
                    ack_next,
                    data: Some(data),
                    echo_ts,
                    killed,
                });
            }
            DATA_TS_CMD => {
                let data = decode_data_ts(&mut rdr)?;
                return Ok(Decoded {
                    ack_next,
                    data: Some(data),
                    echo_ts,
                    killed,
                });
            }
            FRAME_DATA_TS_CMD => {
                let data = decode_frame_data_ts(&mut rdr)?;
                return Ok(Decoded {
                    ack_next,
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
        ack_next,
        data: None,
        echo_ts,
        killed,
    })
}

/// The wire size of one ACK interval (selective block): an 8-byte start
/// plus an 8-byte size. ACK content grows in these 16-byte slots, so a
/// size-slot analysis of ACK datagrams would see 16-byte quantization;
/// the fitted-ack padding's extra jitter is drawn from `[0, this)` to
/// de-quantize the slots.
pub(crate) const ACK_INTERVAL_WIRE_SIZE: usize = 8 + 8;

fn encode_ack(wtr: &mut io::Cursor<&mut [u8]>, ack: AckInterval) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(ack.start.to_wire())
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u64::<BigEndian>(ack.size.get())
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn decode_ack(rdr: &mut io::Cursor<&[u8]>) -> Result<AckInterval, DecodeError> {
    let start = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let size = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    // A zero-size interval is invalid on the wire (and meaningless across
    // wrap): it is not a valid selective range.
    let size = NonZeroU64::new(size).ok_or(DecodeError::Corrupted)?;
    Ok(AckInterval {
        start: SequenceNumber::from_wire(start),
        size,
    })
}

pub const fn data_overhead() -> usize {
    let cmd = std::mem::size_of::<u8>();
    let seq = std::mem::size_of::<u64>();
    let send_ts = std::mem::size_of::<u32>();
    let len = std::mem::size_of::<u16>();
    cmd + seq + send_ts + len
}

fn encode_data(
    wtr: &mut io::Cursor<&mut [u8]>,
    seq: SequenceNumber,
    data: &[u8],
) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(seq.to_wire())
        .pipe(wrap_insufficient_buffer_size_err)?;
    let len =
        u16::try_from(data.len()).map_err(|_| EncodeError::PayloadTooLarge { len: data.len() })?;
    wtr.write_u16::<BigEndian>(len)
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_all(data)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn encode_data_ts(
    wtr: &mut io::Cursor<&mut [u8]>,
    seq: SequenceNumber,
    send_ts: u32,
    data: &[u8],
) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(seq.to_wire())
        .pipe(wrap_insufficient_buffer_size_err)?;
    wtr.write_u32::<BigEndian>(send_ts)
        .pipe(wrap_insufficient_buffer_size_err)?;
    let len =
        u16::try_from(data.len()).map_err(|_| EncodeError::PayloadTooLarge { len: data.len() })?;
    wtr.write_u16::<BigEndian>(len)
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
        seq: SequenceNumber::from_wire(seq),
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
        seq: SequenceNumber::from_wire(seq),
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
/// window (a cumulative next past the sent span, or a `{start:0,
/// size:u64::MAX}` selective block) with no retransmission.
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
    #[error("payload of {len} bytes exceeds the u16 wire length field")]
    PayloadTooLarge { len: usize },
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
    use super::{DecodeError, EncodeData, decode, encode_ack_data, encode_kill};
    use crate::ack::{AckHistory, EncodeAck};
    use crate::sequence::SequenceNumber;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    #[test]
    fn roundtrip_ack_echo_data() {
        let mut queue = AckHistory::new();
        for s in 10..15 {
            queue.insert(seq(s));
        }
        let ack = EncodeAck {
            queue: &queue,
            first_block_index: 0,
            max_blocks: 64,
        };
        let data = EncodeData {
            seq: seq(42),
            send_ts: Some(12_345),
            frame_len: None,
            data: b"hello",
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, Some(ack), Some(0xdead_beef), Some(data), &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, None).unwrap();
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, seq(10));
        assert_eq!(acks[0].size.get(), 5);
        assert_eq!(decoded.echo_ts, Some(0xdead_beef));
        let data = decoded.data.unwrap();
        assert_eq!(data.seq, seq(42));
        assert_eq!(data.send_ts, Some(12_345));
        assert_eq!(&buf[data.buf_range], b"hello");
    }

    #[test]
    fn ack_wire_preserves_cumulative_and_selective_ranges_across_wrap() {
        let mut queue = AckHistory::new_at(seq(u64::MAX - 1));
        for s in [u64::MAX - 1, u64::MAX, 0, 1, 3] {
            queue.insert(seq(s));
        }
        // Cumulative front advanced through u64::MAX, 0, 1; the hole at 2
        // leaves [3, 4) selective.
        assert_eq!(queue.next(), seq(2));
        let ack = EncodeAck {
            queue: &queue,
            first_block_index: 0,
            max_blocks: 64,
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, Some(ack), None, None, &mut buf).unwrap();
        let mut acks = Vec::new();
        let decoded = decode(&buf[..n], &mut acks, None).unwrap();
        assert_eq!(decoded.ack_next, Some(seq(2)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, seq(3));
        assert_eq!(acks[0].size.get(), 1);
    }

    #[test]
    fn ack_wire_rejects_more_than_the_protocol_block_bound() {
        // Hand-craft an ACK with a count above MAX_ACK_BLOCKS.
        let mut buf = vec![0u8]; // ACK_CMD
        buf.extend_from_slice(&7u64.to_be_bytes()); // cumulative next
        buf.push(crate::ack::MAX_ACK_BLOCKS as u8 + 1); // count above the bound
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
        // A second ACK_CMD in one datagram is rejected.
        let mut buf2 = vec![0u8];
        buf2.extend_from_slice(&7u64.to_be_bytes());
        buf2.push(0);
        buf2.push(0); // second ACK_CMD
        buf2.extend_from_slice(&8u64.to_be_bytes());
        buf2.push(0);
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf2, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
        // A zero-size interval is rejected.
        let mut buf3 = vec![0u8];
        buf3.extend_from_slice(&7u64.to_be_bytes());
        buf3.push(1);
        buf3.extend_from_slice(&9u64.to_be_bytes()); // start
        buf3.extend_from_slice(&0u64.to_be_bytes()); // size 0
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf3, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
        // A truncated interval is rejected.
        let mut buf4 = vec![0u8];
        buf4.extend_from_slice(&7u64.to_be_bytes());
        buf4.push(1);
        buf4.extend_from_slice(&9u64.to_be_bytes()); // start only, no size
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf4, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
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
        assert_eq!(decoded.ack_next, None, "a data-only packet has no ACK next");
        assert_eq!(decoded.echo_ts, None);
        let data = decoded.data.unwrap();
        assert_eq!(data.seq, seq(42));
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
        let mut queue = AckHistory::new();
        for s in 10..15 {
            queue.insert(seq(s));
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
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, seq(10));
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
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert!(!decoded.killed);
    }

    #[test]
    fn data_only_datagrams_need_no_tag_but_a_wrong_tag_is_rejected() {
        let tag = 0x1234_5678_9abc_def0;
        let data = EncodeData {
            seq: seq(7),
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
        assert_eq!(decoded.ack_next, None);
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

    #[test]
    fn a_padded_ack_decodes_with_the_same_ack_content() {
        let tag: u64 = 0x1234_5678_9abc_def0;
        let mut queue = AckHistory::new();
        for s in 10..15 {
            queue.insert(seq(s));
        }
        let ack = EncodeAck {
            queue: &queue,
            first_block_index: 0,
            max_blocks: 64,
        };
        let mut buf = vec![0u8; 256];
        let n = encode_ack_data(None, Some(ack), None, None, &mut buf).unwrap();
        let content = buf[..n].to_vec();
        // The padded form: the ACK content followed by an all-zero tail (the
        // fitted-ack fill). The padding rides INSIDE the datagram and must
        // be stripped at decode.
        let mut padded = content.clone();
        padded.extend_from_slice(&[0x00; 128]);
        let mut acks = Vec::new();
        let decoded = decode(&padded, &mut acks, None).unwrap();
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, seq(10));
        assert_eq!(acks[0].size.get(), 5);
        assert!(decoded.data.is_none());
        // The unpadded form decodes identically.
        let mut acks = Vec::new();
        let decoded = decode(&content, &mut acks, None).unwrap();
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].start, seq(10));
        assert_eq!(acks[0].size.get(), 5);
        // A TAGGED padded ACK also decodes (flush_acks pages carry the tag
        // on handshaked connections).
        let mut tagged = vec![6u8];
        tagged.extend_from_slice(&tag.to_be_bytes());
        tagged.extend_from_slice(&content);
        tagged.extend_from_slice(&[0x00; 64]);
        let mut acks = Vec::new();
        let decoded = decode(&tagged, &mut acks, Some(tag)).unwrap();
        assert_eq!(decoded.ack_next, Some(seq(0)));
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].size.get(), 5);
    }

    #[test]
    fn a_nonzero_tail_after_an_ack_is_corrupted() {
        // [ACK content][0x00 — second ACK_CMD: enters padding mode]
        // [0x00 x 3][0xAB] — a nonzero byte in the tail is rejected.
        let mut buf = vec![0u8];
        buf.extend_from_slice(&7u64.to_be_bytes());
        buf.push(0); // count 0
        buf.push(0); // second ACK_CMD → padding mode
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0xAB]);
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
        // The same tail with NO zero padding start byte (a nonzero byte read
        // as a command) is Corrupted too.
        let mut buf2 = vec![0u8];
        buf2.extend_from_slice(&7u64.to_be_bytes());
        buf2.push(0);
        buf2.push(0xAB); // unknown command after the ACK
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf2, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
    }

    #[test]
    fn an_all_zero_tail_without_a_complete_ack_is_corrupted() {
        // The zero-tail rule ONLY fires after a complete ACK_CMD. A data- or
        // echo-only datagram whose trailing zeros would otherwise look like
        // padding stays Corrupted (today's behavior).
        //
        // Data-only: DATA_CMD + all-zero body cut off before the length
        // field completes.
        let mut data = vec![1u8]; // DATA_CMD
        data.extend_from_slice(&[0x00; 8]); // seq
        data.push(0x00); // truncated u16 length field
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&data, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
        // Echo-only: ECHO_TS_CMD + timestamp + an all-zero tail that
        // truncates the ACK parse (no count byte), so the padding rule must
        // not rescue it.
        let mut echo = vec![4u8]; // ECHO_TS_CMD
        echo.extend_from_slice(&0u32.to_be_bytes()); // ts
        echo.extend_from_slice(&[0x00; 9]); // 0x00 (ACK_CMD) + 8 zero bytes, no count
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&echo, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
    }

    #[test]
    fn a_zero_tail_containing_a_nonzero_byte_is_corrupted() {
        // A valid ACK followed by padding that mixes zeros and a nonzero
        // byte is rejected — the fill must be exactly zeros.
        let mut buf = vec![0u8];
        buf.extend_from_slice(&3u64.to_be_bytes());
        buf.push(0); // count 0
        buf.push(0); // second ACK_CMD → padding mode
        buf.extend_from_slice(&[0x00, 0x00, 0x07, 0x00, 0x00]);
        let mut acks = Vec::new();
        assert!(matches!(
            decode(&buf, &mut acks, None),
            Err(DecodeError::Corrupted)
        ));
    }
}
