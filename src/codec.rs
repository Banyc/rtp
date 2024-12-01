use core::num::NonZeroU64;
use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;
use thiserror::Error;

use crate::sack::{AckBall, AckQueue};

const ACK_CMD: u8 = 0;
const DATA_CMD: u8 = 1;
const KILL_CMD: u8 = 2;

pub fn in_cmd_space(buf: u8) -> bool {
    matches!(buf, ACK_CMD | DATA_CMD | KILL_CMD)
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
    if let Some(EncodeData { seq, data }) = data {
        wtr.write_u8(DATA_CMD)
            .pipe(wrap_insufficient_buffer_size_err)?;
        encode_data(&mut wtr, seq, data)?;
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
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct Decoded {
    pub data: Option<DecodedDataPkt>,
    /// broken pipe
    pub killed: bool,
}
pub fn decode(buf: &[u8], ack: &mut Vec<AckBall>) -> Result<Decoded, DecodeError> {
    let mut killed = false;
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
                    killed,
                });
            }
            KILL_CMD => {
                killed = true;
            }
            _ => return Err(DecodeError::Corrupted),
        }
    }
    Ok(Decoded { data: None, killed })
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
    let len = std::mem::size_of::<u16>();
    cmd + seq + len
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
        buf_range: start..end,
    })
}
#[derive(Debug, Clone)]
pub struct DecodedDataPkt {
    pub seq: u64,
    pub buf_range: std::ops::Range<usize>,
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
