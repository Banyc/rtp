use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tap::Pipe;
use thiserror::Error;

const ACK_CMD: u8 = 0;
const DATA_CMD: u8 = 1;
const KILL_CMD: u8 = 2;

pub fn encode_kill(buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    wtr.write_u8(KILL_CMD)
        .pipe(wrap_insufficient_buffer_size_err)?;
    let pos = wtr.position();
    Ok(pos as usize)
}

pub fn encode_ack_data(
    ack: &[u64],
    data: Option<EncodeData<'_>>,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let mut wtr = io::Cursor::new(buf);
    for ack in ack {
        wtr.write_u8(ACK_CMD)
            .pipe(wrap_insufficient_buffer_size_err)?;
        encode_ack(&mut wtr, *ack)?;
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
pub struct EncodeData<'a> {
    pub seq: u64,
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct Decoded {
    pub data: Option<DecodedDataPacket>,
    /// broken pipe
    pub killed: bool,
}
pub fn decode(buf: &[u8], ack: &mut Vec<u64>) -> Result<Decoded, DecodeError> {
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

fn encode_ack(wtr: &mut io::Cursor<&mut [u8]>, ack: u64) -> Result<(), EncodeError> {
    wtr.write_u64::<BigEndian>(ack)
        .pipe(wrap_insufficient_buffer_size_err)?;
    Ok(())
}

fn decode_ack(rdr: &mut io::Cursor<&[u8]>) -> Result<u64, DecodeError> {
    let ack = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    Ok(ack)
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

fn decode_data(rdr: &mut io::Cursor<&[u8]>) -> Result<DecodedDataPacket, DecodeError> {
    let seq = rdr.read_u64::<BigEndian>().pipe(wrap_corrupted_err)?;
    let len = rdr.read_u16::<BigEndian>().pipe(wrap_corrupted_err)?;
    let end = usize::try_from(rdr.position()).unwrap() + usize::from(len);
    if rdr.get_ref().len() < end {
        return Err(DecodeError::Corrupted);
    }
    let start = rdr.position() as usize;
    Ok(DecodedDataPacket {
        seq,
        buf_range: start..end,
    })
}
#[derive(Debug, Clone)]
pub struct DecodedDataPacket {
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
