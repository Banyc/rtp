use std::{
    io,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use fec::{de::FecDecoder, en::FecEncoder};
use tokio::sync::{mpsc, oneshot};

use crate::transmission_layer::{UnreliableRead, UnreliableWrite};

const WINDOW_SIZE: NonZeroU64 = NonZeroU64::new(32).unwrap();
const MAX_GROUP_SIZE: usize = DATA_PARITY_RATIO.len() + *DATA_PARITY_RATIO.last().unwrap() as usize;
const DATA_PARITY_RATIO: &[u8] = &[
    0, 3, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
];

#[derive(Debug, Clone)]
pub struct FecReaderConfig {
    pub symbol_size: usize,
}
#[derive(Debug)]
pub struct FecReader<R> {
    utp: R,
    fec_decoder: FecDecoder,
    recovered: Vec<Vec<u8>>,
    buf: Vec<u8>,
}
impl<R: UnreliableRead> FecReader<R> {
    pub fn new(utp: R, config: FecReaderConfig) -> Self {
        let fec_decoder = FecDecoder::builder()
            .max_group_size(MAX_GROUP_SIZE)
            .symbol_size(config.symbol_size)
            .window_size(WINDOW_SIZE)
            .build();
        Self {
            utp,
            fec_decoder,
            recovered: vec![],
            buf: vec![0; config.symbol_size * 2],
        }
    }
    fn pop_recovered(&mut self, buf: &mut [u8]) -> Option<usize> {
        let data = self.recovered.pop()?;
        Some(max_copy(&data, buf))
    }
    fn on_utp_read(&mut self, buf: &mut [u8], pkt_len: usize) -> Result<usize, std::io::ErrorKind> {
        let pkt = &self.buf[..pkt_len];
        let hdr_len = self.fec_decoder.decode(pkt, |data| {
            self.recovered.push(data.to_vec());
        });
        if let Some(hdr_len) = hdr_len {
            let data = &pkt[hdr_len..];
            return Ok(max_copy(data, buf));
        }
        if let Some(n) = self.pop_recovered(buf) {
            return Ok(n);
        }
        Err(io::ErrorKind::WouldBlock)
    }
}
#[async_trait]
impl<R: UnreliableRead> UnreliableRead for FecReader<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if let Some(n) = self.pop_recovered(buf) {
            return Ok(n);
        }
        let pkt_len = self.utp.try_recv(&mut self.buf)?;
        self.on_utp_read(buf, pkt_len)
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if let Some(n) = self.pop_recovered(buf) {
            return Ok(n);
        }
        loop {
            let pkt_len = self.utp.recv(&mut self.buf).await?;
            match self.on_utp_read(buf, pkt_len) {
                Ok(n) => return Ok(n),
                Err(io::ErrorKind::WouldBlock) => continue,
                Err(e) => return Err(e),
            };
        }
    }
}

fn max_copy(from: &[u8], to: &mut [u8]) -> usize {
    let len = from.len().min(to.len());
    to[..len].copy_from_slice(&from[..len]);
    len
}

#[derive(Debug, Clone)]
pub struct FecWriterConfig {
    pub parity_delay: Duration,
    pub symbol_size: usize,
}
#[derive(Debug)]
pub struct FecWriter {
    data_tx: mpsc::Sender<Vec<u8>>,
    err_rx: SetOnce<io::ErrorKind>,
}
impl FecWriter {
    pub fn new<W: UnreliableWrite>(utp: W, config: FecWriterConfig) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let (err_rx, err_tx) = SetOnce::new();
        tokio::spawn(async move {
            run_writer(rx, utp, config, err_tx).await;
        });
        Self {
            data_tx: tx,
            err_rx,
        }
    }
}
#[async_trait]
impl UnreliableWrite for FecWriter {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, io::ErrorKind> {
        let len = buf.len();
        if self.data_tx.send(buf.to_vec()).await.is_err() {
            let err = self.err_rx.recv().await.unwrap();
            return Err(*err);
        }
        Ok(len)
    }
}

async fn run_writer<W>(
    mut data_rx: mpsc::Receiver<Vec<u8>>,
    utp: W,
    config: FecWriterConfig,
    ret_err: oneshot::Sender<io::ErrorKind>,
) where
    W: UnreliableWrite,
{
    let fec_encoder = FecEncoder::builder()
        .symbol_size(config.symbol_size)
        .build();
    let mut state = WriterState {
        fec_encoder,
        next_flush: Instant::now() + config.parity_delay,
        flush_delay: config.parity_delay,
        buf: vec![0; config.symbol_size * 2],
        utp,
        pause: true,
    };
    let err = loop {
        tokio::select! {
            () = tokio::time::sleep_until(state.next_flush.into()) => {
                if let Err(e) = state.send(None).await {
                    break e;
                };
            }
            res = data_rx.recv() => {
                let Some(data) = res else {
                    return;
                };
                if let Err(e) = state.send(Some(data.as_ref())).await {
                    break e;
                };
            }
        }
    };
    let _ = ret_err.send(err);
}
#[derive(Debug)]
struct WriterState<W: UnreliableWrite> {
    pub fec_encoder: FecEncoder,
    pub next_flush: Instant,
    pub flush_delay: Duration,
    pub buf: Vec<u8>,
    pub utp: W,
    pub pause: bool,
}
impl<W: UnreliableWrite> WriterState<W> {
    pub async fn send(&mut self, data: Option<&[u8]>) -> Result<(), io::ErrorKind> {
        if let Some(data) = data {
            let full = self.fec_encoder.group_data_count() + 1 == DATA_PARITY_RATIO.len();
            if full {
                self.pause = true;
                self.fec_encoder.skip_group();
                return Ok(());
            }
            let n = self.fec_encoder.encode_data(data, &mut self.buf);
            self.utp.send(&self.buf[..n]).await?;
        } else {
            if self.pause {
                self.pause = false;
                return Ok(());
            }
            self.flush_parities().await?;
        }
        Ok(())
    }
    pub async fn flush_parities(&mut self) -> Result<(), io::ErrorKind> {
        let no_data = self.fec_encoder.group_data_count() == 0;
        if no_data {
            return Ok(());
        }
        let mut parity_encoder = self
            .fec_encoder
            .flush_parities(DATA_PARITY_RATIO[self.fec_encoder.group_data_count()]);
        while let Some(n) = parity_encoder.encode_parity(&mut self.buf) {
            let buf = &self.buf[..n];
            self.utp.send(buf).await?;
        }
        self.next_flush = Instant::now() + self.flush_delay;
        Ok(())
    }
}

#[derive(Debug)]
struct SetOnce<T> {
    recv: Option<oneshot::Receiver<T>>,
    recved: Option<T>,
}
impl<T> SetOnce<T> {
    pub fn new() -> (Self, oneshot::Sender<T>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                recv: Some(rx),
                recved: None,
            },
            tx,
        )
    }
    pub async fn recv(&mut self) -> Option<&T> {
        if let Some(rx) = self.recv.take() {
            let a = rx.await.ok()?;
            self.recved = Some(a);
        }
        self.recved.as_ref()
    }
}
