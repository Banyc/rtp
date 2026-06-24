use std::{
    collections::VecDeque,
    io,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use fec::{de::FecDecoder, en::FecEncoder};
use tokio::sync::{mpsc, oneshot};

use crate::transmission_layer::{UnreliableRead, UnreliableWrite};
use crate::reliable_layer::CC_DATA_LOSS_RATE;

const FEC_DEBUG: bool = false;

const WINDOW_SIZE: NonZeroU64 = NonZeroU64::new(32).unwrap();
const MAX_GROUP_SIZE: usize = MAX_DATA_PER_GROUP + MAX_PARITY_PER_GROUP;
/// Maximum data symbols accumulated before a group is forcibly flushed.
const MAX_DATA_PER_GROUP: usize = 20;
/// Parity overhead target: ~25% (1 parity per 4 data), at least 1 per group.
const PARITY_RATIO_NUM: usize = 1;
const PARITY_RATIO_DEN: usize = 4;
const MAX_PARITY_PER_GROUP: usize =
    (MAX_DATA_PER_GROUP * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
/// Groups with at most this many data symbols get parity protection.
/// Larger groups skip parity to avoid impacting throughput of big traffic.
const PARITY_DATA_THRESHOLD: usize = 4;

/// Loss-rate feedback pushed from the reliable layer into the FEC writer so
/// that parity aggressiveness can adapt to the channel the RTP control loop
/// is actually observing.
///
/// The reliable layer is the only component that can measure end-to-end loss
/// (via SACKs / RTOs), so coupling FEC to that signal makes FEC complement
/// retransmission instead of working blind:
///   - high loss  -> more parity, recover losses before RTO/backoff kicks in
///   - low loss   -> skip parity, avoid wasting bandwidth on a clean channel
#[async_trait]
pub trait FecController: core::fmt::Debug + Sync + Send + 'static {
    async fn set_loss_rate(&self, loss_rate: Option<f64>);
    async fn get_loss_rate(&self) -> Option<f64>;
}
#[derive(Debug, Default)]
struct LossRateStore {
    rate: tokio::sync::RwLock<Option<f64>>,
}
#[async_trait]
impl FecController for LossRateStore {
    async fn set_loss_rate(&self, loss_rate: Option<f64>) {
        *self.rate.write().await = loss_rate;
    }
    async fn get_loss_rate(&self) -> Option<f64> {
        *self.rate.read().await
    }
}

#[derive(Debug, Clone)]
pub struct FecReaderConfig {
    pub symbol_size: usize,
}
#[derive(Debug)]
pub struct FecReader<R> {
    utp: R,
    fec_decoder: FecDecoder,
    recovered: VecDeque<Vec<u8>>,
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
            recovered: VecDeque::new(),
            buf: vec![0; config.symbol_size * 2],
        }
    }
    fn pop_recovered(&mut self, buf: &mut [u8]) -> Option<usize> {
        let data = self.recovered.pop_front()?;
        Some(max_copy(&data, buf))
    }
    fn on_utp_read(&mut self, buf: &mut [u8], pkt_len: usize) -> Result<usize, std::io::ErrorKind> {
        let pkt = &self.buf[..pkt_len];
        let before = self.recovered.len();
        let hdr_len = self.fec_decoder.decode(pkt, |data| {
            self.recovered.push_back(data.to_vec());
        });
        if FEC_DEBUG {
            let kind = if hdr_len.is_some() {
                "data"
            } else {
                "parity/none"
            };
            eprintln!(
                "FEC reader: kind={kind} pkt_len={pkt_len} hdr_len={hdr_len:?} recovered_before={before} recovered_after={}",
                self.recovered.len()
            );
        }
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
    data_tx: mpsc::Sender<FecWriterMsg>,
    err_rx: SetOnce<io::ErrorKind>,
    controller: Arc<dyn FecController>,
}
impl FecWriter {
    pub fn new<W: UnreliableWrite>(utp: W, config: FecWriterConfig) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        let (err_rx, err_tx) = SetOnce::new();
        let controller = Arc::new(LossRateStore::default());
        let writer_controller = Arc::clone(&controller);
        tokio::spawn(async move {
            run_writer(rx, utp, config, err_tx, writer_controller).await;
        });
        Self {
            data_tx: tx,
            err_rx,
            controller,
        }
    }
    pub fn controller(&self) -> Arc<dyn FecController> {
        Arc::clone(&self.controller)
    }
}
#[async_trait]
impl UnreliableWrite for FecWriter {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, io::ErrorKind> {
        let len = buf.len();
        if self.data_tx.send(FecWriterMsg::Data(buf.to_vec())).await.is_err() {
            let err = self
                .err_rx
                .recv()
                .await
                .copied()
                .unwrap_or(io::ErrorKind::BrokenPipe);
            return Err(err);
        }
        Ok(len)
    }
}

#[derive(Debug)]
enum FecWriterMsg {
    Data(Vec<u8>),
}

async fn run_writer<W>(
    mut data_rx: mpsc::Receiver<FecWriterMsg>,
    utp: W,
    config: FecWriterConfig,
    ret_err: oneshot::Sender<io::ErrorKind>,
    controller: Arc<dyn FecController>,
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
        controller,
        loss_rate: None,
    };
    let err = loop {
        tokio::select! {
            () = tokio::time::sleep_until(state.next_flush.into()) => {
                state.refresh_loss_rate().await;
                if let Err(e) = state.send(None).await {
                    break e;
                };
            }
            res = data_rx.recv() => {
                let Some(FecWriterMsg::Data(data)) = res else {
                    return;
                };
                state.refresh_loss_rate().await;
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
    pub controller: Arc<dyn FecController>,
    /// Most recent loss rate sampled from the reliable layer, in `[0, 1]`.
    pub loss_rate: Option<f64>,
}
impl<W: UnreliableWrite> WriterState<W> {
    async fn refresh_loss_rate(&mut self) {
        let rate = self.controller.get_loss_rate().await;
        self.loss_rate = rate;
    }
    pub async fn send(&mut self, data: Option<&[u8]>) -> Result<(), io::ErrorKind> {
        if let Some(data) = data {
            let full = self.fec_encoder.group_data_count() + 1 > MAX_DATA_PER_GROUP;
            if full {
                if FEC_DEBUG {
                    eprintln!("FEC writer: group full, flushing parities");
                }
                self.flush_parities().await?;
            }
            let n = self.fec_encoder.encode_data(data, &mut self.buf);
            if FEC_DEBUG {
                eprintln!(
                    "FEC writer: encode_data group_data_count={} n={}",
                    self.fec_encoder.group_data_count(),
                    n
                );
            }
            self.utp.send(&self.buf[..n]).await?;
        } else {
            if FEC_DEBUG {
                eprintln!(
                    "FEC writer: flush tick, group_data_count={} loss_rate={:?}",
                    self.fec_encoder.group_data_count(),
                    self.loss_rate
                );
            }
            self.flush_parities().await?;
        }
        Ok(())
    }
    pub async fn flush_parities(&mut self) -> Result<(), io::ErrorKind> {
        let data_count = self.fec_encoder.group_data_count();
        self.next_flush = Instant::now() + self.flush_delay;
        if data_count == 0 {
            return Ok(());
        }
        // Only send parities for small groups to protect small/control messages.
        // Large groups skip parity to avoid impacting throughput of big traffic.
        if data_count > PARITY_DATA_THRESHOLD {
            self.fec_encoder.skip_group();
            return Ok(());
        }
        let parity_count = parity_for(data_count, self.loss_rate);
        let mut parity_encoder = self.fec_encoder.flush_parities(parity_count);
        while let Some(n) = parity_encoder.encode_parity(&mut self.buf) {
            let buf = &self.buf[..n];
            self.utp.send(buf).await?;
        }
        Ok(())
    }
}

/// Parity count for a group of `data_count` data symbols, scaled by the
/// observed loss rate. With no loss signal we fall back to the static 1:4
/// ratio; when the reliable layer reports high loss we add proportionally
/// more parity so FEC can recover losses before retransmission kicks in.
fn parity_for(data_count: usize, loss_rate: Option<f64>) -> u8 {
    let base = (data_count * PARITY_RATIO_NUM).div_ceil(PARITY_RATIO_DEN);
    let scale = loss_rate.map(parity_scale).unwrap_or(1.0);
    let p = (base as f64 * scale).round() as usize;
    p.clamp(1, MAX_PARITY_PER_GROUP).try_into().unwrap()
}

/// Multiplier applied to the base parity count as a function of the observed
/// loss rate. Stays at 1x while the channel is healthy and grows up to 4x as
/// loss approaches the reliable layer's backoff threshold, capping out to
/// avoid runaway overhead.
fn parity_scale(loss_rate: f64) -> f64 {
    if loss_rate <= 0.02 {
        return 1.0;
    }
    const MAX_SCALE: f64 = 4.0;
    let t = (loss_rate / CC_DATA_LOSS_RATE).clamp(0.0, 1.0);
    1.0 + (MAX_SCALE - 1.0) * t
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
