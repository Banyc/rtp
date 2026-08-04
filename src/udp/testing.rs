//! Test-only utilities for simulating packet loss without OS-level network
//! shaping. Compiled only under `test` builds so production code is completely
//! unaffected — there is no global drop flag on the production
//! `UnreliableRead`/`UnreliableWrite` impls.
//!
//! Loss is per-instance, not global: each test creates a [`LossRate`] and
//! injects it into the wrappers it wants to be lossy, so tests never interfere
//! with each other.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;

/// A toggable loss rate in basis points (0–10_000), owned by a single test
/// and shared (via `Arc`) between the read and write wrappers of one
/// connection. 0 means no loss; 10_000 means drop every packet.
///
/// Create one per test with [`LossRate::new`] and pass clones to
/// [`LossyRead::new`] / [`LossyWrite::new`].
#[derive(Debug, Clone)]
pub struct LossRate(Arc<AtomicUsize>);

impl LossRate {
    /// New loss rate of `bps` basis points (500 = 5%). Clamped to
    /// `[0, 10_000]`.
    pub fn new(bps: usize) -> Self {
        Self(Arc::new(AtomicUsize::new(bps.min(10_000))))
    }

    /// Set the loss rate to `bps` basis points. Clamped to
    /// `[0, 10_000]`.
    pub fn set(&self, bps: usize) {
        self.0.store(bps.min(10_000), Ordering::Relaxed);
    }

    /// Current loss rate in basis points.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    /// Returns `true` with probability `bps / 10_000`.
    fn roll(&self) -> bool {
        let bps = self.0.load(Ordering::Relaxed);
        bps > 0 && rand::random::<u32>() % 10_000 < bps as u32
    }
}

/// Wrapper around any `UnreliableRead` that drops a fraction of received
/// packets per the injected [`LossRate`]. Dropped packets are skipped (recv
/// keeps waiting for the next one); `try_recv` reports `WouldBlock`.
#[derive(Debug)]
pub struct LossyRead<R: UnreliableRead> {
    inner: R,
    rate: LossRate,
}

impl<R: UnreliableRead> LossyRead<R> {
    pub fn new(read: R, rate: LossRate) -> Self {
        Self { inner: read, rate }
    }
}

#[async_trait]
impl<R: UnreliableRead + Send + Sync + 'static> UnreliableRead for LossyRead<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        let n = self.inner.try_recv(buf)?;
        if self.rate.roll() {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        Ok(n)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            let n = self.inner.recv(buf).await?;
            if !self.rate.roll() {
                return Ok(n);
            }
        }
    }
}

/// Wrapper around any `UnreliableWrite` that drops a fraction of sent
/// packets per the injected [`LossRate`]. A dropped send reports success
/// (the data is "written" then silently discarded), simulating a packet
/// lost in flight after the sender's kernel has accepted it.
#[derive(Debug)]
pub struct LossyWrite<W: UnreliableWrite> {
    inner: W,
    rate: LossRate,
}

impl<W: UnreliableWrite> LossyWrite<W> {
    pub fn new(write: W, rate: LossRate) -> Self {
        Self { inner: write, rate }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for LossyWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        if self.rate.roll() {
            return Ok(buf.len());
        }
        self.inner.send(buf).await
    }
}

#[derive(Debug, Clone)]
pub struct ImpairRate {
    pub loss: LossRate,
    pub reorder: LossRate,
    pub duplicate: LossRate,
    applied: Arc<[AtomicUsize; 3]>,
}

impl ImpairRate {
    pub fn new(loss_bps: usize, reorder_bps: usize, duplicate_bps: usize) -> Self {
        Self {
            loss: LossRate::new(loss_bps),
            reorder: LossRate::new(reorder_bps),
            duplicate: LossRate::new(duplicate_bps),
            applied: Arc::new([const { AtomicUsize::new(0) }; 3]),
        }
    }

    pub fn applied(&self) -> (usize, usize, usize) {
        let n = |i: usize| self.applied[i].load(Ordering::Relaxed);
        (n(0), n(1), n(2))
    }

    fn record(&self, i: usize) {
        self.applied[i].fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ImpairedWrite<W: UnreliableWrite> {
    inner: W,
    rate: ImpairRate,
    held: Option<Vec<u8>>,
}

impl<W: UnreliableWrite> ImpairedWrite<W> {
    pub fn new(write: W, rate: ImpairRate) -> Self {
        Self {
            inner: write,
            rate,
            held: None,
        }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for ImpairedWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        if self.rate.loss.roll() {
            self.rate.record(0);
            return Ok(buf.len());
        }
        if let Some(held) = self.held.take() {
            self.inner.send(buf).await?;
            self.inner.send(&held).await?;
            return Ok(buf.len());
        }
        if self.rate.reorder.roll() {
            self.rate.record(1);
            self.held = Some(buf.to_vec());
            return Ok(buf.len());
        }
        let n = self.inner.send(buf).await?;
        if self.rate.duplicate.roll() {
            self.rate.record(2);
            self.inner.send(buf).await?;
            self.inner.send(buf).await?;
            return Ok(buf.len());
        }
        Ok(n)
    }
}

pub fn wrap_fec_impaired<R, W>(read: R, write: W, fec: bool, rate: ImpairRate) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    let (mss, fec_state, tuning) = checked_mss_and_fec(
        fec,
        ValidMss::try_new(NO_FEC_MSS).unwrap(),
        fec_tuning_from_env(),
        FrameDelivery::default(),
    )
    .unwrap();
    UnreliableLayer {
        utp_read: Box::new(read),
        utp_write: Box::new(ImpairedWrite::new(write, rate)),
        post_open_handshake: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery: FrameDelivery::default(),
        rtx_dup: false,
        instream_group_fec: false,
    }
}

/// Like `wrap_fec` but wraps the read/write pair in lossy injectors driven
/// by `rate`. Each connection should get its own `LossRate` (or a shared
/// one if you want both directions of a single link to share loss state).
pub fn wrap_fec_lossy<R, W>(read: R, write: W, fec: bool, rate: LossRate) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    wrap_fec_lossy_with_mss(read, write, fec, NO_FEC_MSS, rate)
}

pub fn wrap_fec_lossy_with_mss<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    rate: LossRate,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    wrap_fec_lossy_with_mss_and_fec_tuning(read, write, fec, mss, fec_tuning_from_env(), rate)
}

/// Like `wrap_fec_lossy_with_mss` but takes an explicit `FecTuning` and
/// threads it through the same `checked_mss_and_fec` /
/// `wrap_fec_with_mss_and_fec_tuning` construction path production uses.
/// Only the lossy read/write injection differs from production; the FEC
/// state, MSS normalisation, and tuning clamping are identical, so a
/// regression that silently disables FEC at a non-default MSS is caught.
pub fn wrap_fec_lossy_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    rate: LossRate,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    let (mss, fec_state, tuning) = checked_mss_and_fec(
        fec,
        ValidMss::try_new(mss).unwrap(),
        tuning,
        FrameDelivery::default(),
    )
    .unwrap();
    UnreliableLayer {
        utp_read: Box::new(LossyRead::new(read, rate.clone())),
        utp_write: Box::new(LossyWrite::new(write, rate)),
        post_open_handshake: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery: FrameDelivery::default(),
        rtx_dup: false,
        instream_group_fec: false,
    }
}
