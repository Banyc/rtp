//! Test-only utilities for simulating packet loss without OS-level network
//! shaping. Compiled only under `test` builds so production code is completely
//! unaffected — there is no global drop flag on the production
//! `UnreliableRead`/`UnreliableWrite` impls.
//!
//! Loss is per-instance, not global: each test creates a [`BasisPoints`] and
//! injects it into the wrappers it wants to be lossy, so tests never interfere
//! with each other.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use super::*;
use crate::obfuscate::padding::AckPaddingMode;

/// Fixed seed for the iid loss stream of every [`BasisPoints`] created
/// through [`BasisPoints::new`]. The stream is a pure function of this seed
/// and the number of rolls, so an impaired run no longer depends on the OS
/// RNG: the realized loss rate and the recovered-symbol count are stable
/// across runs instead of probabilistic.
const DEFAULT_LOSS_SEED: u64 = 0x5EED_1055;

/// Distinct seeds for the three roles of [`ImpairRate`]. Loss, reordering
/// and duplication must draw from independent streams; sharing one seed
/// would make their draws at each send identical, so a duplication rate
/// below the loss and reorder rates could never fire.
const IMPAIR_LOSS_SEED: u64 = DEFAULT_LOSS_SEED;
const IMPAIR_REORDER_SEED: u64 = DEFAULT_LOSS_SEED ^ 0x1111_1111_1111_1111;
const IMPAIR_DUPLICATE_SEED: u64 = DEFAULT_LOSS_SEED ^ 0x2222_2222_2222_2222;

/// A toggable rate in basis points (0–10_000), owned by a single test
/// and shared (via `Arc`) between the read and write wrappers of one
/// connection. 0 means no impairment; 10_000 means affect every packet.
///
/// Create one per test with [`BasisPoints::new`] and pass clones to
/// [`LossyRead::new`] / [`LossyWrite::new`].
#[derive(Debug, Clone)]
pub struct BasisPoints {
    bps: Arc<AtomicUsize>,
    rng: Arc<Mutex<netem_test::RndState>>,
}

impl BasisPoints {
    /// New rate of `bps` basis points (500 = 5%). Clamped to
    /// `[0, 10_000]`.
    pub fn new(bps: usize) -> Self {
        Self::with_seed(bps, DEFAULT_LOSS_SEED)
    }

    /// Rate `bps` drawn from a stream seeded with `seed`. The seed fixes the
    /// draw sequence, so the drops are reproducible from the seed and the
    /// roll count rather than sampled from the OS RNG. Callers that place
    /// several impairments side by side must give each a distinct seed.
    fn with_seed(bps: usize, seed: u64) -> Self {
        Self {
            bps: Arc::new(AtomicUsize::new(bps.min(10_000))),
            rng: Arc::new(Mutex::new(netem_test::RndState::seed(seed))),
        }
    }

    /// Set the rate to `bps` basis points. Clamped to
    /// `[0, 10_000]`.
    pub fn set(&self, bps: usize) {
        self.bps.store(bps.min(10_000), Ordering::Relaxed);
    }

    /// Current rate in basis points.
    pub fn get(&self) -> usize {
        self.bps.load(Ordering::Relaxed)
    }

    /// Returns `true` with probability `bps / 10_000`. A zero rate draws
    /// nothing, so an inert impairment never advances its stream.
    fn roll(&self) -> bool {
        let bps = self.bps.load(Ordering::Relaxed);
        bps > 0 && self.rng.lock().unwrap().next_u32() % 10_000 < bps as u32
    }
}

/// Wrapper around any `UnreliableRead` that drops a fraction of received
/// packets per the injected [`BasisPoints`]. Dropped packets are skipped (recv
/// keeps waiting for the next one); `try_recv` reports `WouldBlock`.
#[derive(Debug)]
pub struct LossyRead<R: UnreliableRead> {
    inner: R,
    rate: BasisPoints,
}

impl<R: UnreliableRead> LossyRead<R> {
    pub fn new(read: R, rate: BasisPoints) -> Self {
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
/// packets per the injected [`BasisPoints`]. A dropped send reports success
/// (the data is "written" then silently discarded), simulating a packet
/// lost in flight after the sender's kernel has accepted it.
#[derive(Debug)]
pub struct LossyWrite<W: UnreliableWrite> {
    inner: W,
    rate: BasisPoints,
}

impl<W: UnreliableWrite> LossyWrite<W> {
    pub fn new(write: W, rate: BasisPoints) -> Self {
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

/// Deterministic burst-loss injector: drops runs of `burst` consecutive
/// packets separated by a pseudo-random quiet gap in `[quiet_min,
/// quiet_max]` packets. Unlike [`BasisPoints`] (iid loss), the drops are
/// adjacent, so a burst can wipe a whole redundancy group (primary + armor
/// copies + the parity that trails the same burst) instead of a single copy.
/// The randomized gap keeps the burst from aliasing onto a fixed phase of the
/// message cadence, and the seeded LCG makes a run reproducible.
#[derive(Debug, Clone)]
pub struct BurstLoss(Arc<BurstLossState>);

#[derive(Debug)]
struct BurstLossState {
    burst: u64,
    quiet_min: u64,
    quiet_max: u64,
    rng: AtomicU64,
    remaining_burst: AtomicU64,
    remaining_quiet: AtomicU64,
    dropped: AtomicU64,
}

impl BurstLoss {
    /// Drop `burst` consecutive packets, then forward a random gap in
    /// `[quiet_min, quiet_max]` packets before the next burst. `burst == 0`
    /// never drops (a clean link, useful as the delay-only control).
    pub fn new(burst: usize, quiet_min: usize, quiet_max: usize, seed: u64) -> Self {
        assert!(quiet_min <= quiet_max, "quiet gap range must be ordered");
        Self(Arc::new(BurstLossState {
            burst: burst as u64,
            quiet_min: quiet_min as u64,
            quiet_max: quiet_max as u64,
            // Any non-zero seed avoids the fixed point of the LCG step.
            rng: AtomicU64::new(seed | 1),
            remaining_burst: AtomicU64::new(0),
            remaining_quiet: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }))
    }

    /// Packets dropped so far.
    pub fn dropped(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }

    /// SplitMix64: one step per quiet-gap resample, so a run is reproducible
    /// from the seed regardless of thread interleaving.
    fn next_gap(&self) -> u64 {
        let mut z = self
            .0
            .rng
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn roll(&self) -> bool {
        if self.0.remaining_burst.load(Ordering::Relaxed) == 0
            && self.0.remaining_quiet.load(Ordering::Relaxed) == 0
        {
            self.0
                .remaining_burst
                .store(self.0.burst, Ordering::Relaxed);
        }
        let in_burst = self.0.remaining_burst.load(Ordering::Relaxed);
        if in_burst > 0 {
            self.0
                .remaining_burst
                .store(in_burst - 1, Ordering::Relaxed);
            if in_burst == 1 {
                let span = self.0.quiet_max - self.0.quiet_min + 1;
                let gap = self.0.quiet_min + self.next_gap() % span;
                self.0.remaining_quiet.store(gap, Ordering::Relaxed);
            }
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let quiet = self.0.remaining_quiet.load(Ordering::Relaxed);
        if quiet > 0 {
            self.0.remaining_quiet.store(quiet - 1, Ordering::Relaxed);
        }
        false
    }
}

/// Wrapper around any `UnreliableWrite` that drops runs of packets per the
/// injected [`BurstLoss`]. A dropped send reports success, modelling a packet
/// lost in flight after the sender's kernel accepted it.
#[derive(Debug)]
pub struct BurstLossyWrite<W: UnreliableWrite> {
    inner: W,
    loss: BurstLoss,
}

impl<W: UnreliableWrite> BurstLossyWrite<W> {
    pub fn new(write: W, loss: BurstLoss) -> Self {
        Self { inner: write, loss }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for BurstLossyWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        if self.loss.roll() {
            return Ok(buf.len());
        }
        self.inner.send(buf).await
    }
}

/// Wrapper around any `UnreliableRead` that holds every datagram for a fixed
/// one-way delay before delivering it. `try_recv` enqueues whatever the inner
/// transport has and releases only ripe datagrams; `recv` polls the delay line
/// until the oldest datagram is due. This models a WAN propagation delay so an
/// ARQ fall-through shows up as a `reorder_window + one round trip` tail,
/// instead of the sub-millisecond loopback floor.
///
/// `sch_netem`'s `delay TIME JITTER` impairment: a fixed one-way propagation
/// delay, the per-packet jitter half-width, and the seed its draw is taken
/// from.  With `jitter == ZERO` the draw is skipped entirely and the delay is
/// exactly `delay`, so a jitter-free arm is byte-for-byte what it was before
/// the knob existed.
#[derive(Debug, Clone, Copy, Default)]
pub struct DelayImpairment {
    pub delay: std::time::Duration,
    pub jitter: std::time::Duration,
    pub seed: u64,
}

/// One-way delay with `sch_netem`'s per-packet jitter: every packet's delay is
/// drawn uniformly from `delay ± jitter` and clamped at zero, exactly as the
/// kernel's `sample_delay` does, so a probe can reproduce the field's
/// `delay TIME JITTER` impairment in-process.  With `jitter` zero the
/// draw is skipped entirely and the delay is fixed, so the jitter-free arms
/// are byte-for-byte what they were before the jitter knob existed.
#[derive(Debug)]
pub struct DelayedRead<R: UnreliableRead> {
    inner: R,
    delay: std::time::Duration,
    jitter: std::time::Duration,
    jitter_state: u64,
    pending: std::collections::VecDeque<(std::time::Instant, Vec<u8>)>,
}

impl<R: UnreliableRead> DelayedRead<R> {
    pub fn new(read: R, delay: std::time::Duration) -> Self {
        Self::with_jitter(read, delay, std::time::Duration::ZERO, 0)
    }

    /// Fixed delay plus a seeded `delay ± jitter` draw per packet (SplitMix64,
    /// so an arm is reproducible and load-independent).
    pub fn with_jitter(
        read: R,
        delay: std::time::Duration,
        jitter: std::time::Duration,
        seed: u64,
    ) -> Self {
        Self {
            inner: read,
            delay,
            jitter,
            jitter_state: seed,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// The delay for one packet: `delay` when the jitter knob is zero,
    /// otherwise a uniform draw from `delay ± jitter` clamped at zero.
    fn next_delay(&mut self) -> std::time::Duration {
        if self.jitter.is_zero() {
            return self.delay;
        }
        self.jitter_state = self.jitter_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.jitter_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let span = self.jitter.as_nanos() as u64;
        let delta = (z % (span * 2 + 1)) as i64 - span as i64;
        let ns = self.delay.as_nanos() as i64 + delta;
        std::time::Duration::from_nanos(ns.max(0) as u64)
    }

    fn enqueue_ripe_candidates(&mut self) {
        let mut scratch = vec![0u8; 64 * 1024];
        loop {
            match self.inner.try_recv(&mut scratch) {
                Ok(n) => {
                    let delay = self.next_delay();
                    self.pending
                        .push_back((std::time::Instant::now() + delay, scratch[..n].to_vec()));
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    fn pop_ripe(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.enqueue_ripe_candidates();
        match self.pending.front() {
            Some((release, data)) if *release <= std::time::Instant::now() => {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                self.pending.pop_front();
                Ok(n)
            }
            _ => Err(std::io::ErrorKind::WouldBlock.into()),
        }
    }
}

#[async_trait]
impl<R: UnreliableRead + Send + Sync + 'static> UnreliableRead for DelayedRead<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.pop_ripe(buf)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            match self.pop_ripe(buf) {
                Ok(n) => return Ok(n),
                Err(error) if error == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
            let wait = self
                .pending
                .front()
                .map(|(release, _)| release.saturating_duration_since(std::time::Instant::now()))
                .unwrap_or(std::time::Duration::from_millis(1))
                .min(std::time::Duration::from_millis(1))
                .max(std::time::Duration::from_micros(100));
            tokio::time::sleep(wait).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImpairRate {
    pub loss: BasisPoints,
    pub reorder: BasisPoints,
    pub duplicate: BasisPoints,
    applied: Arc<[AtomicUsize; 3]>,
}

impl ImpairRate {
    pub fn new(loss_bps: usize, reorder_bps: usize, duplicate_bps: usize) -> Self {
        Self {
            loss: BasisPoints::with_seed(loss_bps, IMPAIR_LOSS_SEED),
            reorder: BasisPoints::with_seed(reorder_bps, IMPAIR_REORDER_SEED),
            duplicate: BasisPoints::with_seed(duplicate_bps, IMPAIR_DUPLICATE_SEED),
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

/// Shared body of the impairment constructors: every one of them differs only
/// in which read/write wrappers are installed, and all of them go through the
/// same `checked_mss_and_fec` normalisation so a probe cannot accidentally
/// measure a different FEC state, MSS, or tuning than production uses.
fn fec_layer(
    utp_read: Box<dyn UnreliableRead>,
    utp_write: Box<dyn UnreliableWrite>,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
) -> UnreliableLayer {
    let (mss, fec_state, tuning) = checked_mss_and_fec(
        fec,
        Mss::try_new(mss).unwrap(),
        tuning,
        FrameMode::default(),
    )
    .unwrap();
    UnreliableLayer {
        utp_read,
        utp_write,
        post_open_handshake: None,
        session_tag: None,
        initial_sequences: crate::sequence::InitialSequences::ZERO,
        initial_rtt: None,
        metrics_observer: None,
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery: FrameMode::default(),
        congestion_lane: crate::CongestionLane::default(),
        retransmission_armor: RetransmissionArmorConfig::disabled(),
        instream_group_fec: false,
        ack_padding: AckPaddingMode::None,
        fresh_tail_armor_copies_override: None,
        clock: crate::clock::ClockRef::system(),
    }
}

pub fn wrap_fec_impaired<R, W>(read: R, write: W, fec: bool, rate: ImpairRate) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(read),
        Box::new(ImpairedWrite::new(write, rate)),
        fec,
        NO_FEC_MSS,
        fec_tuning_from_env(),
    )
}

/// Like `wrap_fec` but wraps the read/write pair in lossy injectors driven
/// by `rate`. Each connection should get its own `BasisPoints` (or a shared
/// one if you want both directions of a single link to share loss state).
pub fn wrap_fec_lossy<R, W>(read: R, write: W, fec: bool, rate: BasisPoints) -> UnreliableLayer
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
    rate: BasisPoints,
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
    rate: BasisPoints,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(LossyRead::new(read, rate.clone())),
        Box::new(LossyWrite::new(write, rate)),
        fec,
        mss,
        tuning,
    )
}

/// Like [`wrap_fec_lossy_with_mss_and_fec_tuning`] but impairs only the write
/// direction with a deterministic run-of-`burst` drop pattern ([`BurstLoss`])
/// instead of iid loss. The read direction is left clean so the echo return
/// path does not add its own loss; the burst is the sender-to-receiver
/// impairment under test.
pub fn wrap_fec_burst_lossy_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    loss: BurstLoss,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(read),
        Box::new(BurstLossyWrite::new(write, loss)),
        fec,
        mss,
        tuning,
    )
}

/// Like [`wrap_fec_burst_lossy_with_mss_and_fec_tuning`], but the read
/// direction also carries a fixed one-way propagation delay ([`DelayedRead`]).
/// The sender-to-receiver burst loss plus a WAN-scale delay reproduces the
/// interactive ARQ tail in-process: a message that falls through its in-band
/// redundancy waits for the reorder window and a full round trip.
pub fn wrap_fec_burst_delayed_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    loss: BurstLoss,
    delay: std::time::Duration,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(DelayedRead::new(read, delay)),
        Box::new(BurstLossyWrite::new(write, loss)),
        fec,
        mss,
        tuning,
    )
}

/// Like [`wrap_fec_burst_delayed_with_mss_and_fec_tuning`], but the read
/// direction's one-way delay carries `sch_netem`'s `delay TIME JITTER` draw
/// ([`DelayedRead::with_jitter`]) instead of a fixed value.
///
/// This is the field's own impairment shape: a WAN-scale round trip whose
/// delay is jittered per packet, so the RTT estimator's variance term is
/// large relative to its smoothed RTT and a corroborated repair rung is
/// `raw_rto` rather than the 300 ms floor.
pub fn wrap_fec_jittered_burst_delayed_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    loss: BurstLoss,
    impairment: DelayImpairment,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    let DelayImpairment {
        delay,
        jitter,
        seed,
    } = impairment;
    fec_layer(
        Box::new(DelayedRead::with_jitter(read, delay, jitter, seed)),
        Box::new(BurstLossyWrite::new(write, loss)),
        fec,
        mss,
        tuning,
    )
}

/// iid-loss + WAN-delay variant of
/// [`wrap_fec_burst_delayed_with_mss_and_fec_tuning`]: the sender-to-receiver
/// direction carries independent (not burst) loss, the read direction is clean
/// but delayed.  A loss that outruns the same-round-trip armor therefore falls
/// through to the reorder-window ARQ tail instead of the sub-millisecond
/// loopback floor, matching the burst probe's timing shape.
pub fn wrap_fec_iid_delayed_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    loss: BasisPoints,
    delay: std::time::Duration,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(DelayedRead::new(read, delay)),
        Box::new(LossyWrite::new(write, loss)),
        fec,
        mss,
        tuning,
    )
}

/// Clean-write partner for [`wrap_fec_burst_delayed_with_mss_and_fec_tuning`]:
/// the echo direction adds only the same one-way delay, no loss.
pub fn wrap_fec_delayed_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    delay: std::time::Duration,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    fec_layer(
        Box::new(DelayedRead::new(read, delay)),
        Box::new(write),
        fec,
        mss,
        tuning,
    )
}

/// Jittered-delay partner for
/// [`wrap_fec_jittered_burst_delayed_with_mss_and_fec_tuning`]: the echo
/// direction adds the same `delay ± jitter` draw, no loss.
pub fn wrap_fec_jittered_delayed_with_mss_and_fec_tuning<R, W>(
    read: R,
    write: W,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    impairment: DelayImpairment,
) -> UnreliableLayer
where
    R: UnreliableRead + Send + Sync + 'static,
    W: UnreliableWrite,
{
    let DelayImpairment {
        delay,
        jitter,
        seed,
    } = impairment;
    fec_layer(
        Box::new(DelayedRead::with_jitter(read, delay, jitter, seed)),
        Box::new(write),
        fec,
        mss,
        tuning,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// The burst injector drops exactly `burst` consecutive packets, then a
    /// quiet gap in the configured range, then the next burst — so a whole
    /// redundancy group can be wiped instead of a single copy.
    #[test]
    fn burst_loss_drops_consecutive_runs_separated_by_random_gaps() {
        let loss = BurstLoss::new(3, 5, 5, 42);
        let rolls: Vec<bool> = (0..40).map(|_| loss.roll()).collect();
        assert_eq!(&rolls[..3], &[true, true, true], "first run is a burst");
        assert_eq!(&rolls[3..8], &[false; 5], "then exactly five quiet packets");
        assert_eq!(&rolls[8..11], &[true, true, true], "then the next burst");
        assert_eq!(
            loss.dropped(),
            15,
            "five bursts of three over forty packets"
        );
    }

    /// `burst == 0` is the delay-only control: it must never drop a packet.
    #[test]
    fn burst_loss_zero_never_drops() {
        let loss = BurstLoss::new(0, 1, 1, 7);
        assert!((0..100).all(|_| !loss.roll()));
        assert_eq!(loss.dropped(), 0);
    }

    /// The iid loss stream is a pure function of its seed and the number of
    /// rolls, and it still realizes the configured rate: two instances built
    /// through [`BasisPoints::new`] reproduce the same drops, and 8 % over a
    /// hundred thousand rolls lands on 8 %. This keeps an impairment
    /// assertion such as "FEC recovers > 0 under 8 % loss" a fact about the
    /// transport rather than a draw from the OS RNG.
    #[test]
    fn basis_points_stream_is_deterministic_and_realizes_its_rate() {
        const ROLLS: usize = 100_000;
        let stream = |bps: usize| -> Vec<bool> {
            let rate = BasisPoints::new(bps);
            (0..ROLLS).map(|_| rate.roll()).collect()
        };
        let first = stream(800);
        assert_eq!(
            first,
            stream(800),
            "two identically seeded streams must produce identical drops"
        );
        let realized = first.iter().filter(|dropped| **dropped).count() as f64 / ROLLS as f64;
        assert!(
            (realized - 0.08).abs() < 0.005,
            "8% configured loss realized {realized:.4} over {ROLLS} rolls"
        );
    }

    /// The three [`ImpairRate`] roles draw from independent streams. Sharing
    /// one seed would make the loss, reorder and duplication draws at each
    /// send identical, so a duplication rate below the other two could never
    /// fire.
    #[test]
    fn impair_rate_roles_use_independent_streams() {
        let rate = ImpairRate::new(1500, 3000, 1000);
        let stream =
            |points: &BasisPoints| -> Vec<bool> { (0..1_000).map(|_| points.roll()).collect() };
        assert_ne!(stream(&rate.loss), stream(&rate.reorder));
        assert_ne!(stream(&rate.reorder), stream(&rate.duplicate));
        assert_ne!(stream(&rate.loss), stream(&rate.duplicate));
    }

    #[derive(Debug)]
    struct OneShotRead(Option<Vec<u8>>);

    #[async_trait]
    impl UnreliableRead for OneShotRead {
        fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            match self.0.take() {
                Some(data) => {
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    Ok(n)
                }
                None => Err(std::io::ErrorKind::WouldBlock.into()),
            }
        }
        async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            self.try_recv(buf)
        }
    }

    /// The delay line holds a datagram for the configured one-way delay and
    /// returns it intact, reproducing a WAN propagation delay without a real
    /// remote peer.
    #[tokio::test]
    async fn delayed_read_holds_a_datagram_for_the_delay() {
        let read = OneShotRead(Some(b"hello".to_vec()));
        let mut delayed = DelayedRead::new(read, Duration::from_millis(40));
        let mut buf = [0u8; 8];
        let started = Instant::now();
        let n = delayed.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
        assert!(
            started.elapsed() >= Duration::from_millis(30),
            "a delayed read returned after only {:?}",
            started.elapsed()
        );
    }

    /// `DelayedRead`'s jitter knob is `sch_netem`'s `delay TIME JITTER` draw:
    /// uniform over `delay ± jitter`, clamped at zero, and reproducible from
    /// its seed so a jittered measurement arm is a fact about the transport
    /// rather than a draw from the OS RNG.  With the knob zero the delay is
    /// exactly the fixed value and no draw is taken, so every delay-only arm
    /// predating the knob keeps its timing byte-for-byte.
    #[test]
    fn delayed_read_jitter_is_the_netem_draw_and_zero_jitter_is_fixed() {
        let read = || OneShotRead(Some(b"x".to_vec()));
        let mut fixed = DelayedRead::new(read(), Duration::from_millis(50));
        assert!(
            (0..64).all(|_| fixed.next_delay() == Duration::from_millis(50)),
            "a zero-jitter delay line must never draw"
        );

        let delay = Duration::from_millis(100);
        let jitter = Duration::from_millis(100);
        let draws = |seed: u64| -> Vec<Duration> {
            let mut line = DelayedRead::with_jitter(read(), delay, jitter, seed);
            (0..4_096).map(|_| line.next_delay()).collect()
        };
        let stream = draws(0x5EED);
        assert_eq!(
            stream,
            draws(0x5EED),
            "the jitter draw must be seeded, not random"
        );
        assert!(
            stream.iter().all(|d| *d <= delay + jitter),
            "a draw above `delay + jitter` is outside the netem window"
        );
        assert!(
            stream.iter().any(|d| *d < delay) && stream.iter().any(|d| *d > delay),
            "the draw must straddle the nominal delay, or no jitter is applied"
        );
        let mean_ms =
            stream.iter().map(|d| d.as_millis() as f64).sum::<f64>() / stream.len() as f64;
        assert!(
            (mean_ms - 100.0).abs() < 5.0,
            "the ±100 ms draw's mean must be the nominal 100 ms, got {mean_ms:.1}"
        );
    }
}
