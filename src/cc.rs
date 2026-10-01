//! Cross-connection CC (congestion-control) signalling on one egress path.
//!
//! This layer **never touches the datagram path**. It is a signal router in
//! two directions:
//!
//! * it **receives** the presence of interactive connections on an egress
//!   path (via their [`MetricsObserver`]), and
//! * it **sends** that fact to the path's bulk connections, which use it to
//!   decide whether their *own* delay-based congestion control is
//!   authoritative.
//!
//! # The unit is an egress path
//!
//! Signals are shared only between connections that are **likely to contend
//! for the same link**, and that is decided by the `(src ip, dst ip)` pair: a
//! local source address and a remote destination address. Two connections with
//! the same pair take the same route, so they share a bottleneck; a different
//! destination means a different route — or at least a different policed path
//! — and a delay measured there is not evidence about this one. That rule gives
//! isolation for free: two access servers reached from the same host share a
//! local source but differ in destination, so a badly-connected client leaves
//! another client's traffic alone.
//!
//! # Why the signal is a *presence* flag, not the other lane's delay
//!
//! A bulk sender's own RTT already measures the queue it is sharing — the
//! bottleneck is between it and its peer, and its own ACKs traverse it. What a
//! per-connection controller *cannot* do on a shared path is trust that
//! measurement in the presence of loss: it treats loss from a buffer it is
//! itself filling as independent evidence of congestion and backs off, instead
//! of draining the delay it is causing. That is the whole asymmetry.
//!
//! So the only cross-connection fact needed is *"another lane shares this
//! path"*. Given it, the bulk connection's own delay gate is authoritative and
//! loss no longer suppresses it. The reaction then runs at the bulk sender's
//! own RTT-sample cadence rather than waiting on a sparse interactive lane's
//! samples, and it needs no cross-connection state beyond one flag.
//!
//! Nothing here caps throughput. No rate, credit, reserve or token bucket
//! exists; the flag does not change *what* the bulk controller does, only
//! *whether* its delay response is allowed to run.
//!
//! # Construction is the caller's
//!
//! There is no global. A caller constructs one [`CcSignalHub`] however it
//! likes (one per egress path, one per process, several independent ones) and asks it
//! for a [`CcSignalGroup`] per egress path. An interactive connection chains
//! [`CcSignalSource::observer`] into its `metrics_observer`; a bulk connection
//! carries [`CcSignalGroup::bulk`] on its connect/accept config.
//!
//! # Lifetime
//!
//! The scheduler holds groups and their signals **weakly**; the last handle to
//! a group removes its path from the map in [`Group::drop`], so neither the
//! path map nor a group's signal list grows with the number of connections or
//! destinations ever seen.

use std::{
    collections::HashMap,
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::metrics::{MetricsEvent, MetricsObserver, MetricsSnapshot};

/// How long an interactive connection's presence counts after its last
/// observation. Past this horizon the lane is treated as gone, so a path does
/// not stay "shared" on a connection that has ended.
pub const SIGNAL_STALE_AFTER: Duration = Duration::from_secs(1);

/// How many of a publishing lane's own control RTTs its state stays valid.
///
/// A payload is published on every `RttSample`, so a fresh payload is never
/// more than one control RTT old; a statement older than that cannot improve
/// on the consumer's own fresh sample and must be read as **absent**, never as
/// zero and never as "unchanged". The horizon is deliberately not
/// [`SIGNAL_STALE_AFTER`]: that one decides *presence* (is a lane there at
/// all), while this one decides whether the lane's *sampled quantities* may be
/// used in a decision.
pub const PAYLOAD_VALID_RTTS: u32 = 1;

/// Sentinel for an absent `Option<Duration>` stored in an `AtomicU64` as
/// nanoseconds. `u64::MAX` ns is ~584 years, beyond any live path, so it can
/// never collide with a real value.
const NONE_NS: u64 = u64::MAX;

/// How long a live interactive lane may go without an `RttSample` before it
/// republishes its state as a heartbeat.
///
/// The payload gates the stand-off's [`STANDOFF_WINDOW`] and is itself valid
/// for [`PAYLOAD_VALID_RTTS`] of the publisher's own control RTTs, so the
/// heartbeat must be at least as fresh as the tighter of the two horizons:
///
/// ```text
/// heartbeat = min(PAYLOAD_VALID_RTTS * control_rtt, STANDOFF_WINDOW)
/// ```
///
/// The control-RTT term keeps the payload inside its own validity window (a
/// heartbeat at the control RTT is the last moment before `is_fresh` would
/// drop it); the window term keeps a long-RTT lane at least as fresh as the
/// decision the payload feeds, so a consumer never reads an idle statement
/// older than the gate it gates. A lane with no control RTT cannot date its
/// fields, so it has no heartbeat and its payload stays absent once stale.
fn heartbeat_interval(control_rtt: Option<Duration>) -> Option<Duration> {
    let rtt = control_rtt?;
    (!rtt.is_zero()).then(|| (rtt * PAYLOAD_VALID_RTTS).min(STANDOFF_WINDOW))
}

fn store_opt_ns(slot: &AtomicU64, value: Option<Duration>) {
    slot.store(
        value.map_or(NONE_NS, |d| d.as_nanos().min(NONE_NS as u128 - 1) as u64),
        Ordering::Relaxed,
    );
}

fn load_opt_ns(slot: &AtomicU64) -> Option<Duration> {
    let raw = slot.load(Ordering::Relaxed);
    (raw != NONE_NS).then(|| Duration::from_nanos(raw))
}

fn store_opt_f64(slot: &AtomicU64, value: Option<f64>) {
    slot.store(value.map_or(NONE_NS, f64::to_bits), Ordering::Relaxed);
}

fn load_opt_f64(slot: &AtomicU64) -> Option<f64> {
    let raw = slot.load(Ordering::Relaxed);
    (raw != NONE_NS).then(|| f64::from_bits(raw))
}

fn min_opt(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (some, None) | (None, some) => some,
    }
}

fn max_opt(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (some, None) | (None, some) => some,
    }
}

fn max_opt_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (some, None) | (None, some) => some,
    }
}

/// One interactive connection's statement about its egress path, as published
/// to the bulk connections that share it and then discarded once it is older
/// than [`PAYLOAD_VALID_RTTS`] control RTTs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathLaneState {
    /// `congestion_rtt_floor`: the lane's windowed, queue-free RTT floor.
    pub floor: Option<Duration>,
    /// `congestion_control_rtt - floor`, saturating: the standing queue this
    /// lane sees, measured by the lane with the least of its own queue.
    pub queue_delay: Option<Duration>,
    /// `congestion_queue_tolerance`: this lane's own ordinary drain margin.
    pub tolerance: Option<Duration>,
    /// `congestion_persistent_queue_for`: this lane's own persistent-queue
    /// latch. `Some` means the lane's own drain gate has fired.
    pub persistent_for: Option<Duration>,
    /// `send_rate_packets_per_second`: the lane's offered packet rate.
    pub offered_pps: f64,
    /// `pending_send_bytes`: data waiting on the lane's send path.
    pub pending_bytes: usize,
    /// `application_write_waiters`: applications blocked on the lane's send
    /// path.
    pub write_waiters: usize,
    /// `congestion_loss_ratio`: the lane's windowed loss evidence.
    pub loss: Option<f64>,
    /// `congestion_delivery_peak_packets_per_second`: carried with its
    /// validity flag so no reader mistakes a sparse lane's offer for capacity.
    pub capacity_pps: Option<f64>,
    /// `delivery_sample_app_limited == Some(false)`: the last sample saturated
    /// the path, so `capacity_pps` is a capacity reading at all.
    pub saturating: bool,
    /// `congestion_control_rtt`: the lane's control interval, which dates every
    /// sampled field above.
    pub control_rtt: Option<Duration>,
    /// When this state was published (elapsed since the hub's epoch).
    pub stamp: Duration,
}

impl PathLaneState {
    /// Whether this lane's sampled fields are within [`PAYLOAD_VALID_RTTS`]
    /// control RTTs of `now`. A lane that has never recorded a control RTT
    /// cannot date its fields, so it is never fresh.
    pub fn is_fresh(&self, now: Duration) -> bool {
        let Some(rtt) = self.control_rtt else {
            return false;
        };
        if rtt.is_zero() {
            return false;
        }
        now.saturating_sub(self.stamp) <= rtt * PAYLOAD_VALID_RTTS
    }
}

/// The aggregate of every fresh lane on one `(src, dst)` path, as read by a
/// bulk connection's congestion controller.
///
/// `None` from [`CcSignal::state`] means no lane on the path published a fresh
/// payload; every consumer must treat that as *absent*, never as zero.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathState {
    /// The newest contributing lane's stamp.
    pub stamp: Duration,
    /// The tightest (smallest) queue-free floor over the fresh lanes.
    pub floor: Option<Duration>,
    /// The least-queued lane's standing queue.
    pub queue_delay: Option<Duration>,
    /// The strictest ordinary drain margin over the fresh lanes.
    pub tolerance: Option<Duration>,
    /// The longest-armed persistent-queue latch over the fresh lanes.
    pub persistent_for: Option<Duration>,
    /// The summed offered packet rate over the fresh lanes.
    pub offered_pps: f64,
    /// The summed pending send bytes over the fresh lanes.
    pub pending_bytes: usize,
    /// The summed application write waiters over the fresh lanes.
    pub write_waiters: usize,
    /// The worst windowed loss over the fresh lanes.
    pub loss: Option<f64>,
    /// The largest delivery peak over the saturating fresh lanes.
    pub capacity_pps: Option<f64>,
    /// Whether any fresh lane's last sample saturated the path.
    pub saturating: bool,
    /// The widest control-RTT horizon over the fresh lanes.
    pub control_rtt: Option<Duration>,
}

impl PathState {
    /// The identity element for [`Self::merge`].
    fn absent() -> Self {
        Self {
            stamp: Duration::ZERO,
            floor: None,
            queue_delay: None,
            tolerance: None,
            persistent_for: None,
            offered_pps: 0.0,
            pending_bytes: 0,
            write_waiters: 0,
            loss: None,
            capacity_pps: None,
            saturating: false,
            control_rtt: None,
        }
    }

    /// Fold one fresh lane's state into the group aggregate. `min` for the
    /// queue-free baselines, `max` for the longest latch and the worst loss,
    /// and `sum` for the activity counters.
    fn merge(&mut self, lane: &PathLaneState) {
        self.stamp = self.stamp.max(lane.stamp);
        self.floor = min_opt(self.floor, lane.floor);
        self.queue_delay = min_opt(self.queue_delay, lane.queue_delay);
        self.tolerance = min_opt(self.tolerance, lane.tolerance);
        self.persistent_for = max_opt(self.persistent_for, lane.persistent_for);
        self.offered_pps += lane.offered_pps;
        self.pending_bytes = self.pending_bytes.saturating_add(lane.pending_bytes);
        self.write_waiters = self.write_waiters.saturating_add(lane.write_waiters);
        self.loss = max_opt_f64(self.loss, lane.loss);
        self.capacity_pps = max_opt_f64(self.capacity_pps, lane.capacity_pps);
        self.saturating |= lane.saturating;
        self.control_rtt = max_opt(self.control_rtt, lane.control_rtt);
    }
}

/// How long the interactive lane may be quiet before a bulk lane on the same
/// path treats the path as free to contest against an external loss-based
/// competitor.
///
/// This is deliberately **not** [`SIGNAL_STALE_AFTER`]: staleness decides when
/// the path stops being *shared* for the loss gate (one second), while this
/// window decides when the interactive lane has been idle long enough that the
/// bulk lane can claim the link without harming it. A window at the staleness
/// horizon would let the bulk lane re-enter on the first user think-time pause
/// and start filling the buffer a resuming interactive packet would queue
/// behind; a window many times the interactive lane's own cadence keeps the
/// bulk yielded through consecutive request/response turns while reclaiming the
/// link within a second or two of a genuine idle gap.
///
/// The value is one and a half seconds: longer than a normal interactive
/// round-trip pause (the operator's client multiplexes a Minecraft-shaped
/// workload whose bursts are hundreds of milliseconds apart) and short enough
/// that an idle link is back under competition before the second second of
/// silence. It is an explicit knob rather than a reuse of the staleness
/// horizon, so a change to one cannot silently move the other.
pub const STANDOFF_WINDOW: Duration = Duration::from_millis(1500);

/// The bulk stand-off's multiplicative-decrease factor on a sampled loss.
///
/// The shipped value is three quarters.  One half makes the competing response
/// identical to the reference AIMD an external TCP-family flow runs, so a
/// symmetric stand-off splits the bottleneck fairly and holds no advantage; a
/// test-only hub sets that value to be a control.  Three quarters is the
/// certified production value: a gentler decrease keeps more of the rate
/// through a loss event, which reclaims the quiet-phase share the symmetric
/// response leaves below half -- measured `0.5027` of the pair's delivered
/// bytes against a reference-AIMD competitor, a fair split rather than
/// domination, paired `+0.0392 [+0.0245,+0.0539]` over the `0.4635` control.
/// The next step, `0.90`, adds a further `1.7` pp that is *not resolved* as an
/// incremental effect and costs a resolved `+63..+74` ms on the interactive
/// lane's first ~`30` messages after every quiet-gap resume -- the tail M1
/// ranks first -- so the gentler, costless value is the one that ships.
///
/// It is a *distinct, named* constant rather than a reuse of the reference
/// law's factor precisely so the two can be measured against each other.
///
/// A test-only hub can set another value per path
/// ([`CcSignalHub::with_standoff_decrease_factor`]); production hubs always use
/// this one.
pub const STANDOFF_DECREASE_FACTOR: f64 = 0.75;

/// The path's sharedness signal, as consumed by one bulk connection's
/// congestion control. Cheap to clone; shared by every bulk connection on the
/// same `(src, dst)` path. Holding one keeps the path's group alive.
#[derive(Clone)]
pub struct CcSignal {
    group: Arc<Group>,
}

impl fmt::Debug for CcSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CcSignal")
            .field("is_shared", &self.is_shared())
            .finish()
    }
}

impl CcSignal {
    /// Whether a live interactive lane shares this egress path, so the bulk
    /// controller's own delay gate is authoritative even under loss.
    pub fn is_shared(&self) -> bool {
        self.group.aggregate.read()
    }

    /// Whether this path runs the bulk interactive stand-off.  Production
    /// hubs always do (a hub never disables it); the test-only
    /// [`CcSignalHub::without_standoff`] constructor turns it off so an arm can
    /// A/B the mechanism against the shipped delay-first policy with the CC
    /// link otherwise attached and the loss gate unchanged.
    pub fn standoff_enabled(&self) -> bool {
        self.group.standoff
    }

    /// The multiplicative-decrease factor this path's bulk stand-off applies
    /// on a sampled loss.  [`STANDOFF_DECREASE_FACTOR`] unless a test-only hub
    /// overrode it (see [`CcSignalHub::with_standoff_decrease_factor`]).
    pub fn standoff_decrease_factor(&self) -> f64 {
        self.group.standoff_decrease_factor
    }

    /// How long the interactive lane has been quiet on this path: the wall time
    /// since its last `RttSample`-driven update, or `None` when no interactive
    /// lane has ever published here. A fresh path that has never seen an
    /// interactive lane therefore reports `None`, and a bulk lane that needs a
    /// quiet clock falls back to its own connection age (see
    /// `CongestionResponse`).
    pub fn quiet_for(&self) -> Option<Duration> {
        self.group.aggregate.quiet_for()
    }

    /// How long the path's interactive lane has gone without an *application
    /// offer*: the wall time since the most recent offer from a live
    /// interactive connection, or `None` when no live lane has ever offered.
    ///
    /// This is the bulk stand-off's activity witness, deliberately separate
    /// from [`Self::quiet_for`]: an offer is recorded on the application write
    /// path (and while send-path data is pending), so it does **not** gap when
    /// the lane's own packets queue behind the bulk and its `RttSample`s stop.
    /// Reading the RTT clock here is what made a queued lane look idle and let
    /// the bulk compete with the very lane it was standing off for.
    pub fn offered_quiet_for(&self) -> Option<Duration> {
        let now_ms = self.group.aggregate.start.elapsed().as_millis() as u64;
        let mut signals = self.group.signals.lock().unwrap();
        signals.retain(|signal| signal.strong_count() > 0);
        let mut latest: Option<u64> = None;
        for signal in signals.iter().filter_map(Weak::upgrade) {
            let offer = signal.offer_ms.load(Ordering::Acquire);
            if offer != u64::MAX {
                latest = Some(latest.map_or(offer, |current| current.max(offer)));
            }
        }
        latest.map(|offer| Duration::from_millis(now_ms.saturating_sub(offer)))
    }

    /// The aggregated cross-lane payload for this path, or `None` when no live
    /// lane has published a fresh one.
    ///
    /// A lane contributes only while its payload is within
    /// [`PAYLOAD_VALID_RTTS`] control RTTs of now; a stale payload is absent,
    /// never zero and never "unchanged". `None` therefore means the consumer
    /// must fall back to its own measurements, and must not read the absence
    /// as either "idle" or "busy".
    ///
    /// A live lane that has gone quiet produces no `RttSample`, so before this
    /// read every live lane is given the chance to [republish its state as a
    /// heartbeat](SignalState::heartbeat). The consumer's own read cadence is
    /// the heartbeat's clock, so the payload is refreshed at
    /// [`heartbeat_interval`] and never expires between the reads that need it.
    pub fn state(&self) -> Option<PathState> {
        let now = Duration::from_millis(self.group.aggregate.start.elapsed().as_millis() as u64);
        let mut signals = self.group.signals.lock().unwrap();
        signals.retain(|signal| signal.strong_count() > 0);
        let mut state = PathState::absent();
        let mut any = false;
        for signal in signals.iter().filter_map(Weak::upgrade) {
            signal.heartbeat(now);
            if let Some(lane) = signal.fresh_lane_state(now) {
                state.merge(&lane);
                any = true;
            }
        }
        any.then_some(state)
    }
}

#[derive(Debug)]
struct CongestionCell {
    start: Instant,
    shared: AtomicBool,
    updated_ms: AtomicU64,
}

impl CongestionCell {
    fn new(start: Instant) -> Self {
        Self {
            start,
            shared: AtomicBool::new(false),
            // `0` with `start` at construction means "older than any epoch";
            // the staleness check below treats it as not-fresh.
            updated_ms: AtomicU64::new(0),
        }
    }

    fn store(&self, shared: bool, now_ms: u64) {
        self.shared.store(shared, Ordering::Release);
        self.updated_ms.store(now_ms, Ordering::Release);
    }

    fn read(&self) -> bool {
        let now_ms = self.start.elapsed().as_millis() as u64;
        let fresh = now_ms.saturating_sub(self.updated_ms.load(Ordering::Acquire))
            <= SIGNAL_STALE_AFTER.as_millis() as u64;
        fresh && self.shared.load(Ordering::Acquire)
    }

    /// Time since the last interactive update, or `None` if there has never
    /// been one. `updated_ms == 0` is the "never" sentinel: [`Self::store`] is
    /// only reached from an interactive update, so a cell that has been live at
    /// least once always carries a non-zero stamp.
    fn quiet_for(&self) -> Option<Duration> {
        let updated_ms = self.updated_ms.load(Ordering::Acquire);
        if updated_ms == 0 {
            return None;
        }
        let now_ms = self.start.elapsed().as_millis() as u64;
        Some(Duration::from_millis(now_ms.saturating_sub(updated_ms)))
    }
}

/// One interactive connection's presence on its path, fed from its metrics
/// observer. Cheap to clone; holding one keeps the path's group alive.
#[derive(Clone)]
pub struct CcSignalSource {
    inner: Arc<SignalState>,
}

impl fmt::Debug for CcSignalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CcSignalSource")
            .field("min_rtt", &self.min_rtt())
            .field("smoothed_rtt", &self.smoothed_rtt())
            .finish()
    }
}

#[derive(Debug)]
struct SignalState {
    /// The group this signal aggregates into. A live signal keeps its group
    /// alive; the group refers back to the signal only `Weak`.
    group: Arc<Group>,
    start: Instant,
    /// Whether an `RttSample` has ever updated this lane. An explicit flag
    /// rather than an `updated_ms == 0` sentinel, because an update in the
    /// group's first millisecond also stamps `0`.
    observed: AtomicBool,
    /// Reset on every observation. There is no other state: presence is the
    /// whole signal.
    updated_ms: AtomicU64,
    /// Set on every application offer (a write, or data pending on the send
    /// path).  The stand-off's activity witness reads this instead of
    /// `updated_ms`, so a lane whose packets are queued — and whose
    /// `RttSample`s have therefore stopped — still reads as active.
    /// `u64::MAX` is the "never offered" sentinel, so an offer recorded in
    /// the first millisecond of the group's life (its timestamp is `0`) is not
    /// mistaken for no offer at all.
    offer_ms: AtomicU64,
    min_rtt_ns: AtomicU64,
    srtt_ns: AtomicU64,
    /// `send_rate_packets_per_second`, as `f64::to_bits`.
    send_rate_bits: AtomicU64,
    /// `pending_send_bytes`.
    pending_bytes: AtomicU64,
    /// `application_write_waiters`.
    write_waiters: AtomicU64,
    /// `congestion_persistent_queue_for`, as nanoseconds; `NONE_NS` is absent.
    persistent_for_ns: AtomicU64,
    /// `congestion_rtt_floor`, as nanoseconds; `NONE_NS` is absent.
    floor_ns: AtomicU64,
    /// `congestion_queue_tolerance`, as nanoseconds; `NONE_NS` is absent.
    tolerance_ns: AtomicU64,
    /// `congestion_loss_ratio`, as `f64::to_bits`; `NONE_NS` is absent.
    loss_bits: AtomicU64,
    /// `congestion_control_rtt`, as nanoseconds; `NONE_NS` is absent.
    control_rtt_ns: AtomicU64,
    /// `congestion_delivery_peak_packets_per_second`, as `f64::to_bits`.
    capacity_pps_bits: AtomicU64,
    /// `delivery_sample_app_limited == Some(false)`.
    saturating: AtomicBool,
}

impl SignalState {
    /// This lane's payload, or `None` when it has never published or its
    /// payload is older than [`PAYLOAD_VALID_RTTS`] control RTTs.
    fn fresh_lane_state(&self, now: Duration) -> Option<PathLaneState> {
        if !self.observed.load(Ordering::Acquire) {
            return None;
        }
        let updated_ms = self.updated_ms.load(Ordering::Acquire);
        let control_rtt = load_opt_ns(&self.control_rtt_ns);
        let floor = load_opt_ns(&self.floor_ns);
        let lane = PathLaneState {
            floor,
            queue_delay: control_rtt
                .zip(floor)
                .map(|(rtt, floor)| rtt.saturating_sub(floor)),
            tolerance: load_opt_ns(&self.tolerance_ns),
            persistent_for: load_opt_ns(&self.persistent_for_ns),
            offered_pps: f64::from_bits(self.send_rate_bits.load(Ordering::Relaxed)),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed) as usize,
            write_waiters: self.write_waiters.load(Ordering::Relaxed) as usize,
            loss: load_opt_f64(&self.loss_bits),
            capacity_pps: load_opt_f64(&self.capacity_pps_bits),
            saturating: self.saturating.load(Ordering::Relaxed),
            control_rtt,
            stamp: Duration::from_millis(updated_ms),
        };
        lane.is_fresh(now).then_some(lane)
    }

    /// Republish this lane's state as a heartbeat when it has gone quiet.
    ///
    /// `RttSample`s are the only publication path, so an idle lane — exactly
    /// the lane whose idleness R1 must see — stops publishing and its payload
    /// expires within one control RTT. This is called from
    /// [`CcSignal::state`] before the freshness read and re-stamps
    /// `updated_ms` at [`heartbeat_interval`], so a consumer reads a *fresh*
    /// idle rather than an absent payload.
    ///
    /// The heartbeat only ever refreshes a lane that is **live**: a lane that
    /// has never published (`observed == false`), and a lane whose last
    /// publication is older than [`SIGNAL_STALE_AFTER`], publish nothing, so
    /// an absent payload keeps meaning "no live lane" and every payload-gated
    /// rule stays skipped. A lane with no control RTT cannot date its fields
    /// and also publishes nothing.
    ///
    /// When the lane has not made an application offer within one control RTT
    /// it is idle, and the heartbeat carries the idle activity triple
    /// (`pending == 0`, `write_waiters == 0`, offered rate `0`). When it is
    /// still offering, the last sampled (busy) triple stands and only the
    /// stamp moves: an offering lane publishes its own fresh payload on every
    /// `RttSample`, so the heartbeat must not overwrite a live offer reading
    /// with a zero.
    fn heartbeat(&self, now: Duration) {
        if !self.observed.load(Ordering::Acquire) {
            return;
        }
        let now_ms = now.as_millis() as u64;
        let updated_ms = self.updated_ms.load(Ordering::Acquire);
        // A lane unseen for longer than the presence horizon is gone, not
        // idle: leave the payload absent rather than resurrect it.
        if now_ms.saturating_sub(updated_ms) > SIGNAL_STALE_AFTER.as_millis() as u64 {
            return;
        }
        let control_rtt = load_opt_ns(&self.control_rtt_ns);
        let Some(interval) = heartbeat_interval(control_rtt) else {
            return;
        };
        if now_ms.saturating_sub(updated_ms) < interval.as_millis() as u64 {
            return;
        }
        // `offer_ms` is refreshed on the application write path and on any
        // send pass with staged data pending, so a quiet offer clock is a
        // sufficient condition for "nothing pending": the lane is idle.
        let offer_ms = self.offer_ms.load(Ordering::Acquire);
        let idle = offer_ms == u64::MAX
            || now_ms.saturating_sub(offer_ms)
                >= control_rtt
                    .expect("a heartbeat interval needs a control RTT")
                    .as_millis() as u64;
        if idle {
            self.send_rate_bits
                .store(0.0f64.to_bits(), Ordering::Relaxed);
            self.pending_bytes.store(0, Ordering::Relaxed);
            self.write_waiters.store(0, Ordering::Relaxed);
        }
        self.updated_ms.store(now_ms, Ordering::Release);
    }
}

impl CcSignalSource {
    fn new(group: &Arc<Group>, start: Instant) -> Self {
        Self {
            inner: Arc::new(SignalState {
                group: Arc::clone(group),
                start,
                observed: AtomicBool::new(false),
                updated_ms: AtomicU64::new(0),
                offer_ms: AtomicU64::new(u64::MAX),
                min_rtt_ns: AtomicU64::new(0),
                srtt_ns: AtomicU64::new(0),
                send_rate_bits: AtomicU64::new(0),
                pending_bytes: AtomicU64::new(0),
                write_waiters: AtomicU64::new(0),
                persistent_for_ns: AtomicU64::new(NONE_NS),
                floor_ns: AtomicU64::new(NONE_NS),
                tolerance_ns: AtomicU64::new(NONE_NS),
                loss_bits: AtomicU64::new(NONE_NS),
                control_rtt_ns: AtomicU64::new(NONE_NS),
                capacity_pps_bits: AtomicU64::new(NONE_NS),
                saturating: AtomicBool::new(false),
            }),
        }
    }

    /// Record that this lane's application offered data: an application write,
    /// or data pending on its send path.  The bulk stand-off reads this clock,
    /// not `RttSample` freshness, so a lane whose packets are queued still
    /// reads as active.
    pub fn offer(&self) {
        self.inner.offer_ms.store(
            self.inner.start.elapsed().as_millis() as u64,
            Ordering::Release,
        );
    }

    fn update(&self, snapshot: &MetricsSnapshot) {
        if let Some(min) = snapshot.minimum_rtt {
            self.inner
                .min_rtt_ns
                .store(min.as_nanos() as u64, Ordering::Relaxed);
        }
        self.inner
            .srtt_ns
            .store(snapshot.smoothed_rtt.as_nanos() as u64, Ordering::Relaxed);
        // The cross-lane payload. Everything below is stored relaxed and then
        // published by the `observed`/`updated_ms` release stores at the end,
        // so a reader that acquires `updated_ms` sees a coherent set.
        self.inner.send_rate_bits.store(
            snapshot.send_rate_packets_per_second.to_bits(),
            Ordering::Relaxed,
        );
        self.inner.pending_bytes.store(
            snapshot.pending_send_bytes.min(u64::MAX as usize) as u64,
            Ordering::Relaxed,
        );
        self.inner.write_waiters.store(
            snapshot.application_write_waiters.min(u64::MAX as usize) as u64,
            Ordering::Relaxed,
        );
        store_opt_ns(
            &self.inner.persistent_for_ns,
            snapshot.congestion_persistent_queue_for,
        );
        store_opt_ns(&self.inner.floor_ns, snapshot.congestion_rtt_floor);
        store_opt_ns(
            &self.inner.tolerance_ns,
            snapshot.congestion_queue_tolerance,
        );
        store_opt_f64(&self.inner.loss_bits, snapshot.congestion_loss_ratio);
        store_opt_ns(&self.inner.control_rtt_ns, snapshot.congestion_control_rtt);
        store_opt_f64(
            &self.inner.capacity_pps_bits,
            snapshot.congestion_delivery_peak_packets_per_second,
        );
        self.inner.saturating.store(
            snapshot.delivery_sample_app_limited == Some(false),
            Ordering::Relaxed,
        );
        self.inner.observed.store(true, Ordering::Release);
        self.inner.updated_ms.store(
            self.inner.start.elapsed().as_millis() as u64,
            Ordering::Release,
        );
        recompute(&self.inner.group);
    }

    /// The measured minimum RTT, if any sample has been seen.
    pub fn min_rtt(&self) -> Option<Duration> {
        let ns = self.inner.min_rtt_ns.load(Ordering::Relaxed);
        (ns > 0).then(|| Duration::from_nanos(ns))
    }

    /// The measured smoothed RTT.
    pub fn smoothed_rtt(&self) -> Duration {
        Duration::from_nanos(self.inner.srtt_ns.load(Ordering::Relaxed))
    }

    /// An observer that publishes this connection's presence. Chain it into the
    /// connection's `metrics_observer` (with any existing observer).
    pub fn observer(&self) -> MetricsObserver {
        let signal = self.clone();
        MetricsObserver::filtered(
            |event, _| matches!(event, MetricsEvent::RttSample),
            move |observation| {
                if let Some(snapshot) = observation.snapshot {
                    signal.update(&snapshot);
                }
            },
        )
    }
}

struct PathMap {
    start: Instant,
    /// Whether bulk connections on this hub run the bulk interactive stand-off.
    /// See [`CcSignalHub::without_standoff`].
    standoff: bool,
    /// The stand-off's multiplicative-decrease factor on a sampled loss; see
    /// [`STANDOFF_DECREASE_FACTOR`] and
    /// [`CcSignalHub::with_standoff_decrease_factor`].
    standoff_decrease_factor: f64,
    /// Presence domains keyed by the egress path `(src, dst)`, held weakly: a
    /// path with no live connection is removed by [`Group::drop`], so the map
    /// stays proportional to live paths rather than to every path ever seen.
    groups: Mutex<HashMap<(IpAddr, IpAddr), Weak<Group>>>,
}

struct Group {
    key: (IpAddr, IpAddr),
    /// Whether bulk connections here run the interactive stand-off; copied from
    /// the hub so a signal can read it without reaching back to the map.
    standoff: bool,
    /// The stand-off's multiplicative-decrease factor, copied from the hub for
    /// the same reason as `standoff`.
    standoff_decrease_factor: f64,
    /// Back-reference used to remove this path's entry when the last handle to
    /// the group goes away. `Weak`, so a map entry never keeps its own group
    /// alive.
    map: Weak<PathMap>,
    /// The interactive connections on this path, held weakly so the list
    /// cannot outlive them. Pruned on every use.
    signals: Mutex<Vec<Weak<SignalState>>>,
    /// The aggregate every bulk connection on this path reads.
    aggregate: Arc<CongestionCell>,
}

impl fmt::Debug for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately opaque: a derived `Debug` would walk the weak signal
        // list back through a live signal's `Arc<Group>`, recursing.
        f.debug_struct("Group").finish_non_exhaustive()
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        let Some(map) = self.map.upgrade() else {
            // The scheduler is gone; its map went with it.
            return;
        };
        let mut groups = map.groups.lock().unwrap();
        // Remove this path's entry — but only if it is still *this* group. A
        // path that was re-registered after this group's last handle dropped
        // holds a newer group, whose entry must stay.
        let is_self = groups
            .get(&self.key)
            .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), self as *const Group));
        if is_self {
            groups.remove(&self.key);
        }
    }
}

/// A per-egress-path congestion-signalling router. One instance governs one egress path;
/// construct as many as there are egress paths, or as many as your topology wants.
/// Within it, [`CcSignalHub::group`] gives each egress path its own domain.
#[derive(Clone)]
pub struct CcSignalHub {
    map: Arc<PathMap>,
}

impl fmt::Debug for CcSignalHub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CcSignalHub").finish_non_exhaustive()
    }
}

impl Default for CcSignalHub {
    fn default() -> Self {
        Self::new()
    }
}

impl CcSignalHub {
    pub fn new() -> Self {
        Self::with_epoch(Instant::now())
    }

    fn with_epoch(start: Instant) -> Self {
        Self::with_standoff_and_factor(start, true, STANDOFF_DECREASE_FACTOR)
    }

    /// The one constructor: the hub's stand-off enable flag and the competing
    /// response's multiplicative-decrease factor.
    fn with_standoff_and_factor(start: Instant, standoff: bool, decrease_factor: f64) -> Self {
        Self {
            map: Arc::new(PathMap {
                start,
                standoff,
                standoff_decrease_factor: decrease_factor,
                groups: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// A hub whose bulk connections keep the shipped delay-first policy: the
    /// path signal still suppresses the loss gate, but the interactive
    /// stand-off is disarmed.  Test-only (behind `testing`), the control arm
    /// that isolates the stand-off from the rest of the CC signal.
    #[cfg(feature = "testing")]
    pub fn without_standoff() -> Self {
        Self::with_standoff_and_factor(Instant::now(), false, STANDOFF_DECREASE_FACTOR)
    }

    /// A hub whose bulk stand-off applies `decrease_factor` on a sampled loss
    /// instead of [`STANDOFF_DECREASE_FACTOR`].  Test-only: the bulk lane's
    /// multiplicative-decrease factor is otherwise one shared constant, so this
    /// is the only way a scenario can measure the share a *gentler* competing
    /// decrease claims against a competitor that keeps halving.
    #[cfg(feature = "testing")]
    pub fn with_standoff_decrease_factor(decrease_factor: f64) -> Self {
        Self::with_standoff_and_factor(Instant::now(), true, decrease_factor)
    }

    /// The presence domain for one egress path, identified by the local source
    /// address and the remote destination address. Connections that pass
    /// different pairs never see each other, so a badly-connected client cannot
    /// change another client's congestion response, and two flows to different
    /// destinations do not share merely because they leave the same host.
    pub fn group(&self, src: IpAddr, dst: IpAddr) -> CcSignalGroup {
        let key = (src, dst);
        let group = {
            let mut groups = self.map.groups.lock().unwrap();
            // Defensive sweep: a group whose last handle dropped during this
            // call window is removed even if its `Drop` lost the race.
            groups.retain(|_, group| group.strong_count() > 0);
            match groups.get(&key).and_then(Weak::upgrade) {
                Some(group) => group,
                None => {
                    let group = Arc::new(Group::new(key, &self.map));
                    groups.insert(key, Arc::downgrade(&group));
                    group
                }
            }
        };
        CcSignalGroup {
            group,
            start: self.map.start,
        }
    }
}

/// Which side of the arbitration a connection is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CcRole {
    /// Publishes presence to its path's group.
    Interactive,
    /// Consumes the path's CC signal.
    Bulk,
}

/// A connection's attachment to a per-egress-path [`CcSignalHub`]: which scheduler,
/// and which role. Carried on the transport connect/accept config; the
/// transport resolves the path's [`CcSignalGroup`] from the socket's own addresses,
/// so the `(src, dst)` pair is derived where the socket is, not guessed by the
/// caller.
#[derive(Debug, Clone)]
pub struct CcLink {
    scheduler: CcSignalHub,
    role: CcRole,
}

impl CcLink {
    pub fn new(scheduler: CcSignalHub, role: CcRole) -> Self {
        Self { scheduler, role }
    }

    pub fn role(&self) -> CcRole {
        self.role
    }

    /// This connection's path group, keyed by the addresses the socket actually
    /// uses.
    pub(crate) fn group(&self, src: IpAddr, dst: IpAddr) -> CcSignalGroup {
        self.scheduler.group(src, dst)
    }
}

impl Group {
    fn new(key: (IpAddr, IpAddr), map: &Arc<PathMap>) -> Self {
        Self {
            key,
            standoff: map.standoff,
            standoff_decrease_factor: map.standoff_decrease_factor,
            map: Arc::downgrade(map),
            signals: Mutex::new(Vec::new()),
            aggregate: Arc::new(CongestionCell::new(map.start)),
        }
    }
}

/// One egress path's isolated domain on a egress path. Cheap to clone.
#[derive(Clone)]
pub struct CcSignalGroup {
    group: Arc<Group>,
    start: Instant,
}

impl fmt::Debug for CcSignalGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CcSignalGroup").finish_non_exhaustive()
    }
}

impl CcSignalGroup {
    /// Register an interactive connection and return its signal. Attach
    /// [`CcSignalSource::observer`] to that connection's `metrics_observer` so this
    /// path is marked shared while the lane is live.
    pub fn interactive(&self) -> CcSignalSource {
        let signal = CcSignalSource::new(&self.group, self.start);
        let mut signals = self.group.signals.lock().unwrap();
        // Drop signals whose connection has ended before adding this one, so
        // the list stays proportional to the live interactive connections.
        signals.retain(|signal| signal.strong_count() > 0);
        signals.push(Arc::downgrade(&signal.inner));
        signal
    }

    /// The signal to carry on one bulk connection's connect/accept config.
    pub fn bulk(&self) -> CcSignal {
        CcSignal {
            group: Arc::clone(&self.group),
        }
    }
}

/// Re-derive a group's sharedness from its live interactive connections. A
/// signal older than [`SIGNAL_STALE_AFTER`] counts as gone.
fn recompute(group: &Arc<Group>) {
    let now_ms = group.aggregate.start.elapsed().as_millis() as u64;
    let stale = SIGNAL_STALE_AFTER.as_millis() as u64;
    let mut signals = group.signals.lock().unwrap();
    signals.retain(|signal| signal.strong_count() > 0);
    let mut shared = false;
    for signal in signals.iter().filter_map(Weak::upgrade) {
        shared |= now_ms.saturating_sub(signal.updated_ms.load(Ordering::Acquire)) <= stale;
    }
    group.aggregate.store(shared, now_ms);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// `(src, dst)` paths. `CLIENT_A` and `CLIENT_B` share the local egress but
    /// differ in destination — exactly the two-access-server case — and
    /// `OTHER_EXIT` differs in source.
    const CLIENT_A: (IpAddr, IpAddr) = (
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
    );
    const CLIENT_B: (IpAddr, IpAddr) = (
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2)),
    );
    const OTHER_EXIT: (IpAddr, IpAddr) = (
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
    );

    fn g(scheduler: &CcSignalHub, path: (IpAddr, IpAddr)) -> CcSignalGroup {
        scheduler.group(path.0, path.1)
    }

    fn sample() -> MetricsSnapshot {
        MetricsSnapshot::default()
    }

    #[test]
    fn a_path_is_not_shared_until_an_interactive_lane_publishes() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        assert!(!bulk.is_shared());
        let interactive = group.interactive();
        interactive.update(&sample());
        assert!(bulk.is_shared());
    }

    #[test]
    fn any_live_interactive_lane_marks_the_path_shared() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let a = group.interactive();
        let b = group.interactive();
        a.update(&sample());
        assert!(bulk.is_shared());
        // One lane ending leaves the path shared while the other is live.
        drop(a);
        b.update(&sample());
        assert!(bulk.is_shared());
    }

    /// A lane that goes quiet must stop marking its path shared.
    #[test]
    fn an_expired_lane_no_longer_marks_its_path_shared() {
        // Epoch two horizons in the past, so backdating an update to 0 is
        // beyond the staleness horizon without waiting on the wall clock.
        let scheduler = CcSignalHub::with_epoch(Instant::now() - SIGNAL_STALE_AFTER * 2);
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        interactive.update(&sample());
        assert!(bulk.is_shared());
        interactive.inner.updated_ms.store(0, Ordering::Release);
        recompute(&group.group);
        assert!(!bulk.is_shared());
    }

    /// A fresh path that has never carried an interactive lane has no quiet
    /// clock: `None`, not `Some(0)`, so the stand-off can fall back to the
    /// connection's own age instead of competing from the first sample.
    #[test]
    fn a_never_used_path_has_no_quiet_clock() {
        let scheduler = CcSignalHub::new();
        let bulk = g(&scheduler, CLIENT_A).bulk();
        assert_eq!(bulk.quiet_for(), None);
        assert!(!bulk.is_shared());
    }

    /// Once an interactive lane has published, the quiet clock runs from its
    /// last update: it is `None` before the first update, near zero right
    /// after one, and grows as the lane goes silent.
    #[test]
    fn the_quiet_clock_runs_from_the_last_interactive_update() {
        // Epoch three seconds in the past so a backdated update exercises the
        // elapsed-time arithmetic without waiting on the wall clock.
        let scheduler = CcSignalHub::with_epoch(Instant::now() - Duration::from_secs(3));
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        assert_eq!(bulk.quiet_for(), None, "no update yet means no clock");
        interactive.update(&sample());
        let fresh = bulk.quiet_for().expect("an update starts the quiet clock");
        assert!(
            fresh < Duration::from_millis(200),
            "the clock restarts at the update, not at the epoch: {fresh:?}"
        );
        // Backdate the aggregate's last-update stamp and re-derive: the clock
        // measures the silence since the update, not since the epoch.  (The
        // aggregate, not the signal, carries the clock: it is the cell every
        // bulk handle reads.)
        group
            .group
            .aggregate
            .updated_ms
            .store(1_000, Ordering::Release);
        let quiet = bulk
            .quiet_for()
            .expect("the clock keeps running once started");
        assert!(
            quiet >= Duration::from_secs(1) && quiet < Duration::from_secs(3),
            "the clock must measure the silence since the update: {quiet:?}"
        );
    }

    /// An `RttSample` is **not** an application offer: the offer clock stays
    /// `None` while the RTT clock marks the path shared.  This separation is
    /// the whole fix — reading the RTT clock made a queued lane look idle.
    #[test]
    fn an_rtt_update_is_not_an_application_offer() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        interactive.update(&sample());
        assert!(bulk.is_shared(), "the RTT update marks the path shared");
        assert_eq!(
            bulk.offered_quiet_for(),
            None,
            "an RTT update is not an application offer"
        );
    }

    /// The offer clock restarts at each offer, measures silence since it, and
    /// ignores RTT updates entirely; it also forgets a lane once it is gone, so
    /// an ended connection cannot hold the gate shut forever.
    #[test]
    fn the_offer_clock_is_the_standoffs_activity_witness() {
        // Epoch three seconds in the past so backdating the offer exercises
        // the elapsed-time arithmetic without waiting on the wall clock.
        let scheduler = CcSignalHub::with_epoch(Instant::now() - Duration::from_secs(3));
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        assert_eq!(
            bulk.offered_quiet_for(),
            None,
            "no offer yet means no clock"
        );
        interactive.offer();
        let fresh = bulk
            .offered_quiet_for()
            .expect("an offer starts the offer clock");
        assert!(
            fresh < Duration::from_millis(200),
            "the offer clock restarts at the offer: {fresh:?}"
        );
        // An RTT update must not refresh the offer clock: the stored offer
        // stamp is unchanged by an `update`.
        let stamp = interactive.inner.offer_ms.load(Ordering::Acquire);
        interactive.update(&sample());
        assert_eq!(
            interactive.inner.offer_ms.load(Ordering::Acquire),
            stamp,
            "an RTT update must not refresh the offer clock"
        );
        // Backdate the signal's offer stamp: the clock measures silence since
        // the offer, not since the epoch.
        interactive.inner.offer_ms.store(1_000, Ordering::Release);
        let quiet = bulk
            .offered_quiet_for()
            .expect("the offer clock keeps running once started");
        assert!(
            quiet >= Duration::from_secs(1) && quiet < Duration::from_secs(3),
            "the clock must measure silence since the offer: {quiet:?}"
        );
        // A dead lane's offer stops counting: with no live signal the path is
        // no longer witnessed as offering, so the gate cannot stay shut on a
        // connection that has ended.
        drop(interactive);
        assert_eq!(
            bulk.offered_quiet_for(),
            None,
            "a dropped lane must not keep the offer clock alive"
        );
    }

    /// A badly-connected client must not change another client's response.
    #[test]
    fn one_client_does_not_mark_another_clients_path_shared() {
        let scheduler = CcSignalHub::new();
        let bulk_b = g(&scheduler, CLIENT_B).bulk();
        g(&scheduler, CLIENT_A).interactive().update(&sample());
        assert!(!bulk_b.is_shared());
    }

    #[test]
    fn a_different_local_egress_is_a_different_path() {
        let scheduler = CcSignalHub::new();
        let bulk = g(&scheduler, OTHER_EXIT).bulk();
        g(&scheduler, CLIENT_A).interactive().update(&sample());
        assert!(!bulk.is_shared());
    }

    /// The path map must not grow with every path ever seen: a group with no
    /// live connection removes its own entry.
    #[test]
    fn the_path_map_and_signal_list_do_not_leak() {
        let scheduler = CcSignalHub::new();
        for i in 0..250u8 {
            let dst = IpAddr::V4(Ipv4Addr::new(203, 0, 113, i));
            let group = scheduler.group(CLIENT_A.0, dst);
            let signal = group.interactive();
            signal.update(&sample());
            drop(group);
        }
        let live = scheduler.map.groups.lock().unwrap().len();
        assert_eq!(
            live, 0,
            "{live} dead path groups survived their connections: the map leaks"
        );
    }

    /// A long-lived group whose interactive connections come and go must not
    /// accumulate dead signal slots.
    #[test]
    fn a_groups_signal_list_drops_finished_connections() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let _bulk = group.bulk();
        for _ in 0..1_000 {
            let signal = group.interactive();
            signal.update(&sample());
            drop(signal);
        }
        let registered = group.group.signals.lock().unwrap().len();
        assert!(
            registered <= 1,
            "{registered} signal slots survived their connections: the group leaks"
        );
    }

    #[test]
    fn independent_schedulers_do_not_share_signals() {
        let a = CcSignalHub::new();
        let b = CcSignalHub::new();
        let bulk_b = g(&b, CLIENT_A).bulk();
        g(&a, CLIENT_A).interactive().update(&sample());
        assert!(!bulk_b.is_shared());
    }

    /// A snapshot with the whole cross-lane payload populated, so each test can
    /// vary exactly the field it is about.
    fn payload_sample(control_rtt: Duration) -> MetricsSnapshot {
        MetricsSnapshot {
            send_rate_packets_per_second: 40.0,
            pending_send_bytes: 0,
            application_write_waiters: 0,
            congestion_persistent_queue_for: None,
            congestion_rtt_floor: Some(Duration::from_millis(50)),
            congestion_queue_tolerance: Some(Duration::from_millis(6)),
            congestion_loss_ratio: Some(0.0),
            congestion_control_rtt: Some(control_rtt),
            congestion_delivery_peak_packets_per_second: Some(1200.0),
            delivery_sample_app_limited: Some(false),
            ..MetricsSnapshot::default()
        }
    }

    #[test]
    fn a_path_has_no_payload_until_a_lane_publishes_one() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        assert_eq!(bulk.state(), None);
        let interactive = group.interactive();
        interactive.update(&payload_sample(Duration::from_millis(100)));
        assert!(bulk.state().is_some());
    }

    /// The group aggregate is `min` for the queue-free baselines, `max` for the
    /// longest latch and the worst loss, and `sum` for the activity counters —
    /// so a bulk lane reads the tightest floor, the least-queued lane's queue
    /// delay, and the group's total activity.
    #[test]
    fn the_payload_aggregates_as_min_max_and_sum_over_fresh_lanes() {
        let scheduler = CcSignalHub::new();
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let a = group.interactive();
        let b = group.interactive();
        a.update(&MetricsSnapshot {
            send_rate_packets_per_second: 40.0,
            pending_send_bytes: 5,
            application_write_waiters: 1,
            congestion_persistent_queue_for: Some(Duration::from_millis(20)),
            congestion_rtt_floor: Some(Duration::from_millis(50)),
            congestion_queue_tolerance: Some(Duration::from_millis(6)),
            congestion_loss_ratio: Some(0.1),
            congestion_control_rtt: Some(Duration::from_millis(100)),
            ..MetricsSnapshot::default()
        });
        b.update(&MetricsSnapshot {
            send_rate_packets_per_second: 10.0,
            pending_send_bytes: 0,
            application_write_waiters: 0,
            congestion_persistent_queue_for: None,
            congestion_rtt_floor: Some(Duration::from_millis(30)),
            congestion_queue_tolerance: Some(Duration::from_millis(3)),
            congestion_loss_ratio: Some(0.3),
            congestion_control_rtt: Some(Duration::from_millis(80)),
            ..MetricsSnapshot::default()
        });
        let state = bulk.state().expect("two fresh lanes must aggregate");
        assert_eq!(state.floor, Some(Duration::from_millis(30)));
        assert_eq!(state.tolerance, Some(Duration::from_millis(3)));
        assert_eq!(state.persistent_for, Some(Duration::from_millis(20)));
        assert_eq!(state.control_rtt, Some(Duration::from_millis(100)));
        assert_eq!(state.pending_bytes, 5);
        assert_eq!(state.write_waiters, 1);
        assert_eq!(state.offered_pps, 50.0);
        assert_eq!(state.loss, Some(0.3));
        // The least-queued lane is `b`: 80 ms control RTT less its 30 ms floor.
        assert_eq!(state.queue_delay, Some(Duration::from_millis(50)));
    }

    /// The freshness policy, the heartbeat, and the vacuity each exists for.
    /// A **live** lane that has gone quiet is heartbeated back to a fresh
    /// *idle* payload, so R1 reads a fresh idle instead of an absent payload;
    /// a lane whose last publication is older than [`SIGNAL_STALE_AFTER`] is
    /// gone and reads as **absent** (never as idle), so an absent payload
    /// keeps meaning "no information". The horizon also scales with the
    /// publisher's own control RTT.
    #[test]
    fn a_live_lane_heartbeats_while_a_dead_one_reads_as_absent() {
        let scheduler = CcSignalHub::with_epoch(Instant::now() - Duration::from_secs(3));
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        interactive.update(&payload_sample(Duration::from_millis(100)));
        assert!(
            bulk.state().is_some(),
            "a just-published payload must be fresh"
        );
        // The lane has not sampled for one control RTT but is still live: the
        // heartbeat republishes its idle state rather than letting the payload
        // vanish. `now` is ~3000 ms since the epoch, so a 2750 ms stamp is
        // ~250 ms old against a 100 ms horizon.
        interactive.inner.updated_ms.store(2_750, Ordering::Release);
        let heartbeat = bulk
            .state()
            .expect("a live idle lane must heartbeat, not vanish");
        assert_eq!(
            heartbeat.pending_bytes, 0,
            "the idle heartbeat carries no pending bytes"
        );
        assert_eq!(
            heartbeat.write_waiters, 0,
            "the idle heartbeat carries no blocked writer"
        );
        assert_eq!(
            heartbeat.offered_pps, 0.0,
            "the idle heartbeat carries no offer"
        );
        // Past the presence horizon the lane is gone and the heartbeat must not
        // resurrect it: absent means unknown, not idle.
        interactive.inner.updated_ms.store(1_000, Ordering::Release);
        assert_eq!(
            bulk.state(),
            None,
            "a lane gone past the presence horizon must read as absent, not as idle"
        );
        // A wider control RTT widens the horizon and the same stamp is fresh
        // again, proving the horizon is the publisher's own control RTT.
        interactive.inner.updated_ms.store(2_750, Ordering::Release);
        interactive.inner.control_rtt_ns.store(
            Duration::from_millis(500).as_nanos() as u64,
            Ordering::Relaxed,
        );
        assert!(
            bulk.state().is_some(),
            "the horizon must scale with the control RTT the payload carries"
        );
    }

    /// The heartbeat must not turn a **busy** lane into an idle one: while the
    /// lane is still offering, the last sampled activity triple stands and the
    /// bulk's R1 claim stays blocked. Without this the heartbeat would itself
    /// manufacture the idle evidence R1 exists to require.
    #[test]
    fn an_offering_lane_is_not_heartbeated_to_idle() {
        let scheduler = CcSignalHub::with_epoch(Instant::now() - Duration::from_secs(3));
        let group = g(&scheduler, CLIENT_A);
        let bulk = group.bulk();
        let interactive = group.interactive();
        interactive.update(&payload_sample(Duration::from_millis(100)));
        // The application is still offering: the offer clock is current even
        // though no new `RttSample` has arrived for more than one control RTT.
        interactive.offer();
        interactive.inner.updated_ms.store(2_750, Ordering::Release);
        let state = bulk.state().expect("a live offering lane must stay fresh");
        assert_eq!(
            state.offered_pps, 40.0,
            "an offering lane's sampled rate must survive the heartbeat"
        );
        assert!(
            !crate::traffic_shaping::core::congestion_response::SharedPath::from_state(&state)
                .lane_idle(),
            "an offering lane must not read as idle"
        );
    }

    /// The heartbeat interval is the tighter of the payload's own validity and
    /// the gate horizon it feeds, and a lane that cannot date its fields has
    /// none.
    #[test]
    fn the_heartbeat_interval_is_the_tighter_of_validity_and_window() {
        assert_eq!(
            heartbeat_interval(Some(Duration::from_millis(100))),
            Some(Duration::from_millis(100)),
            "a short control RTT keeps the payload inside its own validity window"
        );
        assert_eq!(
            heartbeat_interval(Some(Duration::from_secs(10))),
            Some(STANDOFF_WINDOW),
            "a long control RTT keeps the payload as fresh as the gate horizon"
        );
        assert_eq!(
            heartbeat_interval(None),
            None,
            "a lane with no control RTT cannot be dated"
        );
        assert_eq!(
            heartbeat_interval(Some(Duration::ZERO)),
            None,
            "a zero control RTT cannot date a payload"
        );
    }

    /// The shipped stand-off decrease factor is the certified three quarters,
    /// and a production hub carries it: the constant and the hub's readback
    /// are pinned together so neither can drift from the other.  A change here
    /// moves the deployed bulk lane's multiplicative-decrease response on every
    /// path.
    #[test]
    fn the_shipped_standoff_decrease_factor_is_the_certified_three_quarters() {
        assert_eq!(
            STANDOFF_DECREASE_FACTOR, 0.75,
            "the shipped stand-off decrease factor must be the certified 0.75"
        );
        let scheduler = CcSignalHub::new();
        let production = g(&scheduler, CLIENT_A).bulk().standoff_decrease_factor();
        assert_eq!(
            production, 0.75,
            "a production hub must read back the shipped stand-off factor"
        );
    }

    /// The per-path test hook still overrides the shipped default, so the beta
    /// arms can sweep a factor the production hub never carries (the `0.5`
    /// control and the `0.9` alternate).
    #[cfg(feature = "testing")]
    #[test]
    fn the_test_hook_still_overrides_the_shipped_standoff_decrease_factor() {
        let scheduler = CcSignalHub::with_standoff_decrease_factor(0.5);
        let overridden = g(&scheduler, CLIENT_A).bulk().standoff_decrease_factor();
        assert_eq!(
            overridden, 0.5,
            "the test hook must apply the factor it was given"
        );
        assert_ne!(
            overridden, STANDOFF_DECREASE_FACTOR,
            "the overridden factor must differ from the shipped default"
        );
    }
}
