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

    /// How long the interactive lane has been quiet on this path: the wall time
    /// since its last `RttSample`-driven update, or `None` when no interactive
    /// lane has ever published here. A fresh path that has never seen an
    /// interactive lane therefore reports `None`, and a bulk lane that needs a
    /// quiet clock falls back to its own connection age (see
    /// `CongestionResponse`).
    pub fn quiet_for(&self) -> Option<Duration> {
        self.group.aggregate.quiet_for()
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
    /// Reset on every observation. There is no other state: presence is the
    /// whole signal.
    updated_ms: AtomicU64,
    min_rtt_ns: AtomicU64,
    srtt_ns: AtomicU64,
}

impl CcSignalSource {
    fn new(group: &Arc<Group>, start: Instant) -> Self {
        Self {
            inner: Arc::new(SignalState {
                group: Arc::clone(group),
                start,
                updated_ms: AtomicU64::new(0),
                min_rtt_ns: AtomicU64::new(0),
                srtt_ns: AtomicU64::new(0),
            }),
        }
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
        Self::with_epoch_and_standoff(start, true)
    }

    /// A hub whose bulk connections run the interactive stand-off (production).
    fn with_epoch_and_standoff(start: Instant, standoff: bool) -> Self {
        Self {
            map: Arc::new(PathMap {
                start,
                standoff,
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
        Self::with_epoch_and_standoff(Instant::now(), false)
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
}
