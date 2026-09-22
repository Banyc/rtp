use core::num::NonZeroUsize;
use primitive::io::token_bucket::TokenBucket;
use primitive::ops::float::PosR;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TARGET_WAKE_INTERVAL: Duration = Duration::from_millis(1);
// The bucket's capacity is two wake intervals of tokens, but it is never
// allowed to drop below this many packets.  The floor exists so a late wake
// can still send the accrued catch-up batch (integer tokens beyond the
// capacity are discarded, so too small a floor silently loses throughput
// after scheduler jitter).  It must stay small: the old value of 64 let an
// idle link credit ~64 packets, enough to dump ~90 ms of queue at 1 MiB/s and
// spike interactive latency.
const MIN_BURST_PACKETS: usize = 16;
const MAX_BURST_PACKETS: usize = 512;

/// A cloneable synchronized send pacer.  Every operation is a synchronous
/// complete-lock call, so no mutex guard can survive an await: callers pass
/// the whole operation as a closure (`with_token_bucket`) or use the
/// token-accounting convenience methods, and the lock never leaks.
#[derive(Clone, Debug)]
pub(crate) struct SendPacer {
    state: Arc<Mutex<PacerState>>,
}

#[derive(Debug)]
struct PacerState {
    bucket: TokenBucket,
    rate: PosR<f64>,
    capacity: NonZeroUsize,
    /// The floor applied to `capacity`.  It is `MIN_BURST_PACKETS` in
    /// production; tests pin it to the legacy value so non-pacer suites keep
    /// exercising the algorithms they were written for.
    min_burst: usize,
}

impl SendPacer {
    pub(crate) fn new_prefilled(rate: PosR<f64>, now: Instant) -> Self {
        let capacity = burst_capacity(rate, MIN_BURST_PACKETS);
        Self {
            state: Arc::new(Mutex::new(PacerState::with_tokens(
                rate,
                capacity,
                capacity.get(),
                now,
                MIN_BURST_PACKETS,
            ))),
        }
    }

    pub(crate) fn set_rate(&self, rate: PosR<f64>, now: Instant) {
        let mut state = self.state.lock().unwrap();
        state.bucket.gen_tokens(now);
        let tokens = state.bucket.outdated_coined_tokens();
        let min_burst = state.min_burst;
        let capacity = burst_capacity(rate, min_burst);
        *state =
            PacerState::with_tokens(rate, capacity, tokens.min(capacity.get()), now, min_burst);
    }

    /// Test-only: pin the capacity floor to `min_burst` and refill the bucket.
    /// Non-pacer suites need a working burst (the historical 64-packet floor)
    /// so their send/recovery assertions are not re-specified around the
    /// deliberately smaller interactive-rate burst; the pacer's own tests cover
    /// the production floor.
    #[cfg(test)]
    pub(crate) fn set_min_burst_for_test(&self, min_burst: usize, now: Instant) {
        let mut state = self.state.lock().unwrap();
        let rate = state.rate;
        let capacity = burst_capacity(rate, min_burst);
        *state = PacerState::with_tokens(rate, capacity, capacity.get(), now, min_burst);
    }

    #[cfg(test)]
    pub(crate) fn gen_tokens(&self, now: Instant) -> usize {
        self.state.lock().unwrap().bucket.gen_tokens(now)
    }

    pub(crate) fn take_exact_tokens(&self, tokens: usize, now: Instant) -> bool {
        self.state
            .lock()
            .unwrap()
            .bucket
            .take_exact_tokens(tokens, now)
    }

    #[cfg(test)]
    pub(crate) fn take_at_most_tokens(&self, tokens: usize, now: Instant) -> usize {
        self.state
            .lock()
            .unwrap()
            .bucket
            .take_at_most_tokens(tokens, now)
    }

    pub(crate) fn outdated_tokens(&self) -> f64 {
        self.state.lock().unwrap().bucket.outdated_tokens()
    }

    /// Run `use_bucket` against the token bucket while the pacer lock is held,
    /// returning its result.  The closure must not await: the lock is held for
    /// the whole call.
    pub(crate) fn with_token_bucket<R>(&self, use_bucket: impl FnOnce(&mut TokenBucket) -> R) -> R {
        use_bucket(&mut self.state.lock().unwrap().bucket)
    }

    #[cfg(test)]
    fn next_token_time(&self) -> Instant {
        self.state.lock().unwrap().bucket.next_token_time()
    }

    pub(crate) fn next_batch_time(&self, now: Instant, max_sendable_packets: usize) -> Instant {
        let mut state = self.state.lock().unwrap();
        let max_batch = max_sendable_packets.max(1).min(state.capacity.get());
        let target_batch = ((state.rate.get() * TARGET_WAKE_INTERVAL.as_secs_f64()).ceil()
            as usize)
            .clamp(1, max_batch);
        state.bucket.gen_tokens(now);
        let available = state.bucket.outdated_tokens();
        if available >= target_batch as f64 {
            return now;
        }
        let missing = target_batch as f64 - available;
        now + Duration::from_secs_f64(missing / state.rate.get())
    }
}

impl PacerState {
    fn with_tokens(
        rate: PosR<f64>,
        capacity: NonZeroUsize,
        tokens: usize,
        now: Instant,
        min_burst: usize,
    ) -> Self {
        let tokens = tokens.min(capacity.get());
        let mut backdate = Duration::from_secs_f64((tokens as f64 + 0.5) / rate.get());
        let start = loop {
            match now.checked_sub(backdate) {
                Some(start) => break start,
                None => {
                    backdate /= 2;
                    if backdate.is_zero() {
                        break now;
                    }
                }
            }
        };
        let mut bucket = TokenBucket::new(rate, capacity, start);
        bucket.gen_tokens(now);
        Self {
            bucket,
            rate,
            capacity,
            min_burst,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendWake {
    Event,
    Pacing(Instant),
    Protocol(Instant),
}

impl SendWake {
    pub(crate) fn after_send_pass(
        now: Instant,
        pacing_deadline: Option<Instant>,
        mut protocol_deadline: Option<Instant>,
    ) -> Self {
        // The pass has already processed every protocol deadline at or before
        // `now`. An outage retransmission can leave its RTO overdue only
        // because the pacer has no token yet; in that case the pacing deadline
        // is the next useful wake rather than the already-consumed RTO.
        if pacing_deadline.is_some() && protocol_deadline.is_some_and(|deadline| deadline <= now) {
            protocol_deadline = None;
        }
        match (pacing_deadline, protocol_deadline) {
            (Some(pacing), Some(protocol)) if pacing <= protocol => Self::Pacing(pacing),
            (Some(_), Some(protocol)) => Self::Protocol(protocol),
            (Some(pacing), None) => Self::Pacing(pacing),
            (None, Some(protocol)) => Self::Protocol(protocol),
            (None, None) => Self::Event,
        }
    }
}

/// Capacity of the send pacer's token bucket, in packets: two wake intervals
/// of tokens, bounded below by `min_burst` and above by `MAX_BURST_PACKETS`.
/// The upper bound is unchanged so high rates, where the rate-scaled term
/// dominates, keep their exact previous behaviour; only the small-rate floor is
/// lowered to stop an idle bulk transfer from front-loading a queue the
/// interactive path must wait behind.
fn burst_capacity(rate: PosR<f64>, min_burst: usize) -> NonZeroUsize {
    let burst = (rate.get() * 2.0 * TARGET_WAKE_INTERVAL.as_secs_f64()).floor() as usize;
    NonZeroUsize::new(burst.clamp(min_burst, MAX_BURST_PACKETS)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(packets_per_second: f64) -> PosR<f64> {
        PosR::new(packets_per_second).unwrap()
    }

    #[test]
    fn prefilled_pacer_has_immediate_tokens_and_a_fresh_clock() {
        let now = Instant::now();
        let pacer = SendPacer::new_prefilled(rate(128.0), now);
        assert_eq!(pacer.gen_tokens(now), MIN_BURST_PACKETS);
        assert!(pacer.next_token_time() >= now);
    }

    #[test]
    fn rate_change_preserves_credited_tokens_and_clamps_capacity() {
        let now = Instant::now();
        let pacer = SendPacer::new_prefilled(rate(1_000_000.0), now);
        assert_eq!(
            pacer.take_at_most_tokens(usize::MAX, now),
            MAX_BURST_PACKETS
        );
        let later = now + Duration::from_millis(1);
        pacer.set_rate(rate(128.0), later);
        assert_eq!(pacer.gen_tokens(later), MIN_BURST_PACKETS);
        assert!(pacer.next_token_time() >= later);
    }

    #[test]
    fn sparse_work_waits_for_one_token() {
        let now = Instant::now();
        let pacer = SendPacer::new_prefilled(rate(10_000.0), now);
        pacer.take_at_most_tokens(usize::MAX, now);
        assert_eq!(
            pacer.next_batch_time(now, 1),
            now + Duration::from_micros(50)
        );
    }

    #[test]
    fn backlogged_work_waits_for_a_timer_batch() {
        let now = Instant::now();
        let pacer = SendPacer::new_prefilled(rate(10_000.0), now);
        pacer.take_at_most_tokens(usize::MAX, now);
        assert_eq!(
            pacer.next_batch_time(now, 64),
            now + Duration::from_micros(950)
        );
    }

    #[test]
    fn available_batch_is_immediate() {
        let now = Instant::now();
        let pacer = SendPacer::new_prefilled(rate(10_000.0), now);
        assert_eq!(pacer.next_batch_time(now, 64), now);
    }

    /// The send driver defers a resume notification to an armed pacing
    /// deadline, so that deadline must stay within the pacer's target wake
    /// interval: the batch is `ceil(rate * TARGET_WAKE_INTERVAL)` tokens, and
    /// the fractional coining token can leave at most one token period of
    /// slack.  A deadline that drifted further (a larger batch, a coarser
    /// interval) would silently turn the deferral into added send latency.
    #[test]
    fn a_pacing_deadline_stays_within_the_target_wake_interval() {
        assert_eq!(
            TARGET_WAKE_INTERVAL,
            Duration::from_millis(1),
            "the send driver defers a resume to an armed pacing deadline, so \
             this interval is the deferral bound; raising it adds send latency"
        );
        let now = Instant::now();
        for packets_per_second in [1.0, 10.0, 128.0, 1_000.0, 10_000.0, 1_000_000.0] {
            let rate = rate(packets_per_second);
            let token_period = Duration::from_secs_f64(1.0 / packets_per_second);
            let bound = TARGET_WAKE_INTERVAL + token_period;
            for max_sendable in [1usize, 2, 8, 64, 4096] {
                // Drain the prefill so the deadline is a real future wait.
                let pacer = SendPacer::new_prefilled(rate, now);
                pacer.take_at_most_tokens(usize::MAX, now);
                let deadline = pacer.next_batch_time(now, max_sendable);
                assert!(
                    deadline <= now + bound,
                    "rate {packets_per_second}, max_sendable {max_sendable}: deadline {:?} \
                     exceeds the {bound:?} deferral bound",
                    deadline.duration_since(now)
                );
            }
        }
    }

    #[test]
    fn wake_reason_preserves_the_earliest_deadline() {
        let now = Instant::now();
        let pacing = now + Duration::from_millis(2);
        let protocol = now + Duration::from_millis(5);
        assert_eq!(
            SendWake::after_send_pass(now, Some(pacing), Some(protocol)),
            SendWake::Pacing(pacing)
        );
        assert_eq!(
            SendWake::after_send_pass(now, None, Some(protocol)),
            SendWake::Protocol(protocol)
        );
        assert_eq!(SendWake::after_send_pass(now, None, None), SendWake::Event);
    }

    /// The tie at `pacing == protocol` must resolve to `Pacing`, not
    /// `Protocol`: the guard is INCLUSIVE (`pacing <= protocol`), so an
    /// already-fabricated deadline that collides with the pacing deadline
    /// still reports the pacing wake.
    #[test]
    fn a_tie_between_pacing_and_protocol_prefers_pacing() {
        let now = Instant::now();
        let tie = now + Duration::from_millis(7);
        assert_eq!(
            SendWake::after_send_pass(now, Some(tie), Some(tie)),
            SendWake::Pacing(tie),
            "the exact pacing/protocol tie must report Pacing, not Protocol"
        );
        // One instant earlier on each side stays on its own arm.
        let pacing = now + Duration::from_millis(7);
        let protocol = now + Duration::from_millis(8);
        assert_eq!(
            SendWake::after_send_pass(now, Some(pacing), Some(protocol)),
            SendWake::Pacing(pacing)
        );
        let pacing = now + Duration::from_millis(8);
        let protocol = now + Duration::from_millis(7);
        assert_eq!(
            SendWake::after_send_pass(now, Some(pacing), Some(protocol)),
            SendWake::Protocol(protocol)
        );
    }

    #[test]
    fn pacing_block_ignores_an_already_processed_protocol_deadline() {
        let now = Instant::now();
        let pacing = now + Duration::from_millis(1);
        let overdue_rto = now - Duration::from_millis(1);
        assert_eq!(
            SendWake::after_send_pass(now, Some(pacing), Some(overdue_rto)),
            SendWake::Pacing(pacing)
        );
    }

    /// The "already processed" test is `deadline <= now`, INCLUSIVE.  A
    /// protocol deadline that lands exactly on `now` was consumed by this very
    /// pass too, and it must not be reported back: the driver would wake
    /// immediately to re-run the pass with no pacing change, and since that
    /// pass still has no token the same `Protocol(now)` deadline would be
    /// re-fabricated, spinning until the pacing deadline.  The strictly-past
    /// case above cannot see this, because `now - 1 ms` satisfies both `<` and
    /// `<=`.  `Instant` equality is exact, so the boundary needs no tolerance.
    #[test]
    fn a_protocol_deadline_exactly_now_is_already_processed() {
        let now = Instant::now();
        let pacing = now + Duration::from_millis(2);
        assert_eq!(
            SendWake::after_send_pass(now, Some(pacing), Some(now)),
            SendWake::Pacing(pacing),
            "a protocol deadline equal to now was processed by this pass; the \
             next useful wake is the pacing deadline, not an immediate \
             Protocol(now) wake"
        );
    }

    #[test]
    fn burst_capacity_scales_with_rate() {
        assert_eq!(
            burst_capacity(rate(128.0), MIN_BURST_PACKETS).get(),
            MIN_BURST_PACKETS
        );
        let middle = burst_capacity(rate(100_000.0), MIN_BURST_PACKETS).get();
        assert!(middle > MIN_BURST_PACKETS);
        assert!(middle < MAX_BURST_PACKETS);
        assert_eq!(
            burst_capacity(rate(1_000_000.0), MIN_BURST_PACKETS).get(),
            MAX_BURST_PACKETS
        );
    }

    #[test]
    fn low_rate_floor_rejects_a_bulk_sized_burst() {
        // 1 MiB/s with ~1400-byte packets is ~750 pkt/s: `rate * 2 ms` is
        // below the floor, so capacity is exactly the floor.  The old
        // 64-packet floor let this rate dump ~64 packets (~90 ms of link
        // time) after any idle period; it must now be far smaller.
        let interactive = burst_capacity(rate(750.0), MIN_BURST_PACKETS).get();
        assert_eq!(interactive, MIN_BURST_PACKETS);
        assert!(
            interactive < 64,
            "low/interactive-rate burst {interactive} must not be an old-style bulk burst"
        );

        // A sub-packet-per-wake rate must still be floored (never zero) and
        // must stay bounded by the same small floor.
        let trickle = burst_capacity(rate(1.0), MIN_BURST_PACKETS).get();
        assert!(trickle >= 1);
        assert_eq!(trickle, MIN_BURST_PACKETS);
    }

    #[test]
    fn high_rate_burst_capacity_is_unchanged() {
        // Above the floor the rate-scaled term dominates, so these values are
        // identical to the pre-fix formula (`floor(rate * 2 ms)`, capped at
        // MAX): only the low-rate floor changed.
        assert_eq!(
            burst_capacity(rate(100_000.0), MIN_BURST_PACKETS).get(),
            200
        );
        assert_eq!(
            burst_capacity(rate(1_000_000.0), MIN_BURST_PACKETS).get(),
            MAX_BURST_PACKETS
        );
        assert_eq!(
            burst_capacity(rate(10_000_000.0), MIN_BURST_PACKETS).get(),
            MAX_BURST_PACKETS
        );
    }
}
