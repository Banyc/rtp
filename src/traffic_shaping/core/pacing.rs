use core::num::NonZeroUsize;
use primitive::io::token_bucket::TokenBucket;
use primitive::ops::float::PosR;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TARGET_WAKE_INTERVAL: Duration = Duration::from_millis(1);
const MIN_BURST_PACKETS: usize = 64;
const MAX_BURST_PACKETS: usize = 512;
/// The smallest credit a backoff carries when it collapses a whole-token
/// bucket: a few packets at the (much lower) new rate, so the pacer can keep
/// emitting while the controller re-observes the link and begins recovery -
/// a small fraction of `MIN_BURST_PACKETS`, and never the old-rate burst a
/// rebuild used to re-arm.  Capped by the old credit, it cannot accumulate
/// across repeated rebuilds.
const BACKOFF_BRIDGE_TOKENS: f64 = 6.0;

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
}

impl SendPacer {
    pub(crate) fn new_prefilled(rate: PosR<f64>, now: Instant) -> Self {
        let capacity = burst_capacity(rate);
        Self {
            state: Arc::new(Mutex::new(PacerState::with_tokens(
                rate,
                capacity,
                capacity.get(),
                now,
            ))),
        }
    }

    pub(crate) fn set_rate(&self, rate: PosR<f64>, now: Instant) {
        let mut state = self.state.lock().unwrap();
        state.bucket.gen_tokens(now);
        let old_rate = state.rate.get();
        let credit = state.bucket.outdated_tokens();
        let capacity = burst_capacity(rate);
        // A rate change scales the carried credit by new/old in BOTH
        // directions: the earned tokens represented `credit *
        // sec_per_token(old)` of send time, so at the new rate they are worth
        // exactly `credit * new/old` tokens.  Without the down-scaling a
        // congestion backoff would front-load a full old-rate burst the
        // instant the controller meant to back off, and every repeated
        // `set_rate` at the low end would re-mint it; without the up-scaling a
        // recovering rate would keep a collapsed sub-token credit while the
        // link is ready for more.  The fractional coining token rides along
        // inside the credit, so rebuilds never drop sub-token value.
        let mut scaled = credit * (rate.get() / old_rate);
        // A backoff that would shrink a whole-token credit below
        // `BACKOFF_BRIDGE_TOKENS` still carries the bridge floor: the pacer
        // must not hard-stop to zero at the backoff instant, and a few
        // packets still have to be able to flow while the controller
        // re-observes the (now clean) link and the new, lower rate's refill
        // engages.  The bridge is a small fraction of `MIN_BURST_PACKETS` -
        // never the old-rate burst the rebuild used to arm - and it is
        // capped by the old credit, so repeated rebuilds cannot accumulate
        // it beyond what a collapse of that credit could carry.
        if rate.get() < old_rate && credit >= 1.0 {
            scaled = scaled.max(BACKOFF_BRIDGE_TOKENS).min(credit);
        }
        *state = PacerState::with_credit(rate, capacity, scaled, now);
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
    fn with_tokens(rate: PosR<f64>, capacity: NonZeroUsize, tokens: usize, now: Instant) -> Self {
        let tokens = tokens.min(capacity.get());
        // Nudge the backdate by half a token so a float round-trip can never
        // under-coin the requested whole tokens.
        let backdate = Duration::from_secs_f64((tokens as f64 + 0.5) / rate.get());
        let start = backdated_start(now, backdate);
        let mut bucket = TokenBucket::new(rate, capacity, start);
        bucket.gen_tokens(now);
        Self {
            bucket,
            rate,
            capacity,
        }
    }

    /// Rebuild the bucket carrying a fractional credit: `gen_tokens(now)`
    /// lands on `floor(credit)` whole tokens plus the preserved fraction as
    /// the coining token, so a rate change keeps the sub-token credit that a
    /// whole-only rebuild would throw away.
    fn with_credit(rate: PosR<f64>, capacity: NonZeroUsize, credit: f64, now: Instant) -> Self {
        let credit = credit.clamp(0.0, capacity.get() as f64);
        let backdate = Duration::from_secs_f64(credit / rate.get());
        let start = backdated_start(now, backdate);
        let mut bucket = TokenBucket::new(rate, capacity, start);
        bucket.gen_tokens(now);
        Self {
            bucket,
            rate,
            capacity,
        }
    }
}

/// The virtual start of a backdated bucket: the latest `now - backdate` that
/// does not underflow the clock, halving the backdate if it would.
fn backdated_start(now: Instant, mut backdate: Duration) -> Instant {
    loop {
        match now.checked_sub(backdate) {
            Some(start) => return start,
            None => {
                backdate /= 2;
                if backdate.is_zero() {
                    return now;
                }
            }
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

fn burst_capacity(rate: PosR<f64>) -> NonZeroUsize {
    let burst = (rate.get() * 2.0 * TARGET_WAKE_INTERVAL.as_secs_f64()).floor() as usize;
    NonZeroUsize::new(burst.clamp(MIN_BURST_PACKETS, MAX_BURST_PACKETS)).unwrap()
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
    fn rate_decrease_scales_credit_and_never_rearms_a_burst() {
        let now = Instant::now();
        // Start with a full bucket at a high rate (capacity 512).
        let pacer = SendPacer::new_prefilled(rate(1_000_000.0), now);
        assert_eq!(pacer.gen_tokens(now), MAX_BURST_PACKETS);

        // Back off 1M pps -> 100k pps (new burst capacity 200).  The ~512
        // tokens earned at the old rate are worth 512.5 * 100k/1M = ~51.25
        // tokens at the new rate: the immediately-available burst must be the
        // scaled credit, bounded by the NEW capacity -- never the full
        // 200-token burst the old rebuild re-armed from the old-rate credit.
        let low_rate = rate(100_000.0);
        let new_capacity = burst_capacity(low_rate).get();
        assert!(new_capacity > MIN_BURST_PACKETS && new_capacity < MAX_BURST_PACKETS);
        let later = now + Duration::from_millis(1);
        pacer.set_rate(low_rate, later);
        let burst = pacer.take_at_most_tokens(usize::MAX, later);
        let expected = (MAX_BURST_PACKETS as f64 * 0.1).floor() as usize;
        assert!(
            burst.abs_diff(expected) <= 1,
            "expected ~{expected} scaled tokens after backoff, got {burst}"
        );
        assert!(
            burst < new_capacity,
            "burst {burst} not bounded by new capacity {new_capacity}"
        );
        // The fractional coining token survives the rebuild: the 0.25
        // fraction of the scaled credit puts the next token 7.5 us out, not
        // the 5 us a dropped fraction (0.5 nudge) would imply.
        let next = pacer.next_token_time();
        let after = later + Duration::from_micros(7);
        assert!(
            next >= after && next <= after + Duration::from_micros(1),
            "coining fraction was dropped: next token at {next:?}"
        );

        // A collapse (1M pps -> 1 pps) straight out of a full bucket never
        // front-loads a burst: the credit scales to 512.5 * 1/1M and only the
        // `BACKOFF_BRIDGE_TOKENS` floor lets a handful of packets flow at the
        // backoff instant - roughly a tenth of MIN_BURST_PACKETS, and far
        // below the old-rate burst (or even the new rate's 64-token burst
        // capacity) the old rebuild used to re-arm.
        let collapse = SendPacer::new_prefilled(rate(1_000_000.0), now);
        collapse.set_rate(rate(1.0), later);
        let burst = collapse.take_at_most_tokens(usize::MAX, later);
        assert!(
            (BACKOFF_BRIDGE_TOKENS as usize - 1..=BACKOFF_BRIDGE_TOKENS as usize + 1)
                .contains(&burst),
            "backoff must not front-load a burst, got {burst}"
        );
        assert!(burst < MIN_BURST_PACKETS);

        // Repeated set_rate at the low end hands back only what genuine
        // refill has earned since the last rebuild: 1.5 s at 1 pps is 1.5
        // tokens (one whole coin), never the 64-token burst a rebuild used to
        // re-mint from a sub-token balance.
        pacer.set_rate(rate(1.0), later);
        let t2 = later + Duration::from_millis(1500);
        pacer.set_rate(rate(1.0), t2);
        let burst2 = pacer.take_at_most_tokens(usize::MAX, t2);
        assert!(
            burst2 <= 1,
            "repeated set_rate re-armed a {burst2}-token burst"
        );
        assert!(burst2 < MIN_BURST_PACKETS);
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

    #[test]
    fn burst_capacity_scales_with_rate() {
        assert_eq!(burst_capacity(rate(128.0)).get(), MIN_BURST_PACKETS);
        let middle = burst_capacity(rate(100_000.0)).get();
        assert!(middle > MIN_BURST_PACKETS);
        assert!(middle < MAX_BURST_PACKETS);
        assert_eq!(burst_capacity(rate(1_000_000.0)).get(), MAX_BURST_PACKETS);
    }
}
