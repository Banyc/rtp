use core::num::NonZeroUsize;
use primitive::io::token_bucket::TokenBucket;
use primitive::ops::float::PosR;
use std::time::{Duration, Instant};

const TARGET_WAKE_INTERVAL: Duration = Duration::from_millis(1);
const MIN_BURST_PACKETS: usize = 64;
const MAX_BURST_PACKETS: usize = 512;

#[derive(Debug)]
pub(crate) struct SendPacer {
    bucket: TokenBucket,
    rate: PosR<f64>,
    capacity: NonZeroUsize,
}

impl SendPacer {
    pub(crate) fn new_prefilled(rate: PosR<f64>, now: Instant) -> Self {
        let capacity = burst_capacity(rate);
        Self::with_tokens(rate, capacity, capacity.get(), now)
    }

    pub(crate) fn set_rate(&mut self, rate: PosR<f64>, now: Instant) {
        self.bucket.gen_tokens(now);
        let tokens = self.bucket.outdated_coined_tokens();
        let capacity = burst_capacity(rate);
        *self = Self::with_tokens(rate, capacity, tokens.min(capacity.get()), now);
    }

    #[cfg(test)]
    pub(crate) fn gen_tokens(&mut self, now: Instant) -> usize {
        self.bucket.gen_tokens(now)
    }

    pub(crate) fn take_exact_tokens(&mut self, tokens: usize, now: Instant) -> bool {
        self.bucket.take_exact_tokens(tokens, now)
    }

    #[cfg(test)]
    pub(crate) fn take_at_most_tokens(&mut self, tokens: usize, now: Instant) -> usize {
        self.bucket.take_at_most_tokens(tokens, now)
    }

    pub(crate) fn outdated_tokens(&self) -> f64 {
        self.bucket.outdated_tokens()
    }

    pub(crate) fn token_bucket_mut(&mut self) -> &mut TokenBucket {
        &mut self.bucket
    }

    #[cfg(test)]
    fn next_token_time(&self) -> Instant {
        self.bucket.next_token_time()
    }

    pub(crate) fn next_batch_time(&mut self, now: Instant, max_sendable_packets: usize) -> Instant {
        let max_batch = max_sendable_packets.max(1).min(self.capacity.get());
        let target_batch = ((self.rate.get() * TARGET_WAKE_INTERVAL.as_secs_f64()).ceil() as usize)
            .clamp(1, max_batch);
        self.bucket.gen_tokens(now);
        let available = self.bucket.outdated_tokens();
        if available >= target_batch as f64 {
            return now;
        }
        let missing = target_batch as f64 - available;
        now + Duration::from_secs_f64(missing / self.rate.get())
    }

    fn with_tokens(rate: PosR<f64>, capacity: NonZeroUsize, tokens: usize, now: Instant) -> Self {
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

    pub(crate) fn deadline(self) -> Option<Instant> {
        match self {
            Self::Event => None,
            Self::Pacing(deadline) | Self::Protocol(deadline) => Some(deadline),
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
        let mut pacer = SendPacer::new_prefilled(rate(128.0), now);
        assert_eq!(pacer.gen_tokens(now), MIN_BURST_PACKETS);
        assert!(pacer.next_token_time() >= now);
    }

    #[test]
    fn rate_change_preserves_credited_tokens_and_clamps_capacity() {
        let now = Instant::now();
        let mut pacer = SendPacer::new_prefilled(rate(1_000_000.0), now);
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
        let mut pacer = SendPacer::new_prefilled(rate(10_000.0), now);
        pacer.take_at_most_tokens(usize::MAX, now);
        assert_eq!(
            pacer.next_batch_time(now, 1),
            now + Duration::from_micros(50)
        );
    }

    #[test]
    fn backlogged_work_waits_for_a_timer_batch() {
        let now = Instant::now();
        let mut pacer = SendPacer::new_prefilled(rate(10_000.0), now);
        pacer.take_at_most_tokens(usize::MAX, now);
        assert_eq!(
            pacer.next_batch_time(now, 64),
            now + Duration::from_micros(950)
        );
    }

    #[test]
    fn available_batch_is_immediate() {
        let now = Instant::now();
        let mut pacer = SendPacer::new_prefilled(rate(10_000.0), now);
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
