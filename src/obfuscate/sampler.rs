//! The fitted-ack data-size sampler: a constant-memory window over the
//! sizes of recently sent data packets, with a cheap lazy refit that
//! produces a triangular fit (median mode, percentile spread, observed
//! max) the write half uses to zero-pad standalone ACK datagrams so a
//! passive DPI observer cannot tell them from data by wire size.
//!
//! The padding itself lives in the codec: an ACK page zero-filled to the
//! drawn target rides through the FEC envelope untouched and the codec's
//! dispatch strips the all-zero tail after the ACK command. This module
//! only owns the size statistics — O(1) per observed packet, O(WINDOW) per
//! refit (at most once per [`FIT_INTERVAL`]), constant memory, no
//! allocation on the send path.

use std::time::{Duration, Instant};

/// The ring-buffer window: the last `WINDOW` observed data-packet sizes.
const WINDOW: usize = 64;

/// Below this many samples no fit exists (the distribution is not yet
/// informative) and ACKs are sent unpadded.
pub(crate) const MIN_SAMPLES: usize = 16;

/// Refit at most once per second: the fit is reused for every draw until
/// it goes stale, so the amortized refit cost is negligible.
const FIT_INTERVAL: Duration = Duration::from_secs(1);

/// A cached fit: the triangular parameters plus the observed max and the
/// instant the fit expires.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CachedFit {
    /// The fitted mode (median of the sampled sizes).
    mode: u16,
    /// The one-sided spread: `max(|mode - p10|, |p90 - mode|)`, clamped to
    /// at least 1 and into u16 range around `mode`.
    spread: u16,
    /// The largest sampled data-packet size: the ceiling for a padded ACK
    /// target so padded ACKs never exceed the observed data envelope.
    observed_max: u16,
    /// The instant this fit expires; the next `refit_if_stale` at or after
    /// it recomputes the fit from the window.
    refit_at: Instant,
}

/// A constant-memory sampler over the sizes of recently sent data packets.
/// `observe` is O(1); the fit is computed lazily, at most once per
/// [`FIT_INTERVAL`].
#[derive(Debug)]
pub(crate) struct DataSizeSampler {
    /// Ring buffer of the last `WINDOW` observed sizes (`MSS ≪ 65536`, so
    /// `u16` is safe; asserted on observe).
    ring: [u16; WINDOW],
    /// Ring write position (the next slot to overwrite).
    pos: usize,
    /// Total samples observed (saturating).
    count: usize,
    /// The cached fit, valid until its `refit_at`.
    fit: Option<CachedFit>,
}

impl DataSizeSampler {
    pub(crate) fn new() -> Self {
        Self {
            ring: [0; WINDOW],
            pos: 0,
            count: 0,
            fit: None,
        }
    }

    /// Record one sent data-packet size, O(1).
    pub(crate) fn observe(&mut self, size: usize) {
        debug_assert!(
            size < u16::MAX as usize,
            "a data-packet size larger than u16::MAX cannot be sampled"
        );
        self.ring[self.pos] = size as u16;
        self.pos = (self.pos + 1) % WINDOW;
        self.count = self.count.saturating_add(1);
    }

    /// Refit when the cached fit is stale (or absent) and enough samples
    /// have been observed. Cheap: copying and sorting at most `WINDOW`
    /// `u16`s, at most once per [`FIT_INTERVAL`].
    fn refit_if_stale(&mut self, now: Instant) {
        if !self.fit.is_none_or(|fit| now >= fit.refit_at) {
            return;
        }
        if self.count < MIN_SAMPLES {
            self.fit = None;
            return;
        }
        let len = self.count.min(WINDOW);
        // Sort does not need chronological order: copy the ring (the last
        // `len` samples) into a stack array and sort it.
        let mut sorted = [0u16; WINDOW];
        if self.count < WINDOW {
            sorted[..len].copy_from_slice(&self.ring[..len]);
        } else {
            sorted.copy_from_slice(&self.ring);
        }
        let sorted = &mut sorted[..len];
        sorted.sort_unstable();
        let mode = sorted[len / 2];
        let p10 = sorted[len / 10];
        let p90 = sorted[(9 * len) / 10];
        debug_assert!(len / 10 < len && (9 * len) / 10 < len);
        // One-sided spread floor 1, then clamped so the triangular band
        // `[mode - spread, mode + spread]` stays inside u16.
        let spread = (mode as i32 - p10 as i32)
            .abs()
            .max((p90 as i32 - mode as i32).abs())
            .max(1)
            .min(mode as i32)
            .min(i32::from(u16::MAX) - mode as i32);
        let observed_max = sorted[len - 1];
        self.fit = Some(CachedFit {
            mode,
            spread: spread as u16,
            observed_max,
            refit_at: now + FIT_INTERVAL,
        });
    }

    /// The current fit: `(mode, spread, observed_max)`, refitting first
    /// when stale. `None` when fewer than [`MIN_SAMPLES`] samples exist.
    fn fit_mode_and_spread(&mut self, now: Instant) -> Option<(u16, u16, u16)> {
        self.refit_if_stale(now);
        let fit = self.fit?;
        Some((fit.mode, fit.spread, fit.observed_max))
    }

    /// Draw a padded-ACK target for an ACK page of `page_len` bytes: a
    /// triangular draw (difference of two uniforms) peaked at the fitted
    /// median. The target is the page's TOTAL padded length (content plus
    /// the zero fill), clamped into `[page_len, observed_max]` so a padded
    /// ACK can never be smaller than its own content nor larger than the
    /// largest observed data packet (so it stays inside the receiver
    /// buffers, which already fit data packets, and inside the page
    /// buffers, which are sized for data). Returns `None` when no fit
    /// exists yet or the data envelope is too small to hold this ACK (the
    /// caller then sends it unpadded).
    pub(crate) fn draw_target(&mut self, page_len: usize, now: Instant) -> Option<usize> {
        let (mode, spread, observed_max) = self.fit_mode_and_spread(now)?;
        if observed_max < page_len as u16 {
            return None;
        }
        let u = rand::random_range(0..=spread) as i32;
        let v = rand::random_range(0..=spread) as i32;
        let draw = (mode as i32 + u - v) as usize;
        Some(
            draw.clamp(page_len, observed_max as usize)
                .min(observed_max as usize),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_wraps_and_keeps_the_last_window() {
        let mut sampler = DataSizeSampler::new();
        for i in 0..(WINDOW * 3) {
            sampler.observe(1000 + i);
            assert_eq!(
                sampler.pos,
                sampler.count % WINDOW,
                "the write position must track count modulo the window"
            );
        }
        assert_eq!(sampler.count, WINDOW * 3);
        // The window now holds the last WINDOW sizes: 1000+(2*WINDOW) ..
        // 1000+(3*WINDOW)-1. The ring must hold exactly those (sorted).
        let mut got: Vec<u16> = sampler.ring.to_vec();
        got.sort_unstable();
        let expect: Vec<u16> = (1000 + 2 * WINDOW..1000 + 3 * WINDOW)
            .map(|v| v as u16)
            .collect();
        assert_eq!(got, expect, "the ring must hold the last window of sizes");
    }

    #[test]
    fn fit_from_a_known_sample_set_is_exact() {
        let mut sampler = DataSizeSampler::new();
        // 32 identical large samples: the median, spread, and max are all
        // pinned to the sample size.
        for _ in 0..32 {
            sampler.observe(1400);
        }
        let (mode, spread, observed_max) = sampler.fit_mode_and_spread(Instant::now()).unwrap();
        assert_eq!(mode, 1400);
        assert_eq!(spread, 1, "the spread must floor at 1");
        assert_eq!(observed_max, 1400);
    }

    #[test]
    fn fit_tracks_median_and_percentile_spread() {
        let mut sampler = DataSizeSampler::new();
        // 16 samples: ten 1300s, one 1400, five 1500s. Sorted: ten 1300,
        // one 1400, five 1500. len = 16, mode = sorted[8] = 1300,
        // p10 = sorted[1] = 1300, p90 = sorted[14] = 1500, spread = 200.
        for _ in 0..10 {
            sampler.observe(1300);
        }
        sampler.observe(1400);
        for _ in 0..5 {
            sampler.observe(1500);
        }
        let (mode, spread, observed_max) = sampler.fit_mode_and_spread(Instant::now()).unwrap();
        assert_eq!(mode, 1300);
        assert_eq!(spread, 200);
        assert_eq!(observed_max, 1500);
    }

    #[test]
    fn spread_is_clamped_into_u16_range() {
        let mut sampler = DataSizeSampler::new();
        // mode pinned at 60000 with two low outliers: the raw spread is
        // |60000 - 100| = 59900, which must be clamped so mode + spread
        // does not overflow u16 (and mode - spread does not underflow).
        for _ in 0..2 {
            sampler.observe(100);
        }
        for _ in 0..15 {
            sampler.observe(60000);
        }
        let (mode, spread, _) = sampler.fit_mode_and_spread(Instant::now()).unwrap();
        assert_eq!(mode, 60000);
        assert_eq!(spread, 5535, "the spread must clamp at u16::MAX - mode");
        assert!(
            (mode as u32 + spread as u32) <= u32::from(u16::MAX),
            "mode + spread must stay inside u16"
        );
        assert!(mode >= spread, "mode - spread must not underflow");
    }

    #[test]
    fn refit_is_throttled_within_a_second() {
        let now = Instant::now();
        let mut sampler = DataSizeSampler::new();
        for _ in 0..32 {
            sampler.observe(1400);
        }
        let (mode, spread, max) = sampler.fit_mode_and_spread(now).unwrap();
        // New samples observed, but the fit is still cached: another call
        // within the interval must return the SAME parameters.
        for _ in 0..16 {
            sampler.observe(300);
        }
        let (mode2, spread2, max2) = sampler.fit_mode_and_spread(now).unwrap();
        assert_eq!((mode, spread, max), (mode2, spread2, max2));
        // After the refit instant, the fresh samples reshape the fit. Feed
        // enough small samples to evict every 1400 from the 64-slot window
        // so the new median AND observed max both drop.
        for _ in 0..64 {
            sampler.observe(300);
        }
        let later = now + FIT_INTERVAL + Duration::from_millis(1);
        let (mode3, _, max3) = sampler.fit_mode_and_spread(later).unwrap();
        assert_eq!(max3, 300, "the fitted max must follow the newer window");
        assert_eq!(mode3, 300, "the fitted mode must follow the newer window");
    }

    #[test]
    fn draw_respects_the_page_len_to_observed_max_clamp() {
        let now = Instant::now();
        let mut sampler = DataSizeSampler::new();
        for _ in 0..64 {
            sampler.observe(1400);
        }
        // The target is the ACK page's TOTAL padded length: at least the
        // page itself, at most the largest observed data packet.
        for page_len in [10, 500, 1397] {
            let target = sampler.draw_target(page_len, now).unwrap();
            assert!(
                (page_len..=1400).contains(&target),
                "target {target} must stay in [page={page_len}, observed_max=1400]"
            );
        }
        // An ACK page too large for the data envelope has no target: `None`.
        assert_eq!(sampler.draw_target(1401, now), None);
    }

    #[test]
    fn no_fit_below_min_samples_and_under_sized_envelope() {
        let now = Instant::now();
        let mut empty = DataSizeSampler::new();
        assert!(empty.draw_target(5, now).is_none());
        for _ in 0..MIN_SAMPLES - 1 {
            empty.observe(1400);
        }
        assert!(
            empty.draw_target(5, now).is_none(),
            "below MIN_SAMPLES no fit exists"
        );
        // A full window of tiny samples whose envelope cannot hold a large
        // ACK page: `None`, so the caller sends unpadded.
        let mut tiny = DataSizeSampler::new();
        for _ in 0..MIN_SAMPLES {
            tiny.observe(60);
        }
        assert!(
            tiny.draw_target(100, now).is_none(),
            "a 100-byte ACK page cannot fit in a 60-byte data envelope"
        );
    }
}
