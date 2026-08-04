//! Deterministic test PRNGs shared by in-crate unit tests and integration
//! tests.  [`SplitMix64`] is a tiny, seeded, deterministic generator used by
//! the hostile-input fuzzers so a failing seed reproduces exactly.
//!
//! This is the single shared definition: in-crate `#[cfg(test)]` modules use
//! it via `crate::testing::SplitMix64`, and integration tests under `tests/`
//! use it via `rtp::testing::SplitMix64`.

/// Tiny seeded SplitMix64 PRNG for deterministic hostile-input generators.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    /// New generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next `u64` in the SplitMix64 sequence.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Next uniformly-random byte.
    pub fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    /// Uniform random index in `[0, n)`.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Hostile-input fuzz helpers: the packet decoder used by the hostile-input
/// integration fuzzers.  `codec` is `pub(crate)` since the curated root API
/// only exposes Listener/connector/config types, so these are re-exported for
/// the fuzz tests.
pub use crate::codec::{DecodedDataPkt, decode};
