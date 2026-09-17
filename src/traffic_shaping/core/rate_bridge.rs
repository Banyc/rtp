//! The single validated bridge from a computed send rate into a positive rate.

use primitive::ops::float::PosR;

/// Settle a computed `f64` send rate into the validated positive type.
///
/// A non-positive or non-finite computation (a zero-delivery ACK window, a
/// division by a degenerate interval) is not a rate to settle at, so it
/// degrades to the live `fallback` instead of panicking the transport worker.
/// Every computed-rate site hands its raw `f64` here, so no call site can
/// carry an unchecked `PosR::new(...).unwrap()`.
pub(crate) fn settle_computed_rate(computed: f64, fallback: PosR<f64>) -> PosR<f64> {
    PosR::new(computed).unwrap_or(fallback)
}
