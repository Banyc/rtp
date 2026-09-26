//! Injectable monotonic-clock seam for the opening handshake's deadlines.
//!
//! The opening arms each leg's deadline at the instant the leg starts and
//! re-anchors every retry from the current instant. Those reads go through
//! [`ClockRef`] instead of `std::time::Instant::now()` at the decision site,
//! so the boundary is reachable from a test: production carries
//! [`ClockRef::system`] and reads the real monotonic clock exactly as the
//! code did before the seam, while a driven runtime can install a clock that
//! follows its own (possibly paused) time and reach a multi-second deadline
//! without sleeping through it.
//!
//! `proxy` carries the same seam in `proxy/common/src/clock.rs`, but the
//! dependency runs the other way — `proxy` depends on this crate, so `rtp`
//! cannot reuse that type and mirrors the pattern instead.

use std::{sync::Arc, time::Instant};

/// A source of monotonic time.
///
/// The only production implementation is [`SystemClock`]. Test doubles return
/// a controlled instant so a boundary at exactly some deadline is drivable.
pub(crate) trait Clock: std::fmt::Debug + Send + Sync + 'static {
    /// The current instant. Monotonic and non-decreasing.
    fn now(&self) -> Instant;
}

/// The production [`Clock`]: the real monotonic clock.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// The [`Clock`] a connection carries: the system clock by default, or a
/// controlled clock installed by a test.
///
/// The default allocates nothing — the system clock is a zero-sized type the
/// reference resolves to directly — so a production connection behaves
/// exactly as it did before the seam existed.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClockRef(Option<Arc<dyn Clock>>);

impl ClockRef {
    /// The production clock: the real monotonic clock.
    pub(crate) fn system() -> Self {
        Self(None)
    }

    /// A controlled clock, for a test that must drive the boundary.
    #[cfg(test)]
    pub(crate) fn fixed<C: Clock>(clock: C) -> Self {
        Self(Some(Arc::new(clock)))
    }

    /// The current instant.
    pub(crate) fn now(&self) -> Instant {
        match self.0.as_deref() {
            Some(clock) => clock.now(),
            None => SystemClock.now(),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// A clock that follows tokio's (possibly paused) virtual time, so a task
    /// driven under `#[tokio::test(start_paused = true)]` observes the same
    /// advance that `tokio::time::sleep` produces. Lets a handshake deadline
    /// be placed at a virtual instant, so `sleep_until(deadline.into())`
    /// auto-advances the runtime instead of waiting on wall time.
    #[derive(Debug)]
    pub(crate) struct VirtualClock {
        base: Instant,
        epoch: tokio::time::Instant,
    }

    impl VirtualClock {
        pub(crate) fn new() -> Self {
            Self {
                base: Instant::now(),
                epoch: tokio::time::Instant::now(),
            }
        }
    }

    impl Clock for VirtualClock {
        fn now(&self) -> Instant {
            self.base + self.epoch.elapsed()
        }
    }
}
