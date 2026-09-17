//! The queue-growth controller's continuity timer bundle.
//!
//! [`IdleGap`](super::IdleGap) owns the single verdict for "an observation gap
//! voids continuity", but a controller chooses which of its timers that verdict
//! resets.  This module owns the queue-growth controller's choice: the
//! persistent-queue timer and the gentle drain episode are registered as one
//! bundle, so the set of timers that must survive an idle gap is this type's
//! membership and a timer cannot be observed without being added here.

use std::time::{Duration, Instant};

use super::{IdleContinuity, IdleGap};
use crate::traffic_shaping::core::gentle::GentleMode;

/// The queue-growth controller's continuity timers, registered with the shared
/// idle-gap clock.
///
/// The controller assembles one of these from the timers it keeps and hands it
/// to [`IdleGap::observe_queue_growth`], so both timers are voided together.
/// Adding a timer means adding it here, and its reset cannot be forgotten.
pub(crate) struct QueueGrowthContinuity<'a> {
    pub(crate) persistent_since: &'a mut Option<Instant>,
    pub(crate) gentle: &'a mut GentleMode,
}

impl IdleContinuity for QueueGrowthContinuity<'_> {
    fn break_idle_continuity(&mut self) {
        self.persistent_since.break_idle_continuity();
        self.gentle.break_idle_continuity();
    }
}

impl IdleGap {
    /// Record an observation at `now` and, when it followed an idle gap, void
    /// the continuity of the queue-growth timer bundle.
    pub(crate) fn observe_queue_growth(
        &mut self,
        mut timers: QueueGrowthContinuity<'_>,
        now: Instant,
        control_rtt: Duration,
    ) -> bool {
        self.observe(&mut timers, now, control_rtt)
    }
}
