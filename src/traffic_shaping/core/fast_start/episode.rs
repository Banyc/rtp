//! The dedicated-lane bounded fast-start episode.
//!
//! [`FastStart`] is the windowed ACK-clock.  This module owns the episode
//! policy around it, so the reliable layer only applies the outcome: the ramp
//! is armed only once a real RTT estimate exists, and the episode ends on a
//! congestion loss block or a built queue (never on a lone iid loss).

use std::time::{Duration, Instant};

use super::{FastStart, FastStartStep};

/// The dedicated-lane bounded fast-start episode.
///
/// Wraps the windowed ACK clock with the policy that the reliable layer would
/// otherwise inline.  The 5 ms control-RTT floor before the first RTT sample
/// is not a real window: arming the ramp against it would close several tiny
/// windows and corrupt the previous-delivery baseline, so the episode holds
/// until [`crate::traffic_shaping::recovery::pkt_send_space::PktSendSpace::min_rtt`]
/// exists.
#[derive(Debug)]
pub(crate) struct FastStartEpisode {
    window: FastStart,
}

impl FastStartEpisode {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            window: FastStart::new(now),
        }
    }

    pub(crate) fn reset(&mut self, now: Instant) {
        self.window.reset(now);
    }

    /// Record `fresh` freshly-acknowledged packets and decide the next step.
    /// A missing `min_rtt` holds the episode: the window is one control RTT,
    /// so it is only meaningful once a real RTT estimate exists.
    pub(crate) fn on_ack(
        &mut self,
        fresh: usize,
        min_rtt: Option<Duration>,
        now: Instant,
        control_rtt: Duration,
        current_rate: f64,
    ) -> FastStartStep {
        if min_rtt.is_none() {
            return FastStartStep::Hold;
        }
        self.window.on_ack(fresh, now, control_rtt, current_rate)
    }

    /// Whether one delivery-rate sample ends the episode.
    ///
    /// The dedicated ramp exits on a congestion loss or on a built queue.  The
    /// stock probe-target and app-limited exits would fire on the first
    /// sample, before the ramp has begun, so the windowed ACK-clock owns the
    /// ramp and its own delivery-plateau exit.  A lone non-congestion (iid)
    /// loss is not a capacity signal: keep the bounded ramp alive so it
    /// settles at the delivered plateau instead of aborting to the ordinary
    /// probe's overshoot/drain.  Settling the paced rate at the instantaneous
    /// delivery sample would halve the pace on every loss (the sample lags the
    /// pace by about one control RTT) and strand the ramp below capacity.
    pub(crate) fn exits_on_rate_sample(
        loss_blocks_delay_control: bool,
        queue_building: bool,
    ) -> bool {
        loss_blocks_delay_control || queue_building
    }
}
