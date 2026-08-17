//! Typed congestion-response decisions and controller precedence.
//!
//! Controller precedence is probe, transient queue hold, persistent delay
//! drain, then loss backoff.  Loss blocks every delay-control branch.

/// Which probing sub-controller produced a `Probe` decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeKind {
    Gentle,
    Bandwidth,
}

/// One typed congestion-response decision for a delivery-rate sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CongestionDecision {
    Hold,
    Probe { target: f64 },
    Drain { floor: f64, target: f64 },
    LossBackoff { raw: f64, floor: f64, target: f64 },
}

/// The decision plus the evidence that produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CongestionOutcome {
    decision: CongestionDecision,
    probe_kind: Option<ProbeKind>,
    gentle_exit: Option<super::super::GentleExitCause>,
}

impl CongestionOutcome {
    pub(crate) fn new(
        decision: CongestionDecision,
        probe_kind: Option<ProbeKind>,
        gentle_exit: Option<super::super::GentleExitCause>,
    ) -> Self {
        Self {
            decision,
            probe_kind,
            gentle_exit,
        }
    }

    pub(crate) fn decision(&self) -> CongestionDecision {
        self.decision
    }

    /// The probe sub-controller behind a `Probe` decision.  Only meaningful
    /// for `Probe` outcomes, which always carry a kind.
    pub(crate) fn probe_kind(&self) -> ProbeKind {
        self.probe_kind
            .expect("a probe decision always carries its probe kind")
    }

    pub(crate) fn gentle_exit(&self) -> Option<super::super::GentleExitCause> {
        self.gentle_exit
    }
}

/// The controller branch chosen for the current sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponsePath {
    Probe,
    Hold,
    Drain,
    LossBackoff,
}

pub(super) fn select_path(
    queue_building: bool,
    persistent_queue: bool,
    loss_blocks_delay_control: bool,
) -> ResponsePath {
    if !loss_blocks_delay_control && !queue_building {
        ResponsePath::Probe
    } else if !loss_blocks_delay_control && !persistent_queue {
        ResponsePath::Hold
    } else if !loss_blocks_delay_control {
        ResponsePath::Drain
    } else {
        ResponsePath::LossBackoff
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_path_encodes_the_controller_precedence() {
        // Probe when no loss block and no queue.
        assert_eq!(select_path(false, false, false), ResponsePath::Probe);
        // Hold for a transient (non-persistent) queue.
        assert_eq!(select_path(true, false, false), ResponsePath::Hold);
        // Drain for a persistent queue.
        assert_eq!(select_path(true, true, false), ResponsePath::Drain);
        // LossBackoff wins whenever loss blocks delay control.
        assert_eq!(select_path(false, false, true), ResponsePath::LossBackoff);
        assert_eq!(select_path(true, false, true), ResponsePath::LossBackoff);
        assert_eq!(select_path(true, true, true), ResponsePath::LossBackoff);
    }
}
