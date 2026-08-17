//! Retransmission-armor policy: when a recovery packet gets an extra
//! duplicate wire copy.

use super::RetransmissionArmorConfig;

/// Whether a recovery packet gets an armor duplicate copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmorDecision {
    /// Send the duplicate copy.
    Duplicate,
    /// Fresh (non-recovery) sends never get a duplicate.
    SkipNotRecovery,
    /// The sender is queue-building; the duplicate is suppressed.
    SkipQueueBuilding,
    /// The armor toggle is disabled for this session.
    SkipDisabled,
}

/// Immutable retransmission-armor policy for one session.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetransmissionArmor {
    config: RetransmissionArmorConfig,
}

impl RetransmissionArmor {
    pub(crate) fn new(config: RetransmissionArmorConfig) -> Self {
        Self { config }
    }

    /// Decide whether a packet gets an armor duplicate.  Recovery is tested
    /// first, then the enabled toggle, and only then is the lazy queue
    /// closure invoked, so fresh/disabled sends never lock queue state.
    pub(crate) fn decide(
        &self,
        is_recovery: bool,
        queue_building: impl FnOnce() -> bool,
    ) -> ArmorDecision {
        if !is_recovery {
            return ArmorDecision::SkipNotRecovery;
        }
        if !self.config.is_enabled() {
            return ArmorDecision::SkipDisabled;
        }
        if queue_building() {
            return ArmorDecision::SkipQueueBuilding;
        }
        ArmorDecision::Duplicate
    }
}

#[cfg(test)]
mod tests {
    use super::{ArmorDecision, RetransmissionArmor, RetransmissionArmorConfig};
    use std::cell::Cell;

    #[test]
    fn decision_reports_every_policy_branch_and_keeps_queue_observation_lazy() {
        let queue_observations = Cell::new(0);
        let mut observe = || {
            queue_observations.set(queue_observations.get() + 1);
            false
        };
        let armor = RetransmissionArmor::new(RetransmissionArmorConfig::enabled());

        assert_eq!(
            armor.decide(true, &mut observe),
            ArmorDecision::Duplicate,
            "recovery + enabled + not queue-building must duplicate"
        );
        assert_eq!(queue_observations.get(), 1);

        assert_eq!(
            armor.decide(false, &mut observe),
            ArmorDecision::SkipNotRecovery,
            "fresh sends must never duplicate"
        );
        assert_eq!(
            queue_observations.get(),
            1,
            "a fresh send must not observe the queue"
        );

        assert_eq!(
            RetransmissionArmor::new(RetransmissionArmorConfig::disabled())
                .decide(true, &mut observe),
            ArmorDecision::SkipDisabled,
            "a disabled session must never duplicate"
        );
        assert_eq!(
            queue_observations.get(),
            1,
            "a disabled session must not observe the queue"
        );

        let building_observations = Cell::new(0);
        let mut observe_building = || {
            building_observations.set(building_observations.get() + 1);
            true
        };
        assert_eq!(
            armor.decide(true, &mut observe_building),
            ArmorDecision::SkipQueueBuilding,
            "recovery + enabled + queue-building must suppress the duplicate"
        );
        assert_eq!(building_observations.get(), 1);
    }
}
