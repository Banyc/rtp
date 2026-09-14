//! Retransmission-armor policy: when a send gets an extra duplicate wire
//! copy.  Recovery sends are covered by the session toggle; a fresh
//! interactive single-symbol tail is covered by the caller's flag.

use super::RetransmissionArmorConfig;

/// Whether a recovery packet gets an armor duplicate copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmorDecision {
    /// Send the duplicate copy.
    Duplicate,
    /// A fresh send that the caller did not mark as an interactive
    /// single-symbol tail never gets a duplicate.
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

    /// Decide whether a packet gets an armor duplicate.  A recovery send is
    /// eligible when the session toggle is on; a fresh send is eligible only
    /// when the caller marks it as an interactive single-symbol tail (the
    /// `fresh_interactive_tail` argument), independent of the recovery
    /// toggle, because the interactive lane opts in through its FEC tuning.
    /// Eligibility is tested before the lazy queue closure is invoked, so
    /// ineligible sends never lock queue state.
    pub(crate) fn decide(
        &self,
        is_recovery: bool,
        fresh_interactive_tail: bool,
        queue_building: impl FnOnce() -> bool,
    ) -> ArmorDecision {
        let eligible = if is_recovery {
            self.config.is_enabled()
        } else {
            fresh_interactive_tail
        };
        if !eligible {
            return if is_recovery {
                ArmorDecision::SkipDisabled
            } else {
                ArmorDecision::SkipNotRecovery
            };
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
        let observe = || {
            queue_observations.set(queue_observations.get() + 1);
            false
        };
        let armor = RetransmissionArmor::new(RetransmissionArmorConfig::enabled());

        assert_eq!(
            armor.decide(true, false, observe),
            ArmorDecision::Duplicate,
            "recovery + enabled + not queue-building must duplicate"
        );
        assert_eq!(queue_observations.get(), 1);

        assert_eq!(
            armor.decide(false, false, observe),
            ArmorDecision::SkipNotRecovery,
            "a fresh non-tail send must never duplicate"
        );
        assert_eq!(
            queue_observations.get(),
            1,
            "a fresh non-tail send must not observe the queue"
        );

        assert_eq!(
            armor.decide(false, true, observe),
            ArmorDecision::Duplicate,
            "a fresh interactive tail duplicates even with the recovery toggle off"
        );
        assert_eq!(queue_observations.get(), 2);

        assert_eq!(
            RetransmissionArmor::new(RetransmissionArmorConfig::disabled())
                .decide(true, false, observe),
            ArmorDecision::SkipDisabled,
            "a disabled session must never duplicate recovery"
        );
        assert_eq!(
            queue_observations.get(),
            2,
            "a disabled session must not observe the queue"
        );

        let building_observations = Cell::new(0);
        let observe_building = || {
            building_observations.set(building_observations.get() + 1);
            true
        };
        assert_eq!(
            armor.decide(true, false, observe_building),
            ArmorDecision::SkipQueueBuilding,
            "recovery + enabled + queue-building must suppress the duplicate"
        );
        assert_eq!(building_observations.get(), 1);
        assert_eq!(
            armor.decide(false, true, observe_building),
            ArmorDecision::SkipQueueBuilding,
            "a fresh interactive tail is suppressed while the queue is building"
        );
        assert_eq!(building_observations.get(), 2);
    }
}
