//! Interactive FEC capacity gate.  A force-flush tuning uses a "genuinely
//! spare capacity" predicate that does not require the staging buffer to be
//! empty (so a mid-burst full-group flush can fire) and ignores a pending
//! retransmit/tail probe, but still requires send-window room, zero
//! application write waiters, and no queue growth.  Stock/bulk keeps the
//! strict predicate.

/// Which spare-capacity predicate a flush decision consults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapacityGate {
    /// The strict predicate: also requires an empty staging buffer and no
    /// pending retransmit/tail probe.
    Strict,
    /// The interactive predicate: a batched interactive lane is always
    /// repairing, so the staging buffer need not be empty.
    Interactive,
}

impl CapacityGate {
    /// The predicate selected by the connection's in-stream-flush tuning.
    pub(crate) fn for_instream_flush(instream_flush: bool) -> Self {
        if instream_flush {
            Self::Interactive
        } else {
            Self::Strict
        }
    }

    /// Evaluate the selected predicate.  Both closures are accepted but at
    /// most one is invoked, so the unselected side never reads session state.
    pub(crate) fn spare<Strict, Interactive>(self, strict: Strict, interactive: Interactive) -> bool
    where
        Strict: FnOnce() -> bool,
        Interactive: FnOnce() -> bool,
    {
        match self {
            Self::Strict => strict(),
            Self::Interactive => interactive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CapacityGate;
    use std::cell::Cell;

    /// The tuning selects the predicate: a force-flush tuning gets the
    /// interactive predicate, the stock default keeps the strict one.
    #[test]
    fn instream_flush_selects_the_interactive_predicate() {
        assert_eq!(
            CapacityGate::for_instream_flush(true),
            CapacityGate::Interactive
        );
        assert_eq!(
            CapacityGate::for_instream_flush(false),
            CapacityGate::Strict
        );
    }

    /// Evaluating the selected predicate invokes only that side: the
    /// unselected closure never reads session state, so a lane cannot pay for
    /// (or be perturbed by) the predicate it did not select.
    #[test]
    fn spare_invokes_only_the_selected_predicate() {
        let strict_calls = Cell::new(0);
        let interactive_calls = Cell::new(0);
        let strict = || {
            strict_calls.set(strict_calls.get() + 1);
            true
        };
        let interactive = || {
            interactive_calls.set(interactive_calls.get() + 1);
            false
        };

        assert!(CapacityGate::Strict.spare(strict, interactive));
        assert_eq!(strict_calls.get(), 1, "the strict side must be consulted");
        assert_eq!(
            interactive_calls.get(),
            0,
            "the interactive side must not be consulted"
        );

        assert!(!CapacityGate::Interactive.spare(strict, interactive));
        assert_eq!(strict_calls.get(), 1, "the strict side must not be re-read");
        assert_eq!(
            interactive_calls.get(),
            1,
            "the interactive side must be consulted once"
        );
    }
}
