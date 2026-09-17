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
