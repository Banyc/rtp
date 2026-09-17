//! In-stream multi-symbol group parity flush policy.  When the interactive
//! tuning is on, a group accumulates data symbols and emits parities inline;
//! the path is live only while the condition gate has measured loss evidence,
//! so startup without congestion loss accumulates no full group that would
//! never be flushed.

/// The in-stream group FEC enable gate for one connection.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InStreamGroupFlush {
    enabled: bool,
}

impl InStreamGroupFlush {
    pub(crate) fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// In-stream group FEC is only live while the condition gate's loss
    /// evidence is active: startup without measured congestion loss or enough
    /// primary recovery samples emits no parity, so the path must not
    /// accumulate full groups while the gate is closed.
    pub(crate) fn live(&self, loss_active: bool) -> bool {
        self.enabled && loss_active
    }
}
