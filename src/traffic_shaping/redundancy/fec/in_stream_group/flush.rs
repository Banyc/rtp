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

#[cfg(test)]
mod tests {
    use super::InStreamGroupFlush;

    /// In-stream group FEC is live only when the toggle is on AND the
    /// condition gate has measured loss evidence.  Either missing condition
    /// keeps it dark, so a stock connection (toggle off) can never accumulate
    /// a full group and startup without loss never emits parity.
    #[test]
    fn live_requires_both_the_toggle_and_loss_evidence() {
        let on = InStreamGroupFlush::new(true);
        let off = InStreamGroupFlush::new(false);
        assert!(on.live(true), "toggle on + loss active must be live");
        assert!(
            !on.live(false),
            "toggle on without loss evidence must not be live"
        );
        assert!(
            !off.live(true),
            "the stock toggle must never be live even under loss"
        );
        assert!(!off.live(false));
    }
}
