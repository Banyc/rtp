//! Residual parity burst.  A FEC parity burst interrupted by `WouldBlock`
//! retains its unsent tail here, held front-to-back in emission order, and is
//! replayed FIFO before any new parity is generated.  Entries are moved, never
//! copied, so a retry can neither reorder nor duplicate a parity symbol, and
//! at most one group's parity is ever held, bounding the queue.

use std::collections::VecDeque;

/// The unsent tail of one parity burst, stored front-to-back in emission
/// order.
#[derive(Debug)]
pub(crate) struct PendingParityBurst {
    queued: VecDeque<Vec<u8>>,
}

impl PendingParityBurst {
    pub(crate) fn new() -> Self {
        Self {
            queued: VecDeque::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queued.len()
    }

    /// Hold a freshly generated parity burst after any residual already
    /// waiting.  The caller only generates new parity once this is empty, so
    /// the queue holds at most one group's parity.
    pub(crate) fn hold(&mut self, packets: impl IntoIterator<Item = Vec<u8>>) {
        self.queued.extend(packets);
    }

    /// The next datagram to replay, in emission order, or `None` when drained.
    pub(crate) fn next(&mut self) -> Option<Vec<u8>> {
        self.queued.pop_front()
    }

    /// Requeue a datagram the underlay refused, keeping FIFO order.
    pub(crate) fn requeue(&mut self, packet: Vec<u8>) {
        self.queued.push_front(packet);
    }

    /// Abandon the burst (the session is terminating fatally).
    pub(crate) fn abandon(&mut self) {
        self.queued.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::PendingParityBurst;

    fn pkt(byte: u8) -> Vec<u8> {
        vec![byte]
    }

    /// The residual parity burst replays strictly FIFO without reordering or
    /// duplication: a symbol popped and requeued by the underlay returns to the
    /// FRONT (so the next replay is the same symbol, not a later one), and the
    /// tail drains exactly once in emission order.  A retry must never emit a
    /// parity symbol twice or out of order, or the decoder's group accounting
    /// is corrupted.
    #[test]
    fn residual_burst_replays_fifo_without_reorder_or_duplication() {
        let mut burst = PendingParityBurst::new();
        assert!(burst.is_empty());
        assert_eq!(burst.next(), None);

        burst.hold(vec![pkt(1), pkt(2), pkt(3)]);
        assert_eq!(burst.len(), 3);
        assert!(!burst.is_empty());

        // The underlay refuses the first replayed symbol: requeue it in front
        // so the retry replays that same symbol, never skipping ahead.
        let refused = burst.next().expect("the first symbol must replay");
        assert_eq!(refused, pkt(1));
        burst.requeue(refused);
        assert_eq!(burst.len(), 3, "requeue must not duplicate a symbol");

        let mut replayed = vec![];
        while let Some(packet) = burst.next() {
            replayed.push(packet);
        }
        assert_eq!(
            replayed,
            vec![pkt(1), pkt(2), pkt(3)],
            "the burst must drain once, in emission order"
        );
        assert!(burst.is_empty());
        assert_eq!(burst.next(), None);
    }

    /// Abandoning the burst drops every held symbol, so a fatal session exit
    /// leaves nothing to replay into the next session.
    #[test]
    fn abandon_clears_every_held_symbol() {
        let mut burst = PendingParityBurst::new();
        burst.hold(vec![pkt(1), pkt(2)]);
        assert_eq!(burst.len(), 2);
        burst.abandon();
        assert!(burst.is_empty());
        assert_eq!(burst.len(), 0);
        assert_eq!(burst.next(), None);
    }
}
