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
