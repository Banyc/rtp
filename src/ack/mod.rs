//! ACK calculation, encapsulated.
//!
//! This module is the single home for everything an ACK *is* and *means*:
//!
//! - [`AckHistory`] / [`AckInterval`]: the receiver-side model of what has
//!   been received — a cumulative front plus merged intervals of sequence
//!   numbers in wrapping space.
//! - [`EncodeAck`]: the wire selection — which page of the history goes out
//!   in the next ACK datagram.  Every ACK datagram carries the cumulative
//!   `next` even when `block_count` is zero.
//! - [`AckBlocks`]: the sender-side interpretation of the blocks received
//!   from the peer, relative to the sender's `(send_start, sent_span)`
//!   coordinate system — the cumulative ack front, the highest acked
//!   sequence, which unacked packets are covered, and how many newer
//!   packets are acked past each unacked packet.
//!
//! The rest of the crate never computes ACK semantics itself: the receive
//! path records receipts into an [`AckHistory`] and asks [`EncodeAck`]
//! which blocks to send; the send path applies what [`AckBlocks`] reports.
//! A refactor of the ACK scheme (its calculation, its wire shape — the
//! byte-level codec lives in `crate::codec`) stays inside this module.

mod interval;

pub use interval::{AckBlocks, AckHistory, AckInterval};

use crate::sequence::SequenceNumber;

/// The protocol bound on selective blocks per ACK datagram.  The wire
/// `selective_count` field is one byte but must be at most this value.
pub(crate) const MAX_ACK_BLOCKS: usize = 64;

/// The ACK blocks to emit on the wire: a page selected from the receiver's
/// history, plus the cumulative `next` that always accompanies it.#[derive(Debug, Clone)]
pub struct EncodeAck<'a> {
    /// The receiver-side history to select from.
    pub queue: &'a AckHistory,
    /// Resume offset for deep ack-history pages: each flush sends the first
    /// page (starting at index 0) plus one deep page starting here.  Wrapped
    /// back to `max_blocks` on reset and when the cursor reaches the end of
    /// the history.
    pub first_block_index: usize,
    /// Maximum number of blocks per page.
    pub max_blocks: usize,
}
impl EncodeAck<'_> {
    /// The cumulative next: the first sequence the peer has not received.
    pub fn next(&self) -> SequenceNumber {
        self.queue.next()
    }

    /// The number of selective blocks this page will emit.
    pub fn block_count(&self) -> usize {
        self.queue
            .len()
            .saturating_sub(self.first_block_index)
            .min(self.max_blocks)
    }

    /// The selected blocks, in increasing order.
    pub fn blocks(&self) -> impl Iterator<Item = AckInterval> + '_ {
        self.queue
            .select_blocks(self.first_block_index, self.max_blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::SequenceNumber;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    #[test]
    fn encode_ack_selects_the_head_page_and_exposes_next_and_block_count() {
        let mut history = AckHistory::new_at(seq(0));
        for s in 0..100 {
            history.insert(seq(s * 2));
        }
        // insert(0) folded the front, so `next` is 1 and 99 blocks remain.
        assert_eq!(history.next(), seq(1));
        let ack = EncodeAck {
            queue: &history,
            first_block_index: 0,
            max_blocks: 8,
        };
        assert_eq!(ack.next(), seq(1));
        assert_eq!(ack.block_count(), 8);
        let page: Vec<_> = ack.blocks().collect();
        assert_eq!(page.len(), 8);
        assert_eq!(page[0].start, seq(2));
        assert_eq!(page[7].start, seq(16));
        // A nearly-fully-covered history reports the whole remaining page.
        let ack = EncodeAck {
            queue: &history,
            first_block_index: 96,
            max_blocks: 8,
        };
        assert_eq!(ack.block_count(), 3);
        assert_eq!(ack.blocks().count(), 3);
    }

    #[test]
    fn cumulative_next_is_carried_even_when_block_count_is_zero() {
        let history = AckHistory::new_at(seq(7));
        let ack = EncodeAck {
            queue: &history,
            first_block_index: 0,
            max_blocks: 64,
        };
        assert_eq!(ack.block_count(), 0);
        assert!(ack.blocks().next().is_none());
        assert_eq!(ack.next(), seq(7));
    }
}
