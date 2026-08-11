//! ACK calculation, encapsulated.
//!
//! This module is the single home for everything an ACK *is* and *means*:
//!
//! - [`AckHistory`] / [`AckInterval`]: the receiver-side model of what has
//!   been received, as merged intervals of sequence numbers.
//! - [`EncodeAck`]: the wire selection — which page of the history goes out
//!   in the next ACK datagram, and how the deep-page resume cursor advances
//!   ([`next_page_cursor`]).
//! - [`AckBlocks`]: the sender-side interpretation of the blocks received
//!   from the peer — the cumulative ack front, the highest acked sequence,
//!   which unacked packets are covered, and how many newer packets are acked
//!   past each unacked packet.
//!
//! The rest of the crate never computes ACK semantics itself: the receive
//! path records receipts into an [`AckHistory`] and asks [`EncodeAck`]
//! which blocks to send; the send path applies what [`AckBlocks`] reports.
//! A refactor of the ACK scheme (its calculation, its wire shape — the
//! byte-level codec lives in `crate::codec`) stays inside this module.

mod interval;

pub use interval::{AckBlocks, AckHistory, AckInterval};

/// The ACK blocks to emit on the wire: a page selected from the receiver's
/// history.
#[derive(Debug, Clone)]
pub struct EncodeAck<'a> {
    /// The receiver-side history to select from.
    pub queue: &'a AckHistory,
    /// Resume offset for deep ack-history pages: each flush sends cumulative
    /// page 0 plus one deep page starting here. Wrapped back to
    /// `max_blocks` on reset and when the cursor reaches the end of the
    /// history.
    pub first_block_index: usize,
    /// Maximum number of blocks per page.
    pub max_blocks: usize,
}
impl EncodeAck<'_> {
    /// The selected blocks, in increasing order.
    pub fn blocks(&self) -> impl Iterator<Item = AckInterval> + '_ {
        self.queue
            .select_blocks(self.first_block_index, self.max_blocks)
    }
}

/// Advance the deep-page resume cursor after a flush that sent one page of
/// `max_blocks` blocks starting at `cursor`. Wraps back to the start of the
/// deep pages (`max_blocks`, since page 0 is always the cumulative head)
/// when the page reached the end of the history.
pub fn next_page_cursor(cursor: usize, history_count: usize, max_blocks: usize) -> usize {
    if cursor + max_blocks < history_count {
        cursor + max_blocks
    } else {
        max_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_page_cursor_advances_and_wraps() {
        // Page 0 is the cumulative head; the deep page advances by
        // max_blocks while more history remains.
        assert_eq!(next_page_cursor(64, 200, 64), 128);
        assert_eq!(next_page_cursor(64, 128, 64), 64);
        assert_eq!(next_page_cursor(64, 127, 64), 64);
        // A cursor already past the history wraps back to the head.
        assert_eq!(next_page_cursor(64, 40, 64), 64);
    }

    #[test]
    fn encode_ack_selects_the_cumulative_head_and_one_deep_page() {
        let mut history = AckHistory::new();
        for seq in 0..100 {
            history.insert(seq * 2);
        }
        let ack = EncodeAck {
            queue: &history,
            first_block_index: 64,
            max_blocks: 8,
        };
        let page: Vec<_> = ack.blocks().collect();
        assert_eq!(page.len(), 8);
        assert_eq!(page[0].start, 128);
        assert_eq!(page[7].start, 142);
    }
}
