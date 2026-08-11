//! ACK interval algebra in wrapping sequence space: the domain model for
//! what has been received.
//!
//! [`AckHistory`] is the receiver-side history of received sequence
//! numbers: a cumulative front (`next`, the first sequence the peer has not
//! received) plus a merged set of disjoint *selective* ranges that start
//! after it.  [`AckBlocks`] is the sender-side view over the blocks
//! received from the peer in an ACK datagram, interpreted relative to the
//! sender's `(send_start, sent_span)` coordinate system — never by raw
//! ordering, which is invalid across wrap.
//!
//! Everything an ACK *means* is computed here; the transmission and
//! reliable layers only record receipts, carry the bytes, and apply the
//! results.

use core::num::NonZeroU64;

use crate::recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS;
use crate::sequence::{HALF_SEQUENCE_SPACE, SequenceMap, SequenceNumber, ge, le, lt};

/// One contiguous interval of received sequence numbers:
/// `[start, start + size)`.
#[derive(Debug, Clone, Copy)]
pub struct AckInterval {
    pub start: SequenceNumber,
    pub size: NonZeroU64,
}
impl AckInterval {
    /// One past the last sequence covered by this interval, advancing with
    /// wrapping arithmetic (no saturation: an interval reaching the end of
    /// the sequence space continues at zero).
    pub fn end(&self) -> SequenceNumber {
        self.start.advance(self.size.get())
    }

    pub fn contains(&self, seq: SequenceNumber) -> bool {
        ge(seq, self.start) && lt(seq, self.end())
    }
}

/// Receiver-side ACK history: the cumulative front plus received sequence
/// numbers as a merged set of disjoint selective intervals, so the cost of
/// maintaining the history is bounded by the number of holes, not the
/// number of packets received.
#[derive(Debug, Clone)]
pub struct AckHistory {
    next: SequenceNumber,
    start_to_size: SequenceMap<NonZeroU64>,
}
impl AckHistory {
    /// A history seeded at `initial_seq`: the peer has received nothing, so
    /// the cumulative front is `initial_seq` and the map is empty.
    pub fn new_at(initial_seq: SequenceNumber) -> Self {
        Self {
            next: initial_seq,
            start_to_size: SequenceMap::new(initial_seq, MAX_NUM_RECVING_PKTS as u64),
        }
    }

    /// The zero-seeded history (connections opened without the handshake).
    pub fn new() -> Self {
        Self::new_at(SequenceNumber::ZERO)
    }

    /// The cumulative front: the first sequence the peer has not received.
    pub fn next(&self) -> SequenceNumber {
        self.next
    }

    fn interval_at(&self, start: SequenceNumber) -> AckInterval {
        AckInterval {
            start,
            size: *self.start_to_size.get(&start).unwrap(),
        }
    }

    /// Record a received sequence number, merging it into the neighbouring
    /// intervals when they touch.  Values outside the receive window are
    /// ignored; a receipt already covered by an existing interval is a
    /// duplicate and leaves the history unchanged.  Whenever the merged
    /// range covers the cumulative front (a range beginning at `next`, or a
    /// wrapping range that reaches past it) it is folded into `next` — the
    /// map only ever holds noncumulative selective ranges — and the map
    /// anchor moves with it.
    pub fn insert(&mut self, seq: SequenceNumber) {
        if !self.start_to_size.window().contains(seq) {
            return;
        }
        let prev = self
            .start_to_size
            .predecessor(seq)
            .map(|start| self.interval_at(start));
        let next = self
            .start_to_size
            .successor(seq)
            .map(|start| self.interval_at(start));
        if let Some(prev) = prev
            && prev.contains(seq)
        {
            // Already covered: duplicate receipt.
            return;
        }
        let mut start = seq;
        let mut end = seq.advance(1);
        let mut merged_prev = false;
        if let Some(prev) = prev
            && prev.end() == seq
        {
            // Touching predecessor: extend the merged range backwards.
            start = prev.start;
            merged_prev = true;
        }
        if let Some(next) = next
            && next.start == end
        {
            // Touching successor: extend the merged range forwards and
            // drop the successor (the merged range replaces it).
            end = next.end();
            self.start_to_size.remove(&next.start);
        }
        if le(start, self.next) && lt(self.next, end) {
            // The merged range covers the cumulative front (beginning at it,
            // or wrapping past it): everything up to `end` is now cumulative.
            // Any predecessor folded into the range is cumulative too and is
            // dropped with it.
            if merged_prev {
                self.start_to_size.remove(&start);
            }
            self.next = end;
            self.fold_cumulative_prefix();
            self.start_to_size.move_anchor(self.next);
        } else {
            let size = NonZeroU64::new(start.forward_distance_to(end)).unwrap();
            self.start_to_size.insert(start, size);
        }
    }

    /// Fold every selective range that begins exactly at the cumulative
    /// front into `next` (in-order receipts made contiguous by a hole
    /// filling).
    fn fold_cumulative_prefix(&mut self) {
        while let Some(first) = self.start_to_size.first_logical()
            && first == self.next
        {
            let size = *self.start_to_size.get(&first).unwrap();
            self.start_to_size.remove(&first);
            self.next = first.advance(size.get());
        }
    }

    /// The selective ranges, in increasing logical order (the cumulative
    /// front is not included — it is carried by [`Self::next`]).
    pub fn blocks(&self) -> impl Iterator<Item = AckInterval> + '_ {
        self.start_to_size
            .iter()
            .map(|(s, n)| AckInterval { start: s, size: *n })
    }

    pub fn len(&self) -> usize {
        self.start_to_size.len()
    }

    /// Select a page of at most `max_blocks` blocks starting at
    /// `first_block_index`, in increasing order.
    pub fn select_blocks(
        &self,
        first_block_index: usize,
        max_blocks: usize,
    ) -> impl Iterator<Item = AckInterval> + '_ {
        self.blocks().skip(first_block_index).take(max_blocks)
    }
}
impl Default for AckHistory {
    fn default() -> Self {
        Self::new()
    }
}

/// Sender-side view over the ACK blocks received from the peer.  All
/// interpretation of what an incoming ACK means is computed here, relative
/// to the sender's `(send_start, sent_span)` coordinate system: cumulative
/// next is current only when its forward offset is within the sent span,
/// stale when over half-space, otherwise future; every selective block is
/// clipped to the already-sent span.  A stale/future cumulative value or a
/// range beyond the sent span must never release unsent packets or report
/// progress.
#[derive(Debug, Clone)]
pub struct AckBlocks<'a> {
    next: SequenceNumber,
    blocks: &'a [AckInterval],
}
impl<'a> AckBlocks<'a> {
    /// `blocks` must be in increasing order.
    pub fn new(next: SequenceNumber, blocks: &'a [AckInterval]) -> Self {
        Self { next, blocks }
    }

    /// The peer's cumulative next (one past the last packet it received in
    /// order).
    pub fn next(&self) -> SequenceNumber {
        self.next
    }

    /// The valid cumulative front relative to `(send_start, sent_span)`:
    /// `Some(next)` when the forward offset of `next` is within the sent
    /// span (including exactly at `sent_end`, the fully-acked window), and
    /// `None` when it is stale or future.
    fn valid_cumulative_next(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> Option<SequenceNumber> {
        let offset = send_start.forward_distance_to(self.next);
        if offset <= sent_span {
            Some(self.next)
        } else {
            None
        }
    }

    /// Clip a selective block to the already-sent span
    /// `[send_start, send_start + sent_span)`; `None` when the block does
    /// not intersect the span.
    fn clipped_block(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        block: AckInterval,
    ) -> Option<AckInterval> {
        let mut rel_start = send_start.forward_distance_to(block.start);
        if rel_start > HALF_SEQUENCE_SPACE {
            // The block starts before the window head.
            rel_start = 0;
        }
        let mut rel_end = send_start.forward_distance_to(block.end());
        if rel_end > HALF_SEQUENCE_SPACE {
            // The block ends before the window head.
            rel_end = 0;
        }
        let rel_end = rel_end.min(sent_span);
        if rel_end <= rel_start || rel_start >= sent_span {
            return None;
        }
        Some(AckInterval {
            start: send_start.advance(rel_start),
            size: NonZeroU64::new(rel_end - rel_start).unwrap(),
        })
    }

    fn clipped_blocks(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> impl Iterator<Item = AckInterval> + '_ {
        self.blocks
            .iter()
            .filter_map(move |block| self.clipped_block(send_start, sent_span, *block))
    }

    /// The start of the highest *clipped* acked block: the newest in-span
    /// sequence the peer has reported, used to bound how far an
    /// out-of-order ACK can advance the sender's reorder knowledge.
    pub fn out_of_order_seq_end(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> Option<SequenceNumber> {
        self.clipped_blocks(send_start, sent_span)
            .last()
            .map(|block| block.start)
    }

    /// Push, in increasing order, every sequence in `unacked` that is acked
    /// by the valid cumulative prefix or by a clipped selective block.
    /// `unacked` must be in increasing logical order.
    pub fn acked_set(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        unacked: &[SequenceNumber],
        acked: &mut Vec<SequenceNumber>,
    ) {
        if unacked.is_empty() {
            return;
        }
        let mut unacked_i = 0;
        let mut block_i = 0;
        if let Some(cumulative_end) = self.valid_cumulative_next(send_start, sent_span) {
            while unacked_i < unacked.len() && lt(unacked[unacked_i], cumulative_end) {
                acked.push(unacked[unacked_i]);
                unacked_i += 1;
            }
        }
        while block_i < self.blocks.len() && unacked_i < unacked.len() {
            let Some(block) = self.clipped_block(send_start, sent_span, self.blocks[block_i])
            else {
                block_i += 1;
                continue;
            };
            let unacked_seq = unacked[unacked_i];
            if lt(unacked_seq, block.start) {
                unacked_i += 1;
                continue;
            }
            if !block.contains(unacked_seq) {
                block_i += 1;
                continue;
            }
            acked.push(unacked_seq);
            unacked_i += 1;
        }
    }

    /// How many newer in-flight packets the *clipped* blocks ack past
    /// `unacked_seq` (bounded by the sent span).  Each acked packet with a
    /// higher sequence counts as one "pass"; the sum across blocks is the
    /// dup-ACK-pass evidence used by evidence-gated fast loss, mirroring the
    /// classic dup-ACK threshold of 3.
    pub fn sacked_above_count(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        unacked_seq: SequenceNumber,
    ) -> u32 {
        let mut passes: u32 = 0;
        for block in self.clipped_blocks(send_start, sent_span) {
            let block_end = block.end();
            let newer = if lt(unacked_seq, block.start) {
                block.size.get()
            } else if lt(unacked_seq, block_end) {
                unacked_seq.forward_distance_to(block_end) - 1
            } else {
                0
            };
            passes = passes.saturating_add(u32::try_from(newer).unwrap_or(u32::MAX));
        }
        passes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::SequenceNumber;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    fn iv(start: u64, size: u64) -> AckInterval {
        AckInterval {
            start: seq(start),
            size: NonZeroU64::new(size).unwrap(),
        }
    }

    #[test]
    fn test_ack_interval() {
        let a = iv(1, 1);
        let b = iv(1, 2);
        assert!(a.contains(seq(1)));
        assert!(!a.contains(seq(2)));
        assert!(b.contains(seq(2)));
        assert_eq!(b.end(), seq(3));
    }

    #[test]
    fn interval_end_wraps() {
        let block = iv(u64::MAX, 2);
        assert_eq!(block.end(), seq(1));
        assert!(block.contains(seq(u64::MAX)));
        assert!(block.contains(seq(0)));
        assert!(!block.contains(seq(1)));
    }

    #[test]
    fn test_ack_history_merges() {
        let mut a = AckHistory::new();
        a.insert(seq(1));
        a.insert(seq(3));
        a.insert(seq(2));
        // 1, 2, 3 all merged into one selective block [1, 4).
        let blocks: Vec<_> = a.blocks().collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start, seq(1));
        assert_eq!(blocks[0].size.get(), 3);
        assert_eq!(a.next(), seq(0));
    }

    #[test]
    fn cumulative_history_advances_across_wrap_and_discards_the_prefix() {
        let mut a = AckHistory::new_at(seq(u64::MAX - 1));
        assert_eq!(a.next(), seq(u64::MAX - 1));
        // In-order receipts fold straight into the cumulative front.
        a.insert(seq(u64::MAX - 1));
        assert_eq!(a.next(), seq(u64::MAX));
        a.insert(seq(u64::MAX));
        assert_eq!(a.next(), seq(0));
        a.insert(seq(0));
        assert_eq!(a.next(), seq(1));
        assert_eq!(a.len(), 0, "no selective ranges survive an in-order run");
        // Out-of-order receipts stay selective.
        a.insert(seq(3));
        a.insert(seq(5));
        assert_eq!(a.next(), seq(1));
        assert_eq!(a.len(), 2);
        // Filling the holes at 1 and 2 folds the prefix through the run of
        // 3 (received earlier) up to the next gap at 4.
        a.insert(seq(1));
        a.insert(seq(2));
        assert_eq!(
            a.next(),
            seq(4),
            "the prefix folds through the contiguous run"
        );
        let blocks: Vec<_> = a.blocks().collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start, seq(5));
    }

    #[test]
    fn selective_blocks_merge_across_zero() {
        // Two selective ranges straddling the physical zero boundary merge
        // into one wrapping range when a hole between them fills.
        let mut a = AckHistory::new_at(seq(u64::MAX - 2));
        assert_eq!(a.next(), seq(u64::MAX - 2));
        // A wrapping selective range [u64::MAX-1, 0) (covers u64::MAX-1 and
        // u64::MAX) and a low-segment range [0, 1) merge across zero into
        // [u64::MAX-1, 1) once the hole at u64::MAX fills.
        a.insert(seq(u64::MAX - 1));
        a.insert(seq(u64::MAX)); // merges into the wrapping [MAX-1, 0)
        a.insert(seq(0)); // merges across zero into [MAX-1, 1)
        assert_eq!(a.next(), seq(u64::MAX - 2), "the front is still a hole");
        let blocks: Vec<_> = a.blocks().collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start, seq(u64::MAX - 1));
        assert_eq!(
            blocks[0].size.get(),
            3,
            "both sides of the wrap in one range"
        );
        // The wrapped tail of the merged range is covered: a duplicate.
        a.insert(seq(u64::MAX));
        assert_eq!(
            a.len(),
            1,
            "a receipt inside the merged range is a duplicate"
        );
        // Filling the front hole folds the whole wrapped range into `next`.
        a.insert(seq(u64::MAX - 2));
        assert_eq!(a.next(), seq(1), "the front folds across the wrap");
        assert_eq!(a.len(), 0);
    }

    #[test]
    fn insert_ignores_values_outside_the_window() {
        let mut a = AckHistory::new_at(seq(100));
        a.insert(seq(99)); // stale
        a.insert(seq(100 + MAX_NUM_RECVING_PKTS as u64)); // too far ahead
        assert_eq!(a.len(), 0);
        a.insert(seq(100));
        assert_eq!(a.next(), seq(101));
    }

    #[test]
    fn duplicate_receipts_leave_the_history_unchanged() {
        let mut a = AckHistory::new_at(seq(10));
        a.insert(seq(12));
        a.insert(seq(13));
        let before: Vec<_> = a.blocks().collect();
        a.insert(seq(12)); // duplicate inside a range
        a.insert(seq(13)); // duplicate at the range tail (already covered)
        let after: Vec<_> = a.blocks().collect();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].start, after[0].start);
        assert_eq!(before[0].size, after[0].size);
    }

    #[test]
    fn select_blocks_pages_from_the_resume_cursor() {
        // insert(0) folds straight into the cumulative front (next = 1), so
        // the map holds one block per even seq from 2 on.
        let mut history = AckHistory::new_at(seq(0));
        for s in 0..10 {
            history.insert(seq(s * 2));
        }
        assert_eq!(history.next(), seq(1));
        assert_eq!(history.len(), 9);
        let page: Vec<_> = history.select_blocks(4, 3).collect();
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].start, seq(10));
        assert_eq!(page[2].start, seq(14));
    }

    #[test]
    fn sender_interprets_cumulative_and_selective_ack_across_wrap() {
        // Send window: [u64::MAX-2, u64::MAX-1, u64::MAX] => sent_span 3.
        let send_start = seq(u64::MAX - 2);
        let sent_span = 3u64;
        let in_flight = [seq(u64::MAX - 2), seq(u64::MAX - 1), seq(u64::MAX)];

        // Fully-acked window: cumulative next == sent_end (seq 0 after wrap).
        let recved = AckBlocks::new(seq(0), &[]);
        let mut acked = Vec::new();
        recved.acked_set(send_start, sent_span, &in_flight, &mut acked);
        assert_eq!(
            acked.len(),
            3,
            "a fully-acked window must release everything"
        );

        // Cumulative next exactly one past the first packet.
        let recved = AckBlocks::new(seq(u64::MAX - 1), &[]);
        let mut acked = Vec::new();
        recved.acked_set(send_start, sent_span, &in_flight, &mut acked);
        assert_eq!(acked, vec![seq(u64::MAX - 2)]);

        // Stale cumulative next (before the window): nothing released by the
        // prefix; a clipped selective block still releases in-window packets.
        let blocks = [iv(u64::MAX - 1, 2)];
        let stale = AckBlocks::new(seq(u64::MAX - 5), &blocks);
        let mut acked = Vec::new();
        stale.acked_set(send_start, sent_span, &in_flight, &mut acked);
        assert_eq!(acked, vec![seq(u64::MAX - 1), seq(u64::MAX)]);

        // Future cumulative next (beyond what was sent): no prefix release.
        let future = AckBlocks::new(seq(5), &[]);
        let mut acked = Vec::new();
        future.acked_set(send_start, sent_span, &in_flight, &mut acked);
        assert!(acked.is_empty());

        // A block beyond the sent span is clipped away entirely.
        let blocks = [iv(5, 10)];
        let beyond = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        let mut acked = Vec::new();
        beyond.acked_set(send_start, sent_span, &in_flight, &mut acked);
        assert!(
            acked.is_empty(),
            "a range beyond sent_span must release nothing"
        );

        // sacked_above counts only clipped ranges: [MAX, 2) clips to {MAX}
        // at the sent end, so it is one pass above MAX-2 and MAX-1.
        let blocks = [iv(u64::MAX, 2)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        assert_eq!(
            recved.sacked_above_count(send_start, sent_span, seq(u64::MAX - 2)),
            1
        );
        assert_eq!(
            recved.sacked_above_count(send_start, sent_span, seq(u64::MAX - 1)),
            1
        );
        assert_eq!(
            recved.sacked_above_count(send_start, sent_span, seq(u64::MAX)),
            0
        );
        // The second block of [u64::MAX-1, 2] is clipped at sent_end (seq 0).
        let blocks = [iv(u64::MAX - 1, 2)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        assert_eq!(
            recved.out_of_order_seq_end(send_start, sent_span),
            Some(seq(u64::MAX - 1))
        );
        let blocks = [iv(0, 10)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        assert_eq!(
            recved.out_of_order_seq_end(send_start, sent_span),
            None,
            "a fully-clipped range must not advance the reorder bound"
        );
    }

    #[test]
    fn stale_cumulative_and_future_cumulative_never_report_progress() {
        let send_start = seq(100);
        let sent_span = 5u64;
        let in_flight: Vec<_> = (100..105).map(seq).collect();
        for (next, want) in [
            (seq(99), vec![]),
            (seq(106), vec![]),
            (seq(103), vec![seq(100), seq(101), seq(102)]),
        ] {
            let recved = AckBlocks::new(next, &[]);
            let mut acked = Vec::new();
            recved.acked_set(send_start, sent_span, &in_flight, &mut acked);
            assert_eq!(acked, want, "cumulative next {next}");
        }
    }
}
