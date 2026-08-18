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

use crate::ack::MAX_ACK_BLOCKS;
use crate::recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS;
use crate::sequence::{HALF_SEQUENCE_SPACE, SequenceMap, SequenceNumber};

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
    pub fn end(self) -> SequenceNumber {
        self.start.advance(self.size.get())
    }

    fn merge_forward(&self, later: Self) -> Option<Self> {
        let later_offset = self.start.forward_distance_to(later.start);
        if later_offset > self.size.get() || later_offset >= HALF_SEQUENCE_SPACE {
            return None;
        }

        let merged_size = self
            .size
            .get()
            .max(later_offset.checked_add(later.size.get())?);

        Some(Self {
            start: self.start,
            size: NonZeroU64::new(merged_size).unwrap(),
        })
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

    /// Record a received sequence number, merging it into the neighbouring
    /// intervals when they touch.  Values outside the receive window are
    /// ignored; a receipt already covered by an existing interval is a
    /// duplicate and leaves the history unchanged.  Whenever the merged
    /// range covers the cumulative front (a range beginning at `next`, or a
    /// wrapping range that reaches past it) it is folded into `next` — the
    /// map only ever holds noncumulative selective ranges — and the map
    /// anchor moves with it.
    #[cfg(test)]
    pub(crate) fn insert(&mut self, seq: SequenceNumber) {
        if self.next.forward_distance_to(seq) >= MAX_NUM_RECVING_PKTS as u64 {
            return;
        }
        self.insert_in_window(seq);
    }

    pub(crate) fn insert_in_window(&mut self, seq: SequenceNumber) {
        if seq == self.next {
            let mut interval = AckInterval {
                start: seq,
                size: NonZeroU64::new(1).unwrap(),
            };
            if !self.start_to_size.is_empty()
                && let Some(following) = self
                    .start_to_size
                    .successor(seq)
                    .map(|(start, &size)| AckInterval { start, size })
                && let Some(merged) = interval.merge_forward(following)
            {
                self.start_to_size.remove(&following.start);
                interval = merged;
            }
            self.next = interval.end();
            self.start_to_size.advance_anchor(self.next);
            return;
        }
        let previous = self
            .start_to_size
            .floor(seq)
            .map(|(start, &size)| AckInterval { start, size });
        let following = self
            .start_to_size
            .successor(seq)
            .map(|(start, &size)| AckInterval { start, size });
        let mut interval = AckInterval {
            start: seq,
            size: NonZeroU64::new(1).unwrap(),
        };
        if let Some(previous) = previous {
            if previous.start.forward_distance_to(seq) < previous.size.get() {
                return;
            }
            if let Some(merged) = previous.merge_forward(interval) {
                self.start_to_size.remove(&previous.start);
                interval = merged;
            }
        }
        if let Some(following) = following
            && let Some(merged) = interval.merge_forward(following)
        {
            self.start_to_size.remove(&following.start);
            interval = merged;
        }
        debug_assert_ne!(interval.start, self.next);
        self.start_to_size.insert(interval.start, interval.size);
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

/// Classification of the peer's cumulative next relative to the sender's
/// `(send_start, sent_span)` coordinate system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CumulativePosition {
    /// The cumulative next is within the sent span; carries its forward
    /// offset from `send_start`.
    Current(u64),
    /// More than half a space behind the window head: already passed.
    Stale,
    /// Ahead of the sent span but within the forward half space.
    Future,
}

/// The outcome of a single [`AckBlocks::analyze`] pass: whether the peer's
/// cumulative next is current, the highest clipped SACK block start (the
/// reorder bound), and whether any selective evidence was present.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AckAnalysis {
    pub(crate) cumulative_is_current: bool,
    pub(crate) highest_sacked: Option<SequenceNumber>,
    pub(crate) has_sack_evidence: bool,
}
/// ACK facts computed while normalizing selective blocks. The sender uses
/// 'relevant_unacked_end' to bound its occupied-window walk, then consumes the
/// remaining facts and the same normalized block buffer during analysis.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AckPreparation {
    cumulative: Option<u64>,
    highest_sacked: Option<SequenceNumber>,
    raw_blocks_present: bool,
    pub(crate) relevant_unacked_end: u64,
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
    /// `blocks` must be in increasing order (the encoder emits them sorted;
    /// hostile input is normalized by [`Self::normalize_block_offsets`]).
    pub(crate) fn new(next: SequenceNumber, blocks: &'a [AckInterval]) -> Self {
        debug_assert!(blocks.len() <= MAX_ACK_BLOCKS);
        Self { next, blocks }
    }

    /// The peer's cumulative next (one past the last packet it received in
    /// order), classified relative to `(send_start, sent_span)`: current
    /// when its forward offset is within the sent span (including exactly at
    /// `sent_end`, the fully-acked window), stale when over half-space,
    /// otherwise future.
    fn cumulative_position(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> CumulativePosition {
        let offset = send_start.forward_distance_to(self.next);
        if offset <= sent_span {
            CumulativePosition::Current(offset)
        } else if offset > HALF_SEQUENCE_SPACE {
            CumulativePosition::Stale
        } else {
            CumulativePosition::Future
        }
    }

    /// The wire's selective blocks clipped to the already-sent span, as
    /// `(start, end)` forward offsets from `send_start`.  Hostile input —
    /// ranges outside the span, huge sizes that wrap — is bounded here; the
    /// caller may reorder and merge the result.
    fn valid_block_offsets(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.blocks.iter().filter_map(move |block| {
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
            Some((rel_start, rel_end))
        })
    }

    /// Normalize the hostile wire input once: clip to the sent span, sort by
    /// logical start offset, and merge overlapping ranges (but not merely
    /// adjacent ones). After this, SACK membership is a set and
    /// overlapping/duplicate ranges can no longer inflate loss evidence.
    /// Returns the start of the highest *clipped* acked block - the newest
    /// in-span sequence the peer reported, captured before merging so it
    /// bounds how far an out-of-order ACK advances the reorder knowledge.
    fn normalize_block_offsets(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        block_offsets: &mut Vec<(u64, u64)>,
    ) -> Option<SequenceNumber> {
        block_offsets.clear();
        if self.blocks.is_empty() {
            return None;
        }
        block_offsets.extend(self.valid_block_offsets(send_start, sent_span));
        if block_offsets.len() > 1 {
            if !block_offsets.is_sorted_by_key(|&(start, _)| start) {
                block_offsets.sort_unstable_by_key(|&(start, _)| start);
            }
            let highest_sacked = block_offsets
                .last()
                .map(|&(start, _)| send_start.advance(start));
            let mut merged_len = 0;
            for read in 0..block_offsets.len() {
                let (start, end) = block_offsets[read];
                if merged_len > 0 && start < block_offsets[merged_len - 1].1 {
                    block_offsets[merged_len - 1].1 = block_offsets[merged_len - 1].1.max(end);
                } else {
                    block_offsets[merged_len] = (start, end);
                    merged_len += 1;
                }
            }
            block_offsets.truncate(merged_len);
            highest_sacked
        } else {
            // Zero or one clipped block: nothing to sort or merge; a single
            // block is already in order and cannot overlap anything.
            block_offsets
                .last()
                .map(|&(start, _)| send_start.advance(start))
        }
    }

    /// The exclusive forward offset (from `send_start`) of the last unacked
    /// sequence this ACK can possibly deliver: the maximum of the cumulative
    /// front (when current) and the end of the highest clipped selective
    /// block.  Unacked sequences at or beyond this offset are never acked by
    /// this ACK and never carry selective evidence, so sender-side analysis
    /// can stop there without scanning the whole send window.
    pub(crate) fn prepare(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        block_offsets: &mut Vec<(u64, u64)>,
    ) -> AckPreparation {
        let highest_sacked = self.normalize_block_offsets(send_start, sent_span, block_offsets);
        let cumulative = match self.cumulative_position(send_start, sent_span) {
            CumulativePosition::Current(offset) => Some(offset),
            CumulativePosition::Stale | CumulativePosition::Future => None,
        };
        let selective_end = block_offsets.last().map_or(0, |&(_, end)| end);
        AckPreparation {
            cumulative,
            highest_sacked,
            raw_blocks_present: !self.blocks.is_empty(),
            relevant_unacked_end: cumulative.unwrap_or(0).max(selective_end),
        }
    }

    /// The exclusive forward offset (from `send_start`) of the last unacked
    /// sequence this ACK can possibly deliver: the maximum of the cumulative
    /// front (when current) and the end of the highest clipped selective
    /// block.  Unacked sequences at or beyond this offset are never acked by
    /// this ACK and never carry selective evidence, so sender-side analysis
    /// can stop there without scanning the whole send window.

    #[cfg(test)]
    pub(crate) fn relevant_unacked_end_offset(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
    ) -> u64 {
        self.prepare(send_start, sent_span, &mut Vec::new())
            .relevant_unacked_end
    }

    /// One bounded wrapping-safe linear analysis of this ACK against the
    /// sender's in-flight window: computes, in a single pass per direction,
    /// which 'unacked' sequences are delivered (cumulative prefix plus
    /// normalized selective blocks) and the dup-ACK-pass evidence above each
    /// one, plus the analysis summary.
    /// 'unacked' must be in increasing logical offset order (as produced by "SendWindow::iter"). It may be
    /// truncated at ['Self::relevant_unacked_end_offset']: sequences at or beyond the bound are never acked and never contribute evidence.
    /// 'block_offsets', 'acked', and 'sacked_above' are caller-owned reusable buffers. A cumulative-only ACK leaves 'sacked_above' empty because the
    /// returned 'has_sack_evidence' is false and the caller must not inspect
    /// per-sequence evidence; otherwise it is resized to 'unacked.len()'.
    #[cfg(test)]
    pub(crate) fn analyze(
        &self,
        send_start: SequenceNumber,
        sent_span: u64,
        unacked: &[SequenceNumber],
        block_offsets: &mut Vec<(u64, u64)>,
        acked: &mut Vec<SequenceNumber>,
        sacked_above: &mut Vec<u32>,
    ) -> AckAnalysis {
        let prepared = self.prepare(send_start, sent_span, block_offsets);
        self.analyze_prepared(
            prepared,
            send_start,
            unacked,
            block_offsets,
            acked,
            sacked_above,
        )
    }

    pub(crate) fn analyze_prepared(
        &self,
        prepared: AckPreparation,
        send_start: SequenceNumber,
        unacked: &[SequenceNumber],
        block_offsets: &[(u64, u64)],
        acked: &mut Vec<SequenceNumber>,
        sacked_above: &mut Vec<u32>,
    ) -> AckAnalysis {
        let mut block_index = 0;
        for &seq in unacked {
            let offset = send_start.forward_distance_to(seq);
            while block_index < block_offsets.len() && block_offsets[block_index].1 <= offset {
                block_index += 1;
            }
            let cumulatively_acked = prepared.cumulative.is_some_and(|front| offset < front);
            let selectively_acked = block_offsets
                .get(block_index)
                .is_some_and(|&(start, end)| start <= offset && offset < end);
            if cumulatively_acked || selectively_acked {
                acked.push(seq);
            }
        }
        let analysis = AckAnalysis {
            cumulative_is_current: prepared.cumulative.is_some(),
            highest_sacked: prepared.highest_sacked,
            has_sack_evidence: !block_offsets.is_empty(),
        };
        sacked_above.clear();
        if !prepared.raw_blocks_present {
            return analysis;
        }
        sacked_above.resize(unacked.len(), 0);
        if block_offsets.is_empty() {
            return analysis;
        }
        let mut block_index = block_offsets.len();
        let mut complete_blocks_above = 0u64;
        for (sequence_index, &seq) in unacked.iter().enumerate().rev() {
            let offset = send_start.forward_distance_to(seq);
            while block_index > 0 && block_offsets[block_index - 1].0 > offset {
                block_index -= 1;
                let (start, end) = block_offsets[block_index];
                complete_blocks_above = complete_blocks_above.saturating_add(end - start);
            }
            let inside_current_block = block_index
                .checked_sub(1)
                .map(|index| block_offsets[index])
                .filter(|&(start, end)| start <= offset && offset < end)
                .map_or(0, |(_, end)| end - offset - 1);
            sacked_above[sequence_index] =
                u32::try_from(complete_blocks_above.saturating_add(inside_current_block))
                    .unwrap_or(u32::MAX);
        }
        analysis
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
        let mut block_offsets = Vec::new();
        let mut acked = Vec::new();
        let mut evidence = Vec::new();

        // Fully-acked window: cumulative next == sent_end (seq 0 after wrap).
        let recved = AckBlocks::new(seq(0), &[]);
        recved.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(
            acked.len(),
            3,
            "a fully-acked window must release everything"
        );
        assert!(
            evidence.is_empty(),
            "a cumulative-only ACK carries no selective-loss evidence"
        );

        // Cumulative next exactly one past the first packet.
        let recved = AckBlocks::new(seq(u64::MAX - 1), &[]);
        acked.clear();
        recved.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(acked, vec![seq(u64::MAX - 2)]);

        // Stale cumulative next (before the window): nothing released by the
        // prefix; a clipped selective block still releases in-window packets.
        let blocks = [iv(u64::MAX - 1, 2)];
        let stale = AckBlocks::new(seq(u64::MAX - 5), &blocks);
        acked.clear();
        stale.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(acked, vec![seq(u64::MAX - 1), seq(u64::MAX)]);

        // Future cumulative next (beyond what was sent): no prefix release.
        let future = AckBlocks::new(seq(5), &[]);
        acked.clear();
        future.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert!(acked.is_empty());

        // A block beyond the sent span is clipped away entirely.
        let blocks = [iv(5, 10)];
        let beyond = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        acked.clear();
        beyond.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert!(
            acked.is_empty(),
            "a range beyond sent_span must release nothing"
        );

        // Evidence counts only clipped ranges: [MAX, 2) clips to {MAX} at
        // the sent end, so it is one pass above MAX-2 and MAX-1 and zero at
        // MAX itself.
        let blocks = [iv(u64::MAX, 2)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        acked.clear();
        recved.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(evidence, vec![1, 1, 0]);

        // [u64::MAX-1, 2) is clipped at sent_end (seq 0); the highest SACKed
        // start is u64::MAX-1.
        let blocks = [iv(u64::MAX - 1, 2)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        acked.clear();
        let analysis = recved.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(analysis.highest_sacked, Some(seq(u64::MAX - 1)));
        // A fully-clipped range must not advance the reorder bound.
        let blocks = [iv(0, 10)];
        let recved = AckBlocks::new(seq(u64::MAX - 2), &blocks);
        acked.clear();
        let analysis = recved.analyze(
            send_start,
            sent_span,
            &in_flight,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        assert_eq!(
            analysis.highest_sacked, None,
            "a fully-clipped range must not advance the reorder bound"
        );
    }

    #[test]
    fn stale_cumulative_and_future_cumulative_never_report_progress() {
        let send_start = seq(100);
        let sent_span = 5u64;
        let in_flight: Vec<_> = (100..105).map(seq).collect();
        let mut block_offsets = Vec::new();
        let mut acked = Vec::new();
        let mut evidence = Vec::new();
        for (next, want) in [
            (seq(99), vec![]),
            (seq(106), vec![]),
            (seq(103), vec![seq(100), seq(101), seq(102)]),
        ] {
            let recved = AckBlocks::new(next, &[]);
            acked.clear();
            recved.analyze(
                send_start,
                sent_span,
                &in_flight,
                &mut block_offsets,
                &mut acked,
                &mut evidence,
            );
            assert_eq!(acked, want, "cumulative next {next}");
        }
    }

    #[test]
    fn sender_analysis_is_linear_and_wrapping_safe() {
        // Send window of 8 packets straddling u64::MAX -> 0.  The cumulative
        // next at offset 2 covers offsets 0..2; selective blocks at offsets
        // [3, 5) (seqs 0, 1) and [6, 7) (seq 3) cover the rest.
        let send_start = seq(u64::MAX - 2);
        let sent_span = 8u64;
        let unacked = [
            seq(u64::MAX - 2),
            seq(u64::MAX - 1),
            seq(u64::MAX),
            seq(0),
            seq(1),
            seq(2),
            seq(3),
            seq(4),
        ];
        let blocks = [iv(0, 2), iv(3, 1)];
        let recved = AckBlocks::new(send_start.advance(2), &blocks);
        let mut block_offsets = Vec::new();
        let mut acked = Vec::new();
        let mut evidence = Vec::new();
        recved.analyze(
            send_start,
            sent_span,
            &unacked,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        let delivered: Vec<u64> = acked
            .iter()
            .map(|s| send_start.forward_distance_to(*s))
            .collect();
        assert_eq!(delivered, vec![0, 1, 3, 4, 6]);
        assert_eq!(evidence, vec![3, 3, 3, 2, 1, 1, 0, 0]);
    }

    #[test]
    fn relevant_unacked_end_uses_clipped_cumulative_and_selective_bounds() {
        let send_start = seq(100);
        let sent_span = 8u64;
        let unacked: Vec<SequenceNumber> = (0..sent_span).map(|o| send_start.advance(o)).collect();

        // Cumulative-only: the exclusive bound is the cumulative front.
        let recved = AckBlocks::new(seq(104), &[]);
        assert_eq!(recved.relevant_unacked_end_offset(send_start, sent_span), 4);

        // Selective blocks extend the bound to the highest clipped end.
        let blocks = [iv(102, 2), iv(105, 1)];
        let recved = AckBlocks::new(seq(101), &blocks);
        assert_eq!(recved.relevant_unacked_end_offset(send_start, sent_span), 6);

        // A block clipped at the sent span bounds at sent_span.
        let blocks = [iv(105, 100)];
        let recved = AckBlocks::new(seq(101), &blocks);
        assert_eq!(recved.relevant_unacked_end_offset(send_start, sent_span), 8);

        // A stale/future cumulative next contributes nothing; only the
        // clipped selective bound counts.
        let blocks = [iv(102, 2)];
        let recved = AckBlocks::new(seq(99), &blocks);
        assert_eq!(recved.relevant_unacked_end_offset(send_start, sent_span), 4);

        // Analyzing over the bounded prefix agrees with the full-window
        // analysis for both acked sequences and evidence: sequences at or
        // beyond the bound are never delivered and never earn evidence.
        let blocks = [iv(102, 2), iv(105, 1)];
        let recved = AckBlocks::new(seq(101), &blocks);
        let end = recved.relevant_unacked_end_offset(send_start, sent_span);
        let bounded: Vec<SequenceNumber> = unacked
            .iter()
            .copied()
            .take_while(|s| send_start.forward_distance_to(*s) < end)
            .collect();
        assert_eq!(bounded.len(), 6);
        let mut block_offsets = Vec::new();
        let mut acked = Vec::new();
        let mut evidence = Vec::new();
        let mut bounded_acked = Vec::new();
        let mut bounded_evidence = Vec::new();
        recved.analyze(
            send_start,
            sent_span,
            &unacked,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        recved.analyze(
            send_start,
            sent_span,
            &bounded,
            &mut block_offsets,
            &mut bounded_acked,
            &mut bounded_evidence,
        );
        assert_eq!(bounded_acked, acked);
        assert_eq!(bounded_evidence, evidence[..bounded.len()]);
    }

    #[test]
    fn overlapping_sacks_do_not_multiply_loss_evidence() {
        let send_start = seq(0);
        let sent_span = 8u64;
        let unacked: Vec<_> = (0..8).map(seq).collect();
        // Two overlapping wire ranges [3, 6) and [4, 7): normalized to the
        // union [3, 7), so membership is a set and evidence cannot
        // double-count the overlap.
        let blocks = [iv(3, 3), iv(4, 3)];
        let recved = AckBlocks::new(seq(0), &blocks);
        let mut block_offsets = Vec::new();
        let mut acked = Vec::new();
        let mut evidence = Vec::new();
        recved.analyze(
            send_start,
            sent_span,
            &unacked,
            &mut block_offsets,
            &mut acked,
            &mut evidence,
        );
        let delivered: Vec<u64> = acked
            .iter()
            .map(|s| send_start.forward_distance_to(*s))
            .collect();
        assert_eq!(delivered, vec![3, 4, 5, 6], "only 3..=6 is acknowledged");
        assert_eq!(
            evidence[0], 4,
            "the union [3, 7) is exactly four passes above sequence zero, not six"
        );
    }

    #[test]
    fn linear_sender_analysis_matches_a_packet_by_packet_model() {
        let mut rng = crate::testing::SplitMix64::new(0x05ee_da11);
        for case in 0..256 {
            let send_start = match case % 4 {
                0 => seq(0),
                1 => seq(u64::MAX - 1),
                2 => seq(u64::MAX / 2),
                _ => seq(rng.next_u64()),
            };
            let sent_span = 1 + rng.next_u64() % 64;
            // Cumulative next: current / future / stale / arbitrary-wrapping.
            let cumulative_offset = match case % 4 {
                0 => rng.next_u64() % (sent_span + 1),
                1 => sent_span + 1 + rng.next_u64() % (HALF_SEQUENCE_SPACE - sent_span),
                2 => HALF_SEQUENCE_SPACE + 1 + rng.next_u64() % (u64::MAX - HALF_SEQUENCE_SPACE),
                _ => rng.next_u64(),
            };
            let cumulative_next = send_start.advance(cumulative_offset);
            // Up to 64 wire ranges, unsorted, overlapping, half biased near
            // the window and half arbitrary (hostile).
            let num_blocks = (rng.next_u64() % 65) as usize;
            let mut wire_blocks: Vec<(u64, u64)> = Vec::new();
            for _ in 0..num_blocks {
                let start = if rng.next_u64().is_multiple_of(2) {
                    send_start
                        .advance(rng.next_u64() % (sent_span * 3 + 1))
                        .to_wire()
                } else {
                    rng.next_u64()
                };
                let size = 1 + rng.next_u64() % 32;
                wire_blocks.push((start, size));
            }
            let blocks: Vec<AckInterval> = wire_blocks
                .iter()
                .map(|&(start, size)| AckInterval {
                    start: seq(start),
                    size: NonZeroU64::new(size).unwrap(),
                })
                .collect();
            let recved = AckBlocks::new(cumulative_next, &blocks);
            let unacked: Vec<SequenceNumber> = (0..sent_span)
                .map(|offset| send_start.advance(offset))
                .collect();
            let mut block_offsets = Vec::new();
            let mut acked = Vec::new();
            let mut evidence = Vec::new();
            recved.analyze(
                send_start,
                sent_span,
                &unacked,
                &mut block_offsets,
                &mut acked,
                &mut evidence,
            );

            // Reference: a boolean packet-set model of clipped coverage.
            let mut covered = vec![false; sent_span as usize];
            for &(raw_start, size) in &wire_blocks {
                let mut rel_start = send_start.forward_distance_to(seq(raw_start));
                if rel_start > HALF_SEQUENCE_SPACE {
                    rel_start = 0;
                }
                let mut rel_end = send_start.forward_distance_to(seq(raw_start).advance(size));
                if rel_end > HALF_SEQUENCE_SPACE {
                    rel_end = 0;
                }
                let rel_end = rel_end.min(sent_span);
                if rel_end <= rel_start || rel_start >= sent_span {
                    continue;
                }
                for offset in rel_start..rel_end {
                    covered[offset as usize] = true;
                }
            }
            let cumulative_front = if cumulative_offset <= sent_span {
                Some(cumulative_offset)
            } else {
                None
            };
            let mut ref_acked: Vec<u64> = Vec::new();
            let mut ref_evidence = if wire_blocks.is_empty() {
                Vec::new()
            } else {
                vec![0; sent_span as usize]
            };
            for offset in 0..sent_span {
                let index = offset as usize;
                if cumulative_front.is_some_and(|front| offset < front) || covered[index] {
                    ref_acked.push(offset);
                }
                if !wire_blocks.is_empty() {
                    ref_evidence[index] =
                        covered[index + 1..].iter().filter(|&&c| c).count() as u32;
                }
            }
            let delivered: Vec<u64> = acked
                .iter()
                .map(|s| send_start.forward_distance_to(*s))
                .collect();
            assert_eq!(delivered, ref_acked, "case {case}: delivered mismatch");
            assert_eq!(evidence, ref_evidence, "case {case}: evidence mismatch");
        }
    }
}
