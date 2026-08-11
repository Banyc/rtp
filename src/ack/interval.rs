//! ACK interval algebra: the domain model for what has been received.
//!
//! [`AckHistory`] is the receiver-side history of received sequence
//! numbers, kept as a merged set of disjoint intervals. [`AckBlocks`] is
//! the sender-side view over the blocks received from the peer in an ACK
//! datagram, plus the pure calculations derived from them: which unacked
//! sequences the blocks cover, where the peer's cumulative ack front is,
//! and how many newer in-flight packets are acked past each unacked packet.
//!
//! Everything an ACK *means* is computed here; the transmission and
//! reliable layers only record receipts, carry the bytes, and apply the
//! results.

use core::num::NonZeroU64;
use std::collections::BTreeMap;

/// One contiguous interval of received sequence numbers: `[start, start+size)`.
#[derive(Debug, Clone, Copy)]
pub struct AckInterval {
    pub start: u64,
    pub size: NonZeroU64,
}
impl AckInterval {
    /// One past the last sequence covered by this interval, saturating at
    /// `u64::MAX` so an interval reaching the end of the sequence space
    /// still acknowledges.
    pub fn end(&self) -> u64 {
        self.start.saturating_add(self.size.get())
    }

    pub fn contains(&self, seq: u64) -> bool {
        if seq < self.start {
            return false;
        }
        seq < self.end()
    }

    /// Merge two intervals that touch or overlap into one covering both;
    /// `None` when they are disjoint.
    pub fn merge(&self, other: &Self) -> Option<Self> {
        if self.start == other.start {
            return Some(Self {
                start: self.start,
                size: self.size.max(other.size),
            });
        }
        if other.start < self.start {
            return other.merge(self);
        }
        if self.end() < other.start {
            return None;
        }
        let size = other.end() - self.start;
        Some(Self {
            start: self.start,
            size: NonZeroU64::new(size).unwrap().max(self.size),
        })
    }
}

/// Receiver-side ACK history: received sequence numbers as a merged set of
/// disjoint intervals, so the cost of maintaining the history is bounded by
/// the number of holes, not the number of packets received.
#[derive(Debug, Clone)]
pub struct AckHistory {
    start_to_size: BTreeMap<u64, NonZeroU64>,
}
impl AckHistory {
    pub fn new() -> Self {
        Self {
            start_to_size: BTreeMap::new(),
        }
    }

    fn neighbours(&self, seq: u64) -> (Option<AckInterval>, Option<AckInterval>) {
        let block = |(&start, &size): (&u64, &NonZeroU64)| AckInterval { start, size };
        let prev = self.start_to_size.range(..seq).next_back().map(block);
        let next = self.start_to_size.range(seq..).next().map(block);
        (prev, next)
    }

    /// Record a received sequence number, merging it into the neighbouring
    /// intervals when they touch.
    pub fn insert(&mut self, seq: u64) {
        let (prev, next) = self.neighbours(seq);
        let this = AckInterval {
            start: seq,
            size: NonZeroU64::new(1).unwrap(),
        };
        let mut merge_pair = |this: AckInterval, other: Option<AckInterval>| -> AckInterval {
            let Some(other) = other else {
                return this;
            };
            let Some(merged) = other.merge(&this) else {
                return this;
            };
            self.start_to_size.remove(&other.start);
            merged
        };
        let this = merge_pair(this, prev);
        let this = merge_pair(this, next);
        self.start_to_size.insert(this.start, this.size);
    }

    pub fn blocks(&self) -> impl Iterator<Item = AckInterval> + '_ {
        self.start_to_size.iter().map(|(s, n)| AckInterval {
            start: *s,
            size: *n,
        })
    }

    pub fn len(&self) -> usize {
        self.start_to_size.len()
    }

    /// Select a page of at most `max_blocks` blocks starting at
    /// `first_block_index`, in increasing order: the cumulative head (page
    /// 0) plus one deep page from the resume cursor. This is the wire-
    /// selection calculation; the flush loop only carries the bytes out.
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

/// Sender-side view over the ACK blocks received from the peer. All
/// interpretation of what an incoming ACK means is computed here.
#[derive(Debug, Clone)]
pub struct AckBlocks<'a> {
    blocks: &'a [AckInterval],
}
impl<'a> AckBlocks<'a> {
    /// Elements in `blocks` must be in increasing order.
    pub fn new(blocks: &'a [AckInterval]) -> Self {
        Self { blocks }
    }

    /// The start of the highest acked block: the newest sequence the peer
    /// has reported, used to bound how far an out-of-order ACK can advance
    /// the sender's reorder knowledge.
    pub fn out_of_order_seq_end(&self) -> Option<u64> {
        Some(self.blocks.last()?.start)
    }

    /// The first sequence the peer has not acked: the cumulative ack front.
    /// `0` means nothing was acked (or the first block does not start at 0).
    pub fn first_unacked(&self) -> u64 {
        let Some(first_acked) = self.blocks.first() else {
            return 0;
        };
        if first_acked.start != 0 {
            return 0;
        }
        first_acked.start + first_acked.size.get()
    }

    /// Push, in increasing order, every sequence in `unacked` that is
    /// covered by these blocks. `unacked` must be in increasing order.
    pub fn acked_set(&self, unacked: &[u64], acked: &mut Vec<u64>) {
        if self.blocks.is_empty() {
            return;
        }
        let mut unacked_i = 0;
        let mut block_i = 0;
        while block_i < self.blocks.len() && unacked_i < unacked.len() {
            let block = self.blocks[block_i];
            let unacked = unacked[unacked_i];

            if unacked < block.start {
                unacked_i += 1;
                continue;
            }
            if !block.contains(unacked) {
                block_i += 1;
                continue;
            }

            acked.push(unacked);
            unacked_i += 1;
        }
    }

    /// How many newer in-flight packets these blocks ack past
    /// `unacked_seq`, bounded by `sent_end` (the sequence one past the last
    /// packet ever sent). Each acked packet with a higher sequence counts
    /// as one "pass"; the sum across blocks is the dup-ACK-pass evidence
    /// used by evidence-gated fast loss, mirroring the classic dup-ACK
    /// threshold of 3.
    pub fn sacked_above_count(&self, unacked_seq: u64, sent_end: u64) -> u32 {
        let mut passes: u32 = 0;
        for ball in self.blocks {
            let ball_end = ball.end().min(sent_end);
            let newer = if unacked_seq < ball.start {
                ball_end.saturating_sub(ball.start)
            } else if unacked_seq < ball_end {
                ball_end - unacked_seq - 1
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

    #[test]
    fn test_ack_interval() {
        let a = AckInterval {
            start: 1,
            size: NonZeroU64::new(1).unwrap(),
        };
        let b = AckInterval {
            start: 1,
            size: NonZeroU64::new(2).unwrap(),
        };
        {
            let merged = a.merge(&b).unwrap();
            assert_eq!(merged.start, 1);
            assert_eq!(merged.size.get(), 2);
        }
        {
            let merged = b.merge(&a).unwrap();
            assert_eq!(merged.start, 1);
            assert_eq!(merged.size.get(), 2);
        }

        let c = AckInterval {
            start: 2,
            size: NonZeroU64::new(1).unwrap(),
        };
        {
            let merged = a.merge(&c).unwrap();
            assert_eq!(merged.start, 1);
            assert_eq!(merged.size.get(), 2);
        }
    }

    #[test]
    fn test_ack_history() {
        let mut a = AckHistory::new();
        a.insert(1);
        a.insert(3);
        a.insert(2);
        assert_eq!(a.start_to_size.len(), 1);
    }

    #[test]
    fn a_block_reaching_past_the_end_of_the_space_still_acknowledges() {
        let block = AckInterval {
            start: 1,
            size: NonZeroU64::new(u64::MAX).unwrap(),
        };
        assert_eq!(block.end(), u64::MAX, "the end wrapped below the start");
        let recved = AckBlocks::new(std::slice::from_ref(&block));
        let mut acked = Vec::new();
        recved.acked_set(&[5, 9], &mut acked);
        assert_eq!(
            acked,
            vec![5, 9],
            "a block covering the whole space acknowledged nothing"
        );
    }

    #[test]
    fn select_blocks_pages_from_the_resume_cursor() {
        let mut history = AckHistory::new();
        for seq in 0..10 {
            history.insert(seq);
        }
        // All ten merge into one block [0, 10).
        let page: Vec<_> = history.select_blocks(0, 4).collect();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].start, 0);
        assert_eq!(page[0].size.get(), 10);

        // A cursor past the single block selects nothing.
        assert!(history.select_blocks(1, 4).next().is_none());
    }

    #[test]
    fn select_blocks_pages_deep_history() {
        // Non-contiguous receipts produce one block per run.
        let mut history = AckHistory::new();
        for seq in 0..100 {
            history.insert(seq * 2);
        }
        assert_eq!(history.len(), 100);
        let page: Vec<_> = history.select_blocks(10, 8).collect();
        assert_eq!(page.len(), 8);
        assert_eq!(page[0].start, 20);
        assert_eq!(page[7].start, 34);
    }

    #[test]
    fn sacked_above_counts_newer_acked_packets_per_block() {
        let blocks = [
            AckInterval {
                start: 1,
                size: NonZeroU64::new(1).unwrap(),
            },
            AckInterval {
                start: 3,
                size: NonZeroU64::new(2).unwrap(),
            },
        ];
        let recved = AckBlocks::new(&blocks);
        // Seq 0: one newer acked in the first block (1), two in the second
        // (3, 4) => 3 passes.
        assert_eq!(recved.sacked_above_count(0, u64::MAX), 3);
        // Seq 2: the first block is entirely below it, the second block
        // starts above it (3, 4) => 2 passes.
        assert_eq!(recved.sacked_above_count(2, u64::MAX), 2);
        // Seq 5: everything below => no passes.
        assert_eq!(recved.sacked_above_count(5, u64::MAX), 0);
        // Blocks are bounded by sent_end: only acked packets that were
        // actually sent count as evidence.
        assert_eq!(recved.sacked_above_count(0, 4), 2);
    }

    #[test]
    fn sacked_above_a_block_containing_the_seq_counts_below_it() {
        // An unacked seq sitting inside a block (possible when the peer
        // re-acks a prefix the sender already cleared) counts only the
        // acked packets above it inside the block.
        let blocks = [AckInterval {
            start: 0,
            size: NonZeroU64::new(5).unwrap(),
        }];
        let recved = AckBlocks::new(&blocks);
        assert_eq!(recved.sacked_above_count(2, u64::MAX), 2);
        assert_eq!(recved.sacked_above_count(4, u64::MAX), 0);
    }

    fn insert_cost(holes: u64) -> f64 {
        const N: u64 = 20_000;
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut q = AckHistory::new();
            for i in 0..holes {
                q.insert(i * 2);
            }
            let base = holes * 2 + 1_000;
            let start = std::time::Instant::now();
            for i in 0..N {
                q.insert(base + i);
            }
            best = best.min(start.elapsed().as_secs_f64() / N as f64 * 1e9);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/insert ratio; run with cargo test --release -- --ignored"]
    fn a_queue_full_of_holes_costs_no_more_per_packet() {
        let few = insert_cost(16);
        let many = insert_cost(4096);
        assert!(
            many < few * 8.0,
            "{many:.1} ns/insert at 4096 blocks against {few:.1} ns at 16: the per-packet cost grows with the number of holes"
        );
    }

    fn len_cost(holes: u64) -> f64 {
        const N: u64 = 200_000;
        let mut q = AckHistory::new();
        for i in 0..holes {
            q.insert(i * 2);
        }
        let mut best = f64::MAX;
        for _ in 0..3 {
            let start = std::time::Instant::now();
            for _ in 0..N {
                std::hint::black_box(std::hint::black_box(&q).len());
            }
            best = best.min(start.elapsed().as_secs_f64() / N as f64 * 1e9);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/len ratio; run with cargo test --release -- --ignored"]
    fn asking_the_queue_how_big_it_is_does_not_walk_it() {
        let few = len_cost(16);
        let many = len_cost(4096);
        assert!(
            many < few * 8.0,
            "{many:.1} ns/len at 4096 blocks against {few:.1} ns at 16: the size of the ack history is being counted rather than read"
        );
    }
}
