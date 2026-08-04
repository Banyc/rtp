use core::num::NonZeroU64;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct SackIntervals {
    start_to_size: BTreeMap<u64, NonZeroU64>,
}
impl SackIntervals {
    pub fn new() -> Self {
        Self {
            start_to_size: BTreeMap::new(),
        }
    }

    fn neighbours(&self, seq: u64) -> (Option<SackBlock>, Option<SackBlock>) {
        let block = |(&start, &size): (&u64, &NonZeroU64)| SackBlock { start, size };
        let prev = self.start_to_size.range(..seq).next_back().map(block);
        let next = self.start_to_size.range(seq..).next().map(block);
        (prev, next)
    }

    pub fn insert(&mut self, seq: u64) {
        let (prev, next) = self.neighbours(seq);
        let this = SackBlock {
            start: seq,
            size: NonZeroU64::new(1).unwrap(),
        };
        let mut merge_pair = |this: SackBlock, other: Option<SackBlock>| -> SackBlock {
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

    pub fn blocks(&self) -> impl Iterator<Item = SackBlock> + '_ {
        self.start_to_size.iter().map(|(s, n)| SackBlock {
            start: *s,
            size: *n,
        })
    }

    pub fn len(&self) -> usize {
        self.start_to_size.len()
    }
}
impl Default for SackIntervals {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SackBlockSeq<'a> {
    blocks: &'a [SackBlock],
}
impl<'a> SackBlockSeq<'a> {
    /// Elements in `blocks` must be in increasing order.
    pub fn new(blocks: &'a [SackBlock]) -> Self {
        Self { blocks }
    }

    pub fn blocks(&self) -> &'a [SackBlock] {
        self.blocks
    }

    pub fn out_of_order_seq_end(&self) -> Option<u64> {
        Some(self.blocks.last()?.start)
    }

    pub fn first_unacked(&self) -> u64 {
        let Some(first_acked) = self.blocks.first() else {
            return 0;
        };
        if first_acked.start != 0 {
            return 0;
        }
        first_acked.start + first_acked.size.get()
    }

    /// `unacked` must be in increasing order.
    pub fn ack(&self, unacked: &[u64], ack: &mut Vec<u64>) {
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

            ack.push(unacked);
            unacked_i += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SackBlock {
    pub start: u64,
    pub size: NonZeroU64,
}
impl SackBlock {
    pub fn end(&self) -> u64 {
        self.start.saturating_add(self.size.get())
    }

    pub fn contains(&self, seq: u64) -> bool {
        if seq < self.start {
            return false;
        }
        seq < self.end()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sack_block() {
        let a = SackBlock {
            start: 1,
            size: NonZeroU64::new(1).unwrap(),
        };
        let b = SackBlock {
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

        let c = SackBlock {
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
    fn test_sack_intervals() {
        let mut a = SackIntervals::new();
        a.insert(1);
        a.insert(3);
        a.insert(2);
        assert_eq!(a.start_to_size.len(), 1);
    }

    #[test]
    fn a_block_reaching_past_the_end_of_the_space_still_acknowledges() {
        let block = SackBlock {
            start: 1,
            size: NonZeroU64::new(u64::MAX).unwrap(),
        };
        assert_eq!(block.end(), u64::MAX, "the end wrapped below the start");
        let recved = SackBlockSeq::new(std::slice::from_ref(&block));
        let mut acked = Vec::new();
        recved.ack(&[5, 9], &mut acked);
        assert_eq!(
            acked,
            vec![5, 9],
            "a block covering the whole space acknowledged nothing"
        );
    }

    fn insert_cost(holes: u64) -> f64 {
        const N: u64 = 20_000;
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut q = SackIntervals::new();
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
        let mut q = SackIntervals::new();
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
