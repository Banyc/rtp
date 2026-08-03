use core::num::NonZeroU64;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct AckQueue {
    start_to_size: BTreeMap<u64, NonZeroU64>,
}
impl AckQueue {
    pub fn new() -> Self {
        Self {
            start_to_size: BTreeMap::new(),
        }
    }

    fn neighbours(&self, seq: u64) -> (Option<AckBall>, Option<AckBall>) {
        let ball = |(&start, &size): (&u64, &NonZeroU64)| AckBall { start, size };
        let prev = self.start_to_size.range(..seq).next_back().map(ball);
        let next = self.start_to_size.range(seq..).next().map(ball);
        (prev, next)
    }

    pub fn insert(&mut self, seq: u64) {
        let (prev, next) = self.neighbours(seq);
        let this = AckBall {
            start: seq,
            size: NonZeroU64::new(1).unwrap(),
        };
        let mut merge_pair = |this: AckBall, other: Option<AckBall>| -> AckBall {
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

    pub fn balls(&self) -> impl Iterator<Item = AckBall> + '_ {
        self.start_to_size.iter().map(|(s, n)| AckBall {
            start: *s,
            size: *n,
        })
    }

    pub fn len(&self) -> usize {
        self.start_to_size.len()
    }

    pub fn is_empty(&self) -> bool {
        self.start_to_size.is_empty()
    }
}
impl Default for AckQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct AckBallSequence<'a> {
    balls: &'a [AckBall],
}
impl<'a> AckBallSequence<'a> {
    /// Elements in `balls` must be in increasing order.
    pub fn new(balls: &'a [AckBall]) -> Self {
        Self { balls }
    }

    pub fn balls(&self) -> &'a [AckBall] {
        self.balls
    }

    pub fn out_of_order_seq_end(&self) -> Option<u64> {
        Some(self.balls.last()?.start)
    }

    pub fn first_unacked(&self) -> u64 {
        let Some(first_acked) = self.balls.first() else {
            return 0;
        };
        if first_acked.start != 0 {
            return 0;
        }
        first_acked.start + first_acked.size.get()
    }

    /// `unacked` must be in increasing order.
    pub fn ack(&self, unacked: &[u64], ack: &mut Vec<u64>) {
        if self.balls.is_empty() {
            return;
        }
        let mut unacked_i = 0;
        let mut ball_i = 0;
        while ball_i < self.balls.len() && unacked_i < unacked.len() {
            let ball = self.balls[ball_i];
            let unacked = unacked[unacked_i];

            if unacked < ball.start {
                unacked_i += 1;
                continue;
            }
            if !ball.contains(unacked) {
                ball_i += 1;
                continue;
            }

            ack.push(unacked);
            unacked_i += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AckBall {
    pub start: u64,
    pub size: NonZeroU64,
}
impl AckBall {
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
    fn test_ack_ball() {
        let a = AckBall {
            start: 1,
            size: NonZeroU64::new(1).unwrap(),
        };
        let b = AckBall {
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

        let c = AckBall {
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
    fn test_ack_queue() {
        let mut a = AckQueue::new();
        a.insert(1);
        a.insert(3);
        a.insert(2);
        assert_eq!(a.start_to_size.len(), 1);
    }

    #[test]
    fn a_ball_reaching_past_the_end_of_the_space_still_acknowledges() {
        let ball = AckBall {
            start: 1,
            size: NonZeroU64::new(u64::MAX).unwrap(),
        };
        assert_eq!(ball.end(), u64::MAX, "the end wrapped below the start");
        let recved = AckBallSequence::new(std::slice::from_ref(&ball));
        let mut acked = Vec::new();
        recved.ack(&[5, 9], &mut acked);
        assert_eq!(
            acked,
            vec![5, 9],
            "a ball covering the whole space acknowledged nothing"
        );
    }

    fn insert_cost(holes: u64) -> f64 {
        const N: u64 = 20_000;
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut q = AckQueue::new();
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
            "{many:.1} ns/insert at 4096 balls against {few:.1} ns at 16: the per-packet cost grows with the number of holes"
        );
    }

    fn len_cost(holes: u64) -> f64 {
        const N: u64 = 200_000;
        let mut q = AckQueue::new();
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
            "{many:.1} ns/len at 4096 balls against {few:.1} ns at 16: the size of the ack history is being counted rather than read"
        );
    }
}
