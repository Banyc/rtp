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

    pub fn insert(&mut self, seq: u64) {
        let mut prev = None;
        let mut next = None;
        for ball in self.balls() {
            if ball.start < seq {
                prev = Some(ball);
                continue;
            }
            next = Some(ball);
            break;
        }
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
    /// Elements in `seq` must be in increasing order.
    pub fn new(balls: &'a [AckBall]) -> Self {
        Self { balls }
    }

    pub fn out_of_order_seq_end(&self) -> Option<u64> {
        Some(self.balls.last()?.start)
    }

    /// `pipe` must be in increasing order.
    ///
    /// Return the end of immediate retransmission sequence
    pub fn ack(&self, pipe: &[u64], ack: &mut Vec<u64>) {
        if self.balls.is_empty() {
            return;
        }
        let mut pipe_i = 0;
        let mut ball_i = 0;
        while ball_i < self.balls.len() && pipe_i < pipe.len() {
            let ball = self.balls[ball_i];
            let seq = pipe[pipe_i];

            if seq < ball.start {
                pipe_i += 1;
                continue;
            }
            if !ball.contains(seq) {
                ball_i += 1;
                continue;
            }

            ack.push(seq);
            pipe_i += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AckBall {
    pub start: u64,
    pub size: NonZeroU64,
}
impl AckBall {
    pub fn contains(&self, seq: u64) -> bool {
        if seq < self.start {
            return false;
        }
        seq < self.start + self.size.get()
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
        if self.start + self.size.get() < other.start {
            return None;
        }
        let size = other.start - self.start + other.size.get();
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
}
