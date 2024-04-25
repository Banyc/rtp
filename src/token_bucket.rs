use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use strict_num::{NonZeroPositiveF64, NormalizedF64};

#[derive(Debug, Clone)]
pub struct TokenBucket {
    sec_per_token: NonZeroPositiveF64,
    max_tokens: NonZeroUsize,
    tokens: usize,
    coining_token: NormalizedF64,
    last_update: Instant,
}
impl TokenBucket {
    pub fn new(thruput: NonZeroPositiveF64, max_tokens: NonZeroUsize, now: Instant) -> Self {
        let sec_per_token = NonZeroPositiveF64::new(1. / thruput.get()).unwrap();
        Self {
            sec_per_token,
            max_tokens,
            tokens: 0,
            coining_token: NormalizedF64::new(0.).unwrap(),
            last_update: now,
        }
    }

    pub fn outdated_tokens(&self) -> f64 {
        self.tokens as f64 + self.coining_token.get()
    }

    pub fn outdated_coined_tokens(&self) -> usize {
        self.tokens
    }

    pub fn gen_tokens(&mut self, now: Instant) -> usize {
        self.fill_up(now);

        self.tokens
    }

    pub fn set_thruput(&mut self, thruput: NonZeroPositiveF64, now: Instant) {
        self.fill_up(now);

        let sec_per_token = NonZeroPositiveF64::new(1. / thruput.get()).unwrap();
        self.sec_per_token = sec_per_token;
    }

    pub fn take_exact_tokens(&mut self, num_tokens: usize, now: Instant) -> bool {
        self.fill_up(now);

        match self.tokens.checked_sub(num_tokens) {
            Some(tokens) => {
                self.tokens = tokens;
                true
            }
            None => false,
        }
    }

    pub fn take_at_most_tokens(&mut self, num_tokens: usize, now: Instant) -> usize {
        self.fill_up(now);

        let taken_tokens = self.tokens.min(num_tokens);
        self.tokens -= taken_tokens;
        taken_tokens
    }

    pub fn next_token_time(&self) -> Instant {
        let remaining_sec = self.sec_per_token.get() * (1. - self.coining_token.get());
        self.last_update + Duration::from_secs_f64(remaining_sec)
    }

    fn fill_up(&mut self, now: Instant) {
        let dur = now.duration_since(self.last_update);
        let new_tokens = dur.as_secs_f64() / self.sec_per_token.get() + self.coining_token.get();
        let new_complete_tokens = new_tokens as usize;
        let coining_token = (new_tokens - new_complete_tokens as f64).clamp(0.0, 1.0);
        let coining_token = NormalizedF64::new(coining_token).unwrap();
        let tokens = self.tokens + new_complete_tokens;
        let tokens = tokens.min(self.max_tokens.get());
        self.last_update = now;
        self.tokens = tokens;
        self.coining_token = coining_token;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_cal() {
        let now = Instant::now();
        let thruput = NonZeroPositiveF64::new(100.).unwrap();
        let max_tokens = NonZeroUsize::new(usize::MAX).unwrap();
        let mut token_bucket = TokenBucket::new(thruput, max_tokens, now);

        let now = now + Duration::from_secs(100) + Duration::from_nanos(1);
        assert!(!token_bucket.take_exact_tokens(10_001, now));
        assert_eq!(token_bucket.tokens, 10_000);
        assert!(token_bucket.take_exact_tokens(9_999, now));
        assert_eq!(token_bucket.tokens, 1);
        assert_eq!(token_bucket.take_at_most_tokens(0, now), 0);
        assert_eq!(token_bucket.take_at_most_tokens(2, now), 1);
        assert_eq!(token_bucket.tokens, 0);
    }
}
