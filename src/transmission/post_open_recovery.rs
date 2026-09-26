use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

use crate::traffic_shaping::control::handshake::{DueResponse, PostOpenHandshake, PostOpenVerdict};

/// Opening-handshake recovery state owned by the shared `Connection`.  The
/// read driver feeds datagrams via `observe`; the write driver claims due
/// responses and retries blocked sends; the session queries the next send
/// time for the send-wake computation.  All operations are synchronous
/// complete-lock calls.
#[derive(Debug)]
pub(super) struct PostOpenRecovery {
    handshake: Option<Mutex<PostOpenHandshake>>,
    active: AtomicBool,
}

impl PostOpenRecovery {
    pub(super) fn new(handshake: Option<PostOpenHandshake>) -> Self {
        let active = handshake.is_some();
        Self {
            handshake: handshake.map(Mutex::new),
            active: AtomicBool::new(active),
        }
    }

    pub(super) fn observe(&self, datagram: &[u8], now: Instant) -> PostOpenVerdict {
        if !self.active.load(Ordering::Acquire) {
            return PostOpenVerdict::NotHandshake;
        }
        let Some(handshake) = &self.handshake else {
            return PostOpenVerdict::NotHandshake;
        };
        let mut handshake = handshake.lock().unwrap();
        let observation = handshake.observe(datagram, now);
        if observation == PostOpenVerdict::Complete || handshake.expired(now) {
            self.active.store(false, Ordering::Release);
        }
        observation
    }

    pub(super) fn claim_response(&self, now: Instant) -> Option<DueResponse> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        let handshake = self.handshake.as_ref()?;
        let mut handshake = handshake.lock().unwrap();
        let response = handshake.take_due_response(now);
        if handshake.expired(now) {
            self.active.store(false, Ordering::Release);
        }
        response
    }

    pub(super) fn retry_response(&self, now: Instant) {
        if self.active.load(Ordering::Acquire)
            && let Some(handshake) = &self.handshake
        {
            handshake.lock().unwrap().retry_response(now);
        }
    }

    pub(super) fn next_send_time(&self, now: Instant) -> Option<Instant> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        let handshake = self.handshake.as_ref()?;
        let handshake = handshake.lock().unwrap();
        if handshake.expired(now) {
            self.active.store(false, Ordering::Release);
            None
        } else {
            handshake.next_send_time(now)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic_shaping::control::handshake::post_open::retry_delay;
    use crate::traffic_shaping::control::handshake::wire::{Kind, Packet};

    /// A lost `Ready` is answered by the server's +1 s retransmission, and the
    /// client's retried `Ready` then retires the recovery: no later slot may
    /// send. The schedule and the retirement are both `Instant`-based, and the
    /// per-nonce jitter places the +3 s slot in `[3.0, 3.5)` s, so no fixed
    /// sleep in an integration test observes the retirement deterministically.
    /// Driving `now` by argument covers it exactly, at no wall clock.
    #[test]
    fn retried_ready_retires_the_scheduled_retransmission_chain() {
        let nonce = 0x0123_4567_89ab_cdef;
        let t0 = Instant::now();
        let recovery = PostOpenRecovery::new(Some(PostOpenHandshake::server(nonce, t0)));
        let ready = Packet {
            kind: Kind::Ready,
            nonce,
        }
        .encode();

        let slot0 = t0 + retry_delay(nonce, 0);
        assert_eq!(recovery.next_send_time(t0), Some(slot0));
        assert!(
            recovery.claim_response(slot0).is_some(),
            "the +1 s slot must fire when the client's Ready is lost"
        );

        assert_eq!(recovery.observe(&ready, slot0), PostOpenVerdict::Complete);
        let slot1 = t0 + retry_delay(nonce, 1);
        assert!(
            slot1 > slot0,
            "the +3 s slot is scheduled after the +1 s one"
        );
        assert_eq!(
            recovery.next_send_time(slot1),
            None,
            "a retired recovery must expose no further send time"
        );
        assert!(
            recovery.claim_response(slot1).is_none(),
            "the +3 s slot must not send after the retried Ready retires recovery"
        );
    }
}
