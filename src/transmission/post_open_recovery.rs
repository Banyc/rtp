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
