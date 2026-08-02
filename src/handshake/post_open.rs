use std::time::{Duration, Instant};

use super::wire::{
    FEC_GUARD, Kind, MAGIC, PACKET_LEN, Packet, RecoveryResponse, SEND_RETRY_INTERVAL,
};

const POST_OPEN_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(7),
    Duration::from_secs(15),
    Duration::from_secs(31),
];
pub(crate) const POST_OPEN_LIFETIME: Duration = Duration::from_secs(63);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostOpenRole {
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Observation {
    NotHandshake,
    Filtered,
    ReplyQueued,
    Complete,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClaimedResponse {
    pub(crate) bytes: RecoveryResponse,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct PostOpenHandshake {
    role: PostOpenRole,
    nonce: u64,
    confirmation: RecoveryResponse,
    established_at: Instant,
    expires_at: Instant,
    retry_index: usize,
    pending_at: Option<Instant>,
}

impl PostOpenHandshake {
    pub(crate) fn client(nonce: u64, established_at: Instant) -> Self {
        Self {
            role: PostOpenRole::Client,
            nonce,
            confirmation: Packet {
                kind: Kind::Ready,
                nonce,
            }
            .encode(),
            established_at,
            expires_at: established_at + POST_OPEN_LIFETIME,
            retry_index: POST_OPEN_RETRY_DELAYS.len(),
            pending_at: Some(established_at),
        }
    }

    pub(crate) fn server(nonce: u64, established_at: Instant) -> Self {
        Self {
            role: PostOpenRole::Server,
            nonce,
            confirmation: Packet {
                kind: Kind::ConfirmAck,
                nonce,
            }
            .encode(),
            established_at,
            expires_at: established_at + POST_OPEN_LIFETIME,
            retry_index: 0,
            pending_at: None,
        }
    }

    pub(crate) fn observe(&mut self, datagram: &[u8], now: Instant) -> Observation {
        let Some(packet) = Packet::decode(datagram) else {
            return Observation::NotHandshake;
        };
        if packet.nonce != self.nonce || self.expires_at <= now {
            return Observation::Filtered;
        }
        if self.role == PostOpenRole::Server && packet.kind == Kind::Ready {
            self.pending_at = None;
            return Observation::Complete;
        }
        let should_reply = match self.role {
            PostOpenRole::Client => packet.kind == Kind::ConfirmAck,
            PostOpenRole::Server => packet.kind == Kind::Confirm,
        };
        if !should_reply {
            return Observation::Filtered;
        }
        self.pending_at = Some(self.pending_at.map_or(now, |pending| pending.min(now)));
        Observation::ReplyQueued
    }

    pub(crate) fn next_send_time(&self, now: Instant) -> Option<Instant> {
        if self.expires_at <= now {
            return None;
        }
        let scheduled = (self.role == PostOpenRole::Server)
            .then(|| {
                POST_OPEN_RETRY_DELAYS
                    .get(self.retry_index)
                    .map(|delay| self.established_at + *delay)
            })
            .flatten();
        match (self.pending_at, scheduled) {
            (Some(pending), Some(scheduled)) => Some(pending.min(scheduled)),
            (Some(pending), None) => Some(pending),
            (None, Some(scheduled)) => Some(scheduled),
            (None, None) => Some(self.expires_at),
        }
    }

    pub(crate) fn claim_response(&mut self, now: Instant) -> Option<ClaimedResponse> {
        if self.expires_at <= now {
            self.pending_at = None;
            return None;
        }
        let pending_due = self.pending_at.is_some_and(|pending| pending <= now);
        let mut scheduled_due = false;
        while self.role == PostOpenRole::Server
            && let Some(delay) = POST_OPEN_RETRY_DELAYS.get(self.retry_index)
        {
            if self.established_at + *delay > now {
                break;
            }
            self.retry_index += 1;
            scheduled_due = true;
        }
        if !pending_due && !scheduled_due {
            return None;
        }
        if pending_due {
            self.pending_at = None;
        }
        Some(ClaimedResponse {
            bytes: self.confirmation,
        })
    }

    pub(crate) fn retry_response(&mut self, now: Instant) {
        let retry_at = now + SEND_RETRY_INTERVAL;
        self.pending_at = Some(
            self.pending_at
                .map_or(retry_at, |pending| pending.min(retry_at)),
        );
    }

    pub(crate) fn expired(&self, now: Instant) -> bool {
        self.expires_at <= now
    }
}

pub(crate) fn is_post_open_candidate(datagram: &[u8]) -> bool {
    datagram.len() == PACKET_LEN && datagram[..MAGIC.len()] == MAGIC && datagram[8] == FEC_GUARD
}
