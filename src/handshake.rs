use std::{
    io,
    time::{Duration, Instant},
};

use rand::TryRng;

use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

const OPENING_TIMEOUT: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_millis(250);
const POST_OPEN_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(7),
    Duration::from_secs(15),
    Duration::from_secs(31),
];
const POST_OPEN_LIFETIME: Duration = Duration::from_secs(63);
const SEND_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const SEND_RETRY_BUDGET: Duration = Duration::from_millis(500);
const MAGIC: [u8; 8] = [0xf7, b'R', b'T', b'P', b'O', b'P', 1, 0];
const FEC_GUARD: u8 = 0xff;
const PACKET_LEN: usize = 18;

pub(crate) type RecoveryResponse = [u8; PACKET_LEN];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Hello = 1,
    HelloAck = 2,
    Confirm = 3,
    ConfirmAck = 4,
    Ready = 5,
}

impl Kind {
    fn decode(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Hello),
            2 => Some(Self::HelloAck),
            3 => Some(Self::Confirm),
            4 => Some(Self::ConfirmAck),
            5 => Some(Self::Ready),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Packet {
    kind: Kind,
    nonce: u64,
}

impl Packet {
    fn encode(self) -> [u8; PACKET_LEN] {
        let mut bytes = [0; PACKET_LEN];
        bytes[..MAGIC.len()].copy_from_slice(&MAGIC);
        bytes[8] = FEC_GUARD;
        bytes[9] = self.kind as u8;
        bytes[10..].copy_from_slice(&self.nonce.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PACKET_LEN || bytes[..MAGIC.len()] != MAGIC || bytes[8] != FEC_GUARD {
            return None;
        }
        Some(Self {
            kind: Kind::decode(bytes[9])?,
            nonce: u64::from_be_bytes(bytes[10..].try_into().ok()?),
        })
    }
}

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
    fn client(nonce: u64, established_at: Instant) -> Self {
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

    fn server(nonce: u64, established_at: Instant) -> Self {
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

enum Received {
    Handshake(Packet),
    NextProtocol,
    Deadline,
}

pub async fn client_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    let mut nonce_bytes = [0; std::mem::size_of::<u64>()];
    rand::rngs::SysRng
        .try_fill_bytes(&mut nonce_bytes)
        .expect("operating-system randomness unavailable");
    let nonce = u64::from_be_bytes(nonce_bytes);
    let deadline = Instant::now() + OPENING_TIMEOUT;
    client_phase(unreliable, nonce, Kind::Hello, Kind::HelloAck, deadline).await?;
    client_phase(unreliable, nonce, Kind::Confirm, Kind::ConfirmAck, deadline).await?;
    unreliable.post_open_handshake = Some(PostOpenHandshake::client(nonce, Instant::now()));
    Ok(())
}

pub async fn server_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    let deadline = Instant::now() + OPENING_TIMEOUT;
    let hello = loop {
        match receive_until(&mut unreliable.utp_read, deadline).await? {
            Received::Handshake(packet) if packet.kind == Kind::Hello => break packet,
            Received::Deadline => return Err(timeout()),
            Received::Handshake(_) | Received::NextProtocol => {}
        }
    };
    server_wait_for_confirm(unreliable, hello.nonce, deadline).await?;
    server_confirm(unreliable, hello.nonce, deadline).await?;
    unreliable.post_open_handshake = Some(PostOpenHandshake::server(hello.nonce, Instant::now()));
    Ok(())
}

async fn client_phase(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    request: Kind,
    response: Kind,
    deadline: Instant,
) -> io::Result<()> {
    let request = Packet {
        kind: request,
        nonce,
    }
    .encode();
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send(&mut unreliable.utp_write, &request, deadline).await?;
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at).await? {
                Received::Handshake(packet) if packet.nonce == nonce && packet.kind == response => {
                    return Ok(());
                }
                Received::Deadline => break,
                Received::Handshake(_) | Received::NextProtocol => {}
            }
        }
    }
}

async fn server_wait_for_confirm(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    deadline: Instant,
) -> io::Result<()> {
    let hello_ack = Packet {
        kind: Kind::HelloAck,
        nonce,
    }
    .encode();
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send(&mut unreliable.utp_write, &hello_ack, deadline).await?;
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at).await? {
                Received::Handshake(packet)
                    if packet.nonce == nonce && packet.kind == Kind::Confirm =>
                {
                    return Ok(());
                }
                Received::Handshake(packet)
                    if packet.nonce == nonce && packet.kind == Kind::Hello =>
                {
                    break;
                }
                Received::Deadline => break,
                Received::Handshake(_) | Received::NextProtocol => {}
            }
        }
    }
}

async fn server_confirm(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    deadline: Instant,
) -> io::Result<()> {
    let confirm_ack = Packet {
        kind: Kind::ConfirmAck,
        nonce,
    }
    .encode();
    send(&mut unreliable.utp_write, &confirm_ack, deadline).await
}

fn retry_at(deadline: Instant) -> Instant {
    Instant::now()
        .checked_add(RETRY_INTERVAL)
        .map(|instant| instant.min(deadline))
        .unwrap_or(deadline)
}

async fn receive_until(
    read: &mut Box<dyn UnreliableRead>,
    deadline: Instant,
) -> io::Result<Received> {
    if Instant::now() >= deadline {
        return Ok(Received::Deadline);
    }
    let mut bytes = [0; PACKET_LEN + 1];
    tokio::select! {
        result = read.recv(&mut bytes) => {
            let len = result.map_err(io::Error::from)?;
            let received = bytes.get(..len).ok_or(io::ErrorKind::InvalidData)?;
            Ok(match Packet::decode(received) {
                Some(packet) => Received::Handshake(packet),
                None => Received::NextProtocol,
            })
        }
        () = tokio::time::sleep_until(deadline.into()) => Ok(Received::Deadline),
    }
}

async fn send(
    write: &mut Box<dyn UnreliableWrite>,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    let send_deadline = deadline.min(Instant::now() + SEND_RETRY_BUDGET);
    loop {
        if Instant::now() >= send_deadline {
            return Err(timeout());
        }
        match write.send(bytes).await {
            Ok(len) if len == bytes.len() => return Ok(()),
            Ok(_) | Err(io::ErrorKind::WouldBlock) => {}
            Err(kind) => return Err(io::Error::from(kind)),
        }
        if Instant::now() >= send_deadline {
            return Err(timeout());
        }
        let retry_at = Instant::now()
            .checked_add(SEND_RETRY_INTERVAL)
            .map(|instant| instant.min(send_deadline))
            .unwrap_or(send_deadline);
        tokio::time::sleep_until(retry_at.into()).await;
    }
}

fn timeout() -> io::Error {
    io::ErrorKind::TimedOut.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packets_are_framed_and_rejected_by_both_rtp_wire_modes() {
        for kind in [
            Kind::Hello,
            Kind::HelloAck,
            Kind::Confirm,
            Kind::ConfirmAck,
            Kind::Ready,
        ] {
            let packet = Packet {
                kind,
                nonce: 0x0123_4567_89ab_cdef,
            };
            let encoded = packet.encode();
            assert_eq!(Packet::decode(&encoded), Some(packet));
            let mut overlong = encoded.to_vec();
            overlong.push(0);
            assert!(Packet::decode(&overlong).is_none());
            let mut cmd_space = encoded;
            cmd_space[0] = 0;
            assert!(Packet::decode(&cmd_space).is_none());
        }
    }

    #[test]
    fn post_open_recovery_uses_tcp_schedule_and_coalesces_missed_slots() {
        let established_at = Instant::now();
        let nonce = 0x0123_4567_89ab_cdef;
        let mut recovery = PostOpenHandshake::server(nonce, established_at);
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at + Duration::from_secs(1))
        );
        let confirm = Packet {
            kind: Kind::Confirm,
            nonce,
        }
        .encode();
        assert_eq!(
            recovery.observe(&confirm, established_at),
            Observation::ReplyQueued
        );
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at)
        );
        let response = recovery.claim_response(established_at).unwrap();
        assert_eq!(
            Packet::decode(&response.bytes),
            Some(Packet {
                kind: Kind::ConfirmAck,
                nonce
            })
        );
        let late = established_at + Duration::from_secs(20);
        assert!(recovery.claim_response(late).is_some());
        assert_eq!(
            recovery.next_send_time(late),
            Some(established_at + Duration::from_secs(31))
        );
        let final_retry = established_at + Duration::from_secs(31);
        assert!(recovery.claim_response(final_retry).is_some());
        let expired = established_at + POST_OPEN_LIFETIME;
        assert_eq!(recovery.next_send_time(final_retry), Some(expired));
        assert_eq!(recovery.observe(&confirm, expired), Observation::Filtered);
    }

    #[test]
    fn client_queues_nonce_bound_ready_after_confirmation() {
        let established_at = Instant::now();
        let nonce = 0x0123_4567_89ab_cdef;
        let mut recovery = PostOpenHandshake::client(nonce, established_at);
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at)
        );
        let response = recovery.claim_response(established_at).unwrap();
        assert_eq!(
            Packet::decode(&response.bytes),
            Some(Packet {
                kind: Kind::Ready,
                nonce
            })
        );
        assert_eq!(
            recovery.next_send_time(established_at),
            Some(established_at + POST_OPEN_LIFETIME)
        );
        let confirm_ack = Packet {
            kind: Kind::ConfirmAck,
            nonce,
        }
        .encode();
        assert_eq!(
            recovery.observe(&confirm_ack, established_at),
            Observation::ReplyQueued
        );
        assert!(recovery.claim_response(established_at).is_some());
        let mut server = PostOpenHandshake::server(nonce, established_at);
        assert_eq!(
            server.observe(&response.bytes, established_at),
            Observation::Complete
        );
    }
}
