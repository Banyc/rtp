use std::io;
use std::mem::size_of;
use std::time::{Duration, Instant};

use rand::TryRng;

use super::padding::pad_handshake;
use super::post_open::PostOpenHandshake;
use super::wire::{Kind, Packet, SEND_RETRY_INTERVAL};
use crate::sequence::{InitialSequences, SequenceNumber};
use crate::transmission::transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite};

const OPENING_TIMEOUT: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_millis(250);
const SEND_RETRY_BUDGET: Duration = Duration::from_millis(500);
/// Random pre-handshake delay so connection opens do not all start with an
/// immediate burst.
const OPENING_JITTER_MS: u64 = 50;
/// Jitter on the handshake retry interval so retransmission timing does not
/// fingerprint.
const RETRY_JITTER_MS: u64 = 50;
const SEND_RETRY_JITTER_MS: u64 = 25;

/// Domain-separated splitmix64 finalizer over the handshake nonce.  Each
/// derivation domain yields an independent value from the same nonce.
fn mix(nonce: u64, domain: u64) -> u64 {
    let mut z = nonce ^ domain;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Domain for the per-connection session tag that authenticates codec
/// control-plane datagrams after open.  Derived from the handshake nonce
/// (splitmix64 finalizer, domain-separated) rather than used raw, so
/// data-plane traffic does not reveal the recovery-handshake nonce.  Both
/// peers compute the same value from the same nonce; an off-path attacker
/// never sees the nonce and therefore cannot forge a tag.
///
/// **Limitation:** the tag is only as secret as the nonce.  When datagram
/// obfuscation is disabled the handshake nonce travels in clear, so any
/// party that can observe the handshake (a passive on-path observer) can
/// derive the tag and forge `KILL`/`ACK`/`ECHO_TS` commands from a spoofed
/// source address.  The tag's value is blocking blind off-path injection
/// without sniffing; the obfuscation key is the real secret.
const SESSION_TAG_DOMAIN: u64 = 0x9e37_79b9_7f4a_7c15;
/// Domain for the client-to-server directional sequence start (`rtp-c2s!`).
const CLIENT_TO_SERVER_DOMAIN: u64 = 0x7274_702d_6332_7321;
/// Domain for the server-to-client directional sequence start (`rtp-s2c!`).
const SERVER_TO_CLIENT_DOMAIN: u64 = 0x7274_702d_7332_6321;

fn session_tag(nonce: u64) -> u64 {
    mix(nonce, SESSION_TAG_DOMAIN)
}

/// Directional initial sequences derived from the handshake nonce: the
/// client sends `client_to_server` and receives `server_to_client`; the
/// server's view is role-inverted.
fn directional_initial_sequences(nonce: u64) -> (SequenceNumber, SequenceNumber) {
    let client_to_server = SequenceNumber::from_wire(mix(nonce, CLIENT_TO_SERVER_DOMAIN));
    let server_to_client = SequenceNumber::from_wire(mix(nonce, SERVER_TO_CLIENT_DOMAIN));
    (client_to_server, server_to_client)
}

enum Received {
    Handshake(Packet),
    NextProtocol,
    Deadline,
}

pub async fn client_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    // Jitter the opening so connection opens do not all start with an
    // immediate burst.
    tokio::time::sleep(Duration::from_millis(rand::random_range(
        0..=OPENING_JITTER_MS,
    )))
    .await;
    let mut nonce_bytes = [0; size_of::<u64>()];
    rand::rngs::SysRng
        .try_fill_bytes(&mut nonce_bytes)
        .expect("operating-system randomness unavailable");
    let nonce = u64::from_be_bytes(nonce_bytes);
    let deadline = Instant::now() + OPENING_TIMEOUT;
    let mss = unreliable.mss;
    client_phase(
        unreliable,
        nonce,
        Kind::Hello,
        Kind::HelloAck,
        deadline,
        mss,
    )
    .await?;
    // Confirm→ConfirmAck is the final unambiguous leg that directly precedes
    // RTP traffic; the Hello→HelloAck leg is deliberately not sampled.
    let initial_rtt = client_phase(
        unreliable,
        nonce,
        Kind::Confirm,
        Kind::ConfirmAck,
        deadline,
        mss,
    )
    .await?;
    unreliable.initial_rtt = initial_rtt;
    unreliable.post_open_handshake = Some(PostOpenHandshake::client(nonce, Instant::now()));
    unreliable.session_tag = Some(session_tag(nonce));
    let (client_to_server, server_to_client) = directional_initial_sequences(nonce);
    unreliable.initial_sequences = InitialSequences::client(client_to_server, server_to_client);
    Ok(())
}

pub async fn server_opening_handshake(unreliable: &mut UnreliableLayer) -> io::Result<()> {
    let deadline = Instant::now() + OPENING_TIMEOUT;
    let mss = unreliable.mss;
    let hello = loop {
        match receive_until(&mut unreliable.utp_read, deadline, mss).await? {
            Received::Handshake(packet) if packet.kind == Kind::Hello => break packet,
            Received::Deadline => return Err(timeout()),
            Received::Handshake(_) | Received::NextProtocol => {}
        }
    };
    let initial_rtt = server_wait_for_confirm(unreliable, hello.nonce, deadline, mss).await?;
    unreliable.initial_rtt = initial_rtt;
    server_confirm(unreliable, hello.nonce, deadline, mss).await?;
    unreliable.post_open_handshake = Some(PostOpenHandshake::server(hello.nonce, Instant::now()));
    unreliable.session_tag = Some(session_tag(hello.nonce));
    let (client_to_server, server_to_client) = directional_initial_sequences(hello.nonce);
    unreliable.initial_sequences = InitialSequences::server(client_to_server, server_to_client);
    Ok(())
}

async fn client_phase(
    unreliable: &mut UnreliableLayer,
    nonce: u64,
    request: Kind,
    response: Kind,
    deadline: Instant,
    mss: crate::mss::Mss,
) -> io::Result<Option<Duration>> {
    let request = Packet {
        kind: request,
        nonce,
    }
    .encode();
    let mut attempts = 0usize;
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send_padded(&mut unreliable.utp_write, &request, deadline, mss).await?;
        attempts += 1;
        let sent_at = Instant::now();
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at, mss).await? {
                Received::Handshake(packet) if packet.nonce == nonce && packet.kind == response => {
                    // A sample is valid only when the request succeeded on its
                    // first transmission; after a retry the response is
                    // ambiguous, so report `None`.
                    return Ok((attempts == 1).then(|| Instant::now().duration_since(sent_at)));
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
    mss: crate::mss::Mss,
) -> io::Result<Option<Duration>> {
    let hello_ack = Packet {
        kind: Kind::HelloAck,
        nonce,
    }
    .encode();
    let mut attempts = 0usize;
    loop {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        send_padded(&mut unreliable.utp_write, &hello_ack, deadline, mss).await?;
        attempts += 1;
        let sent_at = Instant::now();
        let retry_at = retry_at(deadline);
        loop {
            match receive_until(&mut unreliable.utp_read, retry_at, mss).await? {
                Received::Handshake(packet)
                    if packet.nonce == nonce && packet.kind == Kind::Confirm =>
                {
                    // A sample is valid only when the HelloAck succeeded on
                    // its first transmission; after a retry the Confirm is
                    // ambiguous, so report `None`.
                    return Ok((attempts == 1).then(|| Instant::now().duration_since(sent_at)));
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
    mss: crate::mss::Mss,
) -> io::Result<()> {
    let confirm_ack = Packet {
        kind: Kind::ConfirmAck,
        nonce,
    }
    .encode();
    send_padded(&mut unreliable.utp_write, &confirm_ack, deadline, mss).await
}

fn retry_at(deadline: Instant) -> Instant {
    let jitter = Duration::from_millis(rand::random_range(0..=RETRY_JITTER_MS));
    Instant::now()
        .checked_add(RETRY_INTERVAL + jitter)
        .map(|instant| instant.min(deadline))
        .unwrap_or(deadline)
}

async fn receive_until(
    read: &mut Box<dyn UnreliableRead>,
    deadline: Instant,
    mss: crate::mss::Mss,
) -> io::Result<Received> {
    if Instant::now() >= deadline {
        return Ok(Received::Deadline);
    }
    // Sized to the connection's MSS-derived maximum padded handshake packet
    // so a peer padding up to its own MSS is never truncated or dropped.
    let mut bytes = vec![0u8; mss.get()];
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
            Ok(_) => {}
            Err(error) if error == io::ErrorKind::WouldBlock => {}
            Err(kind) => return Err(io::Error::from(kind)),
        }
        if Instant::now() >= send_deadline {
            return Err(timeout());
        }
        let retry_at = Instant::now()
            .checked_add(
                SEND_RETRY_INTERVAL
                    + Duration::from_millis(rand::random_range(0..=SEND_RETRY_JITTER_MS)),
            )
            .map(|instant| instant.min(send_deadline))
            .unwrap_or(send_deadline);
        tokio::time::sleep_until(retry_at.into()).await;
    }
}

/// Send a handshake packet with a random padding tail (see
/// [`pad_handshake`]), retrying on WouldBlock like [`send`]. The peer strips
/// the tail in `receive_until` before decoding. `mss` is the connection's
/// MSS; the padding bound is derived from it in the padding module.
async fn send_padded(
    write: &mut Box<dyn UnreliableWrite>,
    core: &[u8],
    deadline: Instant,
    mss: crate::mss::Mss,
) -> io::Result<()> {
    let mut padded = vec![0u8; mss.get()];
    let n = pad_handshake(core, &mut padded, mss);
    send(write, &padded[..n], deadline).await
}

fn timeout() -> io::Error {
    io::ErrorKind::TimedOut.into()
}

#[cfg(test)]
mod tests;
