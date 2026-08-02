#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};

use crate::io_err::IoErr;

#[cfg(unix)]
pub type MaybeRawFd = RawFd;
#[cfg(not(unix))]
pub type MaybeRawFd = ();
#[cfg(unix)]
pub fn maybe_raw_fd(udp: &impl AsRawFd) -> MaybeRawFd {
    udp.as_raw_fd()
}
#[cfg(not(unix))]
pub fn maybe_raw_fd<T>(_udp: &T) -> MaybeRawFd {
    ()
}

/// Returns `true` if `code` is the raw OS errno for `ENOBUFS` on the current
/// platform (macOS `55`, Linux `105`).
fn is_enobufs_raw_os_error(code: i32) -> bool {
    cfg_select! {
        target_os = "macos" => code == 55,
        target_os = "linux" => code == 105,
        _ => false,
    }
}

/// Normalize transient UDP send-buffer exhaustion (ENOBUFS / ENOBUFS-equivalent
/// OS errors) to [`std::io::ErrorKind::WouldBlock`].
///
/// UDP has no flow control: when the kernel send buffer is full the OS
/// reports a transient error (macOS errno 55 `ENOBUFS`, Linux errno 105
/// `ENOBUFS`). These are not fatal — the packet is simply dropped and the
/// caller should treat it as transient backpressure (equivalent to a loss
/// event). Reliability is provided above this layer by the reliable layer's
/// retransmit logic, so dropping an outgoing packet here is recoverable.
///
/// All other errors are passed through unchanged.
pub(crate) fn normalize_send_err(e: std::io::Error) -> IoErr {
    let err = IoErr::from(e);
    match err.raw_os_error().is_some_and(is_enobufs_raw_os_error) {
        true => err.with_kind(std::io::ErrorKind::WouldBlock),
        false => err,
    }
}

/// Decide whether a failed [`crate::transmission::transmission_layer::UnreliableWrite::send`]
/// should fall back to the async `send` path (waiting for the socket to become
/// writable), or to the raw-fd fallback ([`raw_sendto_fallback`]).
///
/// We only want to wait when the error is a genuine "would block" from a
/// non-blocking socket that is not currently writable. If the kernel
/// reported `ENOBUFS` (transient send-buffer exhaustion) we must *not*
/// wait — the socket is writable, the packet was simply dropped, and
/// blocking here would spin on a ready-but-lossy path. In that case the
/// error is normalized to [`std::io::ErrorKind::WouldBlock`] by
/// [`normalize_send_err`] and surfaced to the caller as a loss event.
pub(crate) fn should_wait_after_try_send(e: &std::io::Error) -> bool {
    if e.kind() != std::io::ErrorKind::WouldBlock {
        return false;
    }
    match e.raw_os_error() {
        Some(code) => !is_enobufs_raw_os_error(code),
        None => true,
    }
}

/// On macOS, kqueue EVFILT_WRITE tracks only socket sndbuf, not mbuf/
/// interface-queue pressure — so tokio UDP writability readiness is
/// *poisoned* under interface backpressure: it reports writable, the send
/// returns EAGAIN, and a readiness-await parks forever.  Fall back to
/// bounded raw `send` / `sendto` retries on the socket's raw fd instead
/// of ever awaiting writability.  Interrupted/EINTR is retried (without
/// consuming a retry) and each retry backs off with increasing delay so
/// the send buffer has time to drain.
///
/// When `peer` is `Some`, the socket is unconnected and `send_to` is used
/// to address the peer directly.  When `None`, the socket is connected
#[cfg(unix)]
unsafe fn borrowed_udp_socket(raw_fd: MaybeRawFd) -> std::mem::ManuallyDrop<std::net::UdpSocket> {
    use std::os::fd::FromRawFd;
    std::mem::ManuallyDrop::new(unsafe { std::net::UdpSocket::from_raw_fd(raw_fd) })
}

/// and a plain `send` suffices.
///
/// The raw fd is borrowed via `std::net::UdpSocket::from_raw_fd` so the
/// OS handles the sockaddr encoding — this avoids both the byte-order bug
/// of hand-rolled `sockaddr_in` (`from_be_bytes` stores 127.0.0.1 as
/// memory [1,0,0,127] on little-endian) and the Linux build break from
/// the BSD-only `sin_len`/`sin6_len` fields.  The borrowed socket is
/// `mem::forget`ten so the fd is never closed.
///
/// Returns `Err(WouldBlock)` when retry budget is exhausted — the caller
/// must retry later, not treat the packet as sent.
pub(crate) async fn raw_sendto_fallback(
    raw_fd: MaybeRawFd,
    buf: &[u8],
    peer: Option<core::net::SocketAddr>,
) -> Result<usize, IoErr> {
    const BACKOFFS_US: [u64; 5] = [1_000, 2_000, 4_000, 8_000, 16_000];
    #[cfg(not(unix))]
    {
        let _ = (raw_fd, buf, peer);
        return Err(std::io::ErrorKind::Unsupported.into());
    }
    #[cfg(unix)]
    {
        let socket = unsafe { borrowed_udp_socket(raw_fd) };
        let mut attempt = 0;
        loop {
            let res = match &peer {
                Some(peer) => socket.send_to(buf, peer),
                None => socket.send(buf),
            };
            match res {
                Ok(n) => {
                    return Ok(n);
                }
                Err(err) => {
                    let kind = err.kind();
                    match kind {
                        std::io::ErrorKind::Interrupted => continue,
                        std::io::ErrorKind::WouldBlock => {
                            if attempt >= BACKOFFS_US.len() {
                                return Err(std::io::ErrorKind::WouldBlock.into());
                            }
                            tokio::time::sleep(std::time::Duration::from_micros(
                                BACKOFFS_US[attempt],
                            ))
                            .await;
                            attempt += 1;
                        }
                        _ => {
                            return Err(normalize_send_err(err));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_wait_after_plain_wouldblock() {
        // A WouldBlock with no underlying raw OS error (e.g. synthesized by a
        // non-blocking mpsc channel) should fall back to the async wait path.
        let e = std::io::Error::from(std::io::ErrorKind::WouldBlock);
        assert!(should_wait_after_try_send(&e));
    }

    #[test]
    fn should_not_wait_after_enobufs() {
        #[cfg(target_os = "macos")]
        let code = 55;
        #[cfg(target_os = "linux")]
        let code = 105;
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let e = std::io::Error::from(std::io::ErrorKind::WouldBlock);
            assert!(should_wait_after_try_send(&e));
            return;
        }
        let e = std::io::Error::from_raw_os_error(code);
        assert!(!should_wait_after_try_send(&e));
        let normalized = normalize_send_err(e);
        assert_eq!(normalized.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(normalized.raw_os_error(), Some(code));
        assert!(
            normalized.to_string().contains(&format!("os error {code}")),
            "a normalized ENOBUFS must still name itself, got {normalized}"
        );
    }

    /// Fix #1: `raw_fallback_sends_to_peer_on_unconnected_socket` — bind an
    /// unconnected `UdpSocket`, send via the raw fallback with
    /// `Some(127.0.0.1:port)`, and assert the datagram arrives at that peer.
    /// Before the fix, `u32::from_be_bytes(octets)` stored 127.0.0.1 as memory
    /// `[1,0,0,127]` on little-endian (macOS), so the datagram went to the
    /// wrong address and never arrived.
    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raw_fallback_sends_to_peer_on_unconnected_socket() {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();

        // Bind an unconnected socket so `peer` is `Some`.
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.set_nonblocking(true).unwrap();
        let raw_fd = std::os::fd::AsRawFd::as_raw_fd(&sender);

        let payload = b"raw-fallback-test";
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            raw_sendto_fallback(raw_fd, payload, Some(peer_addr)),
        )
        .await;

        match result {
            Ok(Ok(n)) => assert_eq!(n, payload.len()),
            Ok(Err(e)) => panic!("raw_sendto_fallback failed: {e:?}"),
            Err(_) => panic!("raw_sendto_fallback hung"),
        }

        let mut buf = [0u8; 32];
        let (n, from) =
            tokio::time::timeout(std::time::Duration::from_secs(1), peer.recv_from(&mut buf))
                .await
                .expect("peer recv timed out")
                .expect("peer recv failed");
        assert_eq!(&buf[..n], payload);
        assert_eq!(
            from.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn dropping_borrowed_socket_never_closes_original_fd() {
        let original = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw_fd = std::os::fd::AsRawFd::as_raw_fd(&original);
        {
            let _socket = unsafe { borrowed_udp_socket(raw_fd) };
        }
        original
            .send_to(b"alive", original.local_addr().unwrap())
            .unwrap();
    }
}
