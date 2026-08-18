/// Cross-actor coordination signals owned by the shared `Connection`.  Every
/// field is private; callers use the immutable accessor methods so the
/// notification handles cannot be replaced or aliased away.
#[derive(Debug)]
pub(super) struct Signals {
    sent_data_pkt: tokio::sync::Notify,
    recv_data_pkt: tokio::sync::Notify,
    sent_pkt_acked: tokio::sync::Notify,
    session_outbound_progress: tokio::sync::Notify,
    resume_send: tokio::sync::Notify,
    recv_fin: tokio_util::sync::CancellationToken,
    recv_eof: tokio_util::sync::CancellationToken,
}

impl Signals {
    pub(super) fn new() -> Self {
        Self {
            sent_data_pkt: tokio::sync::Notify::new(),
            recv_data_pkt: tokio::sync::Notify::new(),
            sent_pkt_acked: tokio::sync::Notify::new(),
            session_outbound_progress: tokio::sync::Notify::new(),
            resume_send: tokio::sync::Notify::new(),
            recv_fin: tokio_util::sync::CancellationToken::new(),
            recv_eof: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Woken whenever the send stage moved (a datagram was sent or FIN
    /// written), i.e. the send buffer may now have capacity.
    pub(super) fn sent_data_pkt(&self) -> &tokio::sync::Notify {
        &self.sent_data_pkt
    }

    /// Woken whenever a payload was delivered to the receive buffer.
    pub(super) fn recv_data_pkt(&self) -> &tokio::sync::Notify {
        &self.recv_data_pkt
    }

    /// Woken whenever an ACK from the peer freed send-window capacity.
    pub(super) fn sent_pkt_acked(&self) -> &tokio::sync::Notify {
        &self.sent_pkt_acked
    }

    /// Woken whenever outbound progress happened (data sent, ACK flushed, or
    /// peer ACK processed); used by the graceful-close drain wait.
    pub(super) fn session_outbound_progress(&self) -> &tokio::sync::Notify {
        &self.session_outbound_progress
    }

    /// Woken to ask the send driver to run another pass.
    pub(super) fn resume_send(&self) -> &tokio::sync::Notify {
        &self.resume_send
    }

    /// Cancelled when the peer's FIN was accepted into the receive buffer.
    pub(super) fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.recv_fin
    }

    /// Cancelled when every received payload has been delivered to the
    /// application (receive EOF).
    pub(super) fn recv_eof(&self) -> &tokio_util::sync::CancellationToken {
        &self.recv_eof
    }
}
