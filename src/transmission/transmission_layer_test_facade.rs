use std::sync::Arc;
use std::time::Instant;

use super::read_half::ReadHalf;
use super::shared::Shared;
use super::shared::{build_parts, build_parts_with_watchdog_tuning};
use super::transmission_layer::{
    LogConfig, RecvBufs, RecvPkts, SendBufs, SendKillPkt, UnreliableLayer,
};
use super::write_half::WriteHalf;

#[cfg(test)]
pub struct TransmissionLayer {
    pub(crate) shared: Arc<Shared>,
    pub(crate) write_half: tokio::sync::Mutex<WriteHalf>,
    pub(crate) read_half: tokio::sync::Mutex<ReadHalf>,
}

#[cfg(test)]
impl std::ops::Deref for TransmissionLayer {
    type Target = Shared;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[cfg(test)]
impl TransmissionLayer {
    pub fn new(unreliable_layer: UnreliableLayer, log_config: Option<LogConfig>) -> Self {
        let (shared, write_half, read_half, _reaper) = build_parts(unreliable_layer, log_config);
        Self {
            shared,
            write_half: tokio::sync::Mutex::new(write_half),
            read_half: tokio::sync::Mutex::new(read_half),
        }
    }

    pub fn new_with_watchdog_tuning(
        unreliable_layer: UnreliableLayer,
        tuning: crate::transmission::watchdog_tuning::WatchdogTuning,
    ) -> Self {
        let (shared, write_half, read_half, _reaper) =
            build_parts_with_watchdog_tuning(unreliable_layer, None, tuning);
        Self {
            shared,
            write_half: tokio::sync::Mutex::new(write_half),
            read_half: tokio::sync::Mutex::new(read_half),
        }
    }

    /// Test-only: force the `RTP_RTX_DUP` toggle to a fixed value regardless
    /// of the process environment, so parallel tests in the same binary do not
    /// race on the env var.
    pub(crate) fn set_rtx_dup_for_test(&mut self, enabled: bool) {
        self.shared
            .rtx_dup
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: force the `RTP_INSTREAM_GROUP_FEC` toggle to a fixed value
    /// regardless of the process environment, so parallel tests in the same
    /// binary do not race on the env var.
    pub(crate) fn set_instream_group_fec_for_test(&mut self, enabled: bool) {
        self.shared
            .instream_group_fec_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: take up to `n` tokens from the shared send-rate limiter so
    /// the retransmission-armor duplicate-copy token gate can be exercised
    /// (the primary rtx bypasses the bucket; the dup needs a token).
    pub(crate) fn drain_rate_limiter_for_test(&self, n: usize, now: Instant) -> usize {
        self.shared
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_at_most_tokens(n, now)
    }

    pub async fn send_pkts(&self, bufs: &mut SendBufs) -> Result<bool, std::io::ErrorKind> {
        self.write_half.lock().await.send_pkts(bufs).await
    }

    pub async fn flush_acks(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        self.write_half.lock().await.flush_acks(bufs).await
    }

    pub fn has_pending_acks(&self) -> bool {
        self.write_half.blocking_lock().has_pending_acks()
    }

    pub async fn send_kill_pkt(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        self.write_half.lock().await.send_kill_pkt(bufs).await
    }

    pub async fn send_kill_and_abort(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        self.write_half.lock().await.send_kill_and_abort(bufs).await
    }

    pub async fn recv_pkts(
        &self,
        bufs: &mut RecvBufs,
    ) -> Result<RecvPkts, (std::io::ErrorKind, SendKillPkt)> {
        self.read_half.lock().await.recv_pkts(bufs).await
    }
}
