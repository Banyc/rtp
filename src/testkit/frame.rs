// ═══════════════════════════════════════════════════════════════════════════════
// Frame‑delivery adapter
// ═══════════════════════════════════════════════════════════════════════════════

use netem_test::kit::{TestScope, TestTask, TestTaskSubmitter, submit_test_task};

pub type RtpFrameReader = crate::socket::FrameByteReader;
pub type RtpFrameDeliveryWriter = crate::socket::FrameByteWriter;

/// Connect an rtp client using frame delivery.  Returns the frame-preserving
/// reader and writer adapters that guarantee one-mux-frame-per-one-rtp-frame.
///
/// `tasks` owns the rtp session supervisor as a non-required keepalive; the
/// supervisor is awaited in the background and the test body owns teardown.
/// A normal FIN shutdown does not fail the test.
pub async fn rtp_frame_delivery_connect(
    tasks: &mut TestScope,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_with_mss_config(
        tasks,
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Default,
    )
    .await
}

/// [`rtp_frame_delivery_connect`] through the bounded task-submission handle,
/// for use inside [`TestScope::run`] bodies where `&mut TestScope` is
/// unavailable. The supervisor keepalive is submitted as a non-required
/// keepalive through the handle.
pub async fn rtp_frame_delivery_connect_via(
    tx: &TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| submit_test_task(tx, fut),
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Default,
        crate::FrameMode::enabled(),
        crate::FecTuning::default(),
        None,
    )
    .await
}

/// [`rtp_frame_delivery_connect_via`] with receiver-side fast-forward
/// enabled: the connection uses [`crate::FrameMode::enabled_reordering`], so a
/// complete frame starting past an unrepaired in-order hole is delivered
/// immediately instead of being withheld behind the hole. Both peers must use
/// the reordering mode; the frame-mode `*_reorder` server helper is the
/// matching accept side.
pub async fn rtp_frame_delivery_connect_reorder_via(
    tx: &TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| submit_test_task(tx, fut),
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Default,
        crate::FrameMode::enabled_reordering(),
        crate::FecTuning::default(),
        None,
    )
    .await
}

/// [`rtp_frame_delivery_connect_via`] with an explicit per-connection
/// [`crate::FecTuning`] and an optional metrics observer, so a frame-delivery
/// scenario can run the deployment's frame-mode-plus-FEC path and capture its
/// FEC counters. Both peers must set the same `fec` and tuning (there is no
/// in-band negotiation).
pub async fn rtp_frame_delivery_connect_with_fec_tuning_via(
    tx: &TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    fec_tuning: crate::FecTuning,
    metrics_observer: Option<crate::metrics::MetricsObserver>,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| submit_test_task(tx, fut),
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Default,
        crate::FrameMode::enabled(),
        fec_tuning,
        metrics_observer,
    )
    .await
}

/// [`rtp_frame_delivery_connect_reorder_via`] with an explicit
/// per-connection [`crate::FecTuning`] and an optional metrics observer: the
/// deployment's interactive lane (frame fast-forward **and** FEC) with both
/// peers on the same tuning.
pub async fn rtp_frame_delivery_connect_reorder_with_fec_tuning_via(
    tx: &TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    fec_tuning: crate::FecTuning,
    metrics_observer: Option<crate::metrics::MetricsObserver>,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| submit_test_task(tx, fut),
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Default,
        crate::FrameMode::enabled_reordering(),
        fec_tuning,
        metrics_observer,
    )
    .await
}

/// Connect an rtp client using frame delivery with a custom MSS.
pub async fn rtp_frame_delivery_connect_with_mss(
    tasks: &mut TestScope,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: usize,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_with_mss_config(
        tasks,
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Custom(mss),
    )
    .await
}

/// [`rtp_frame_delivery_connect_with_mss`] through the bounded
/// task-submission handle, for use inside [`TestScope::run`] bodies where
/// `&mut TestScope` is unavailable.
pub async fn rtp_frame_delivery_connect_with_mss_via(
    tx: &TestTaskSubmitter,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: usize,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| submit_test_task(tx, fut),
        proxy_client_addr,
        fec,
        crate::udp::MssConfig::Custom(mss),
        crate::FrameMode::enabled(),
        crate::FecTuning::default(),
        None,
    )
    .await
}

async fn rtp_frame_delivery_connect_with_mss_config(
    tasks: &mut TestScope,
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: crate::udp::MssConfig,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    rtp_frame_delivery_connect_core(
        |fut| tasks.spawn(fut),
        proxy_client_addr,
        fec,
        mss,
        crate::FrameMode::enabled(),
        crate::FecTuning::default(),
        None,
    )
    .await
}

/// Shared core for [`rtp_frame_delivery_connect_with_mss_config`] and the
/// `_via` variants: opens the connection and hands the supervisor keepalive to
/// `spawn` (either a [`TestScope`] spawn or the bounded reaper submission) as
/// a non-required keepalive.
async fn rtp_frame_delivery_connect_core(
    spawn: impl FnOnce(TestTask),
    proxy_client_addr: std::net::SocketAddr,
    fec: bool,
    mss: crate::udp::MssConfig,
    frame_mode: crate::FrameMode,
    fec_tuning: crate::FecTuning,
    metrics_observer: Option<crate::metrics::MetricsObserver>,
) -> (RtpFrameReader, RtpFrameDeliveryWriter) {
    let connected = crate::udp::FrameDeliveryIo::connect(
        "0.0.0.0:0",
        &proxy_client_addr.to_string(),
        crate::udp::ConnectConfig {
            handshake: false,
            fec,
            mss,
            frame_delivery: frame_mode,
            fec_tuning,
            metrics_observer,
            ..crate::udp::ConnectConfig::default()
        },
    )
    .await
    .unwrap();
    // Hold the rtp session owner for the connection's lifetime; the supervisor
    // is polled as a non-required keepalive so a normal FIN shutdown does not
    // fail the test. An early session end still surfaces through the
    // higher-level supervision (mux session / pump write errors).
    spawn(Box::pin(async move {
        let _ = connected.supervisor.await;
    }));
    (connected.read, connected.write)
}
