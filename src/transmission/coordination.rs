#[derive(Debug)]
pub(crate) struct Coordination {
    pub(crate) sent_data_pkt: tokio::sync::Notify,
    pub(crate) recv_data_pkt: tokio::sync::Notify,
    pub(crate) sent_pkt_acked: tokio::sync::Notify,
    pub(crate) session_outbound_progress: tokio::sync::Notify,
    pub(crate) resume_send: tokio::sync::Notify,
    pub(crate) recv_fin: tokio_util::sync::CancellationToken,
    pub(crate) recv_eof: tokio_util::sync::CancellationToken,
}

impl Coordination {
    pub(crate) fn new() -> Self {
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
}
