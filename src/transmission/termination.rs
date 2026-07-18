use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::transmission_layer::ProactiveTerminationContext;

const GRACEFUL_CLOSE_TIMEOUT: Duration = Duration::from_secs(675);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerReset {
    NoKill,
    SendKill,
}

#[derive(Debug, Clone)]
struct FirstErrorValue {
    kind: std::io::ErrorKind,
    context: Option<ProactiveTerminationContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillState {
    NotRequested,
    Requested,
    InProgress,
    Finished,
}

#[derive(Debug)]
struct State {
    first_error: Option<FirstErrorValue>,
    kill: KillState,
    writer_alive: bool,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    terminal: CancellationToken,
    kill_requested: CancellationToken,
    kill_finished: CancellationToken,
}

#[derive(Debug, Clone)]
pub(crate) struct TerminationPresser {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) struct TerminationWriter {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone)]
pub(crate) struct TerminationReaper {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) struct KillAttempt {
    inner: Arc<Inner>,
}

pub(crate) fn channel() -> (TerminationPresser, TerminationWriter, TerminationReaper) {
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            first_error: None,
            kill: KillState::NotRequested,
            writer_alive: true,
        }),
        terminal: CancellationToken::new(),
        kill_requested: CancellationToken::new(),
        kill_finished: CancellationToken::new(),
    });
    (
        TerminationPresser {
            inner: Arc::clone(&inner),
        },
        TerminationWriter {
            inner: Arc::clone(&inner),
        },
        TerminationReaper { inner },
    )
}

impl TerminationPresser {
    pub(crate) fn press_error(&self, kind: std::io::ErrorKind) -> bool {
        self.press(kind, None, PeerReset::NoKill)
    }
    pub(crate) fn press_broken_pipe(
        &self,
        peer_reset: PeerReset,
        context: Option<ProactiveTerminationContext>,
    ) -> bool {
        self.press(std::io::ErrorKind::BrokenPipe, context, peer_reset)
    }
    fn press(
        &self,
        kind: std::io::ErrorKind,
        context: Option<ProactiveTerminationContext>,
        peer_reset: PeerReset,
    ) -> bool {
        let mut request_kill = false;
        let mut finish_kill = false;
        let inserted = {
            let mut state = self.inner.state.lock().unwrap();
            if state.first_error.is_some() {
                false
            } else {
                state.first_error = Some(FirstErrorValue { kind, context });
                if peer_reset == PeerReset::SendKill {
                    if state.writer_alive {
                        state.kill = KillState::Requested;
                        request_kill = true;
                    } else {
                        state.kill = KillState::Finished;
                        finish_kill = true;
                    }
                }
                true
            }
        };
        if request_kill {
            self.inner.kill_requested.cancel();
        }
        if finish_kill {
            self.inner.kill_finished.cancel();
        }
        self.inner.terminal.cancel();
        inserted
    }
    pub(crate) fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        match &self.inner.state.lock().unwrap().first_error {
            Some(error) => Err(error.kind),
            None => Ok(()),
        }
    }
    pub(crate) fn has_error(&self) -> bool {
        self.inner.state.lock().unwrap().first_error.is_some()
    }
    pub(crate) fn io_error(&self, kind: std::io::ErrorKind) -> std::io::Error {
        let context = self
            .inner
            .state
            .lock()
            .unwrap()
            .first_error
            .as_ref()
            .filter(|error| error.kind == kind)
            .and_then(|error| error.context.clone());
        match context {
            Some(context) => std::io::Error::new(kind, context),
            None => std::io::Error::from(kind),
        }
    }
    pub(crate) fn terminal(&self) -> &CancellationToken {
        &self.inner.terminal
    }
}

impl TerminationWriter {
    pub(crate) fn kill_requested(&self) -> Option<CancellationToken> {
        let state = self.inner.state.lock().unwrap();
        if matches!(state.kill, KillState::Requested | KillState::InProgress) {
            Some(self.inner.kill_finished.clone())
        } else {
            None
        }
    }
    pub(crate) fn take_kill_attempt(&self) -> Option<KillAttempt> {
        let mut state = self.inner.state.lock().unwrap();
        if state.kill != KillState::Requested {
            return None;
        }
        state.kill = KillState::InProgress;
        Some(KillAttempt {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl Drop for TerminationWriter {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.inner.state.lock().unwrap();
            state.writer_alive = false;
            if matches!(state.kill, KillState::Requested | KillState::InProgress) {
                state.kill = KillState::Finished;
                true
            } else {
                false
            }
        };
        if notify {
            self.inner.kill_finished.cancel();
        }
    }
}

impl Drop for KillAttempt {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.inner.state.lock().unwrap();
            if state.kill == KillState::InProgress {
                state.kill = KillState::Finished;
                true
            } else {
                false
            }
        };
        if notify {
            self.inner.kill_finished.cancel();
        }
    }
}

impl TerminationReaper {
    pub(crate) async fn ready(&self) {
        self.inner.terminal.cancelled().await;
        let wait_for_kill = matches!(
            self.inner.state.lock().unwrap().kill,
            KillState::Requested | KillState::InProgress
        );
        if wait_for_kill {
            self.inner.kill_finished.cancelled().await;
        }
    }
    pub(crate) async fn ready_or_graceful_close<F>(
        &self,
        peer_fin: &CancellationToken,
        outbound_drained: F,
    ) where
        F: Future<Output = Result<(), std::io::ErrorKind>>,
    {
        self.ready_or_graceful_close_with_timeout(
            peer_fin,
            outbound_drained,
            GRACEFUL_CLOSE_TIMEOUT,
        )
        .await;
    }
    async fn ready_or_graceful_close_with_timeout<F>(
        &self,
        peer_fin: &CancellationToken,
        outbound_drained: F,
        timeout: Duration,
    ) where
        F: Future<Output = Result<(), std::io::ErrorKind>>,
    {
        tokio::pin!(outbound_drained);
        let timeout = tokio::time::sleep(timeout);
        tokio::pin!(timeout);
        tokio::select! {
            () = self.ready() => return,
            () = &mut timeout => return,
            () = peer_fin.cancelled() => (),
        }
        let drain_result = tokio::select! {
            () = self.ready() => return,
            () = &mut timeout => return,
            result = &mut outbound_drained => result,
        };
        if drain_result.is_err() {
            tokio::select! {
                () = self.ready() => (),
                () = &mut timeout => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_kill_allows_immediate_reap() {
        let (presser, _writer, reaper) = channel();
        presser.press_broken_pipe(PeerReset::NoKill, None);
        tokio::time::timeout(std::time::Duration::from_millis(100), reaper.ready())
            .await
            .expect("a terminal error without KILL must reap immediately");
    }

    #[tokio::test]
    async fn kill_gates_reap_until_attempt_finishes() {
        let (presser, writer, reaper) = channel();
        presser.press_broken_pipe(PeerReset::SendKill, None);
        let mut ready = Box::pin(reaper.ready());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut ready)
                .await
                .is_err()
        );
        let attempt = writer.take_kill_attempt().expect("KILL request");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut ready)
                .await
                .is_err()
        );
        drop(attempt);
        tokio::time::timeout(std::time::Duration::from_millis(100), ready)
            .await
            .expect("finished KILL attempt must release the reaper");
    }

    #[tokio::test]
    async fn writer_drop_releases_kill_waiter() {
        let (presser, writer, reaper) = channel();
        presser.press_broken_pipe(PeerReset::SendKill, None);
        drop(writer);
        tokio::time::timeout(std::time::Duration::from_millis(100), reaper.ready())
            .await
            .expect("a dead writer cannot complete a KILL attempt");
    }

    #[tokio::test]
    async fn graceful_close_waits_for_peer_fin_and_outbound_drain() {
        let (_presser, _writer, reaper) = channel();
        let peer_fin = CancellationToken::new();
        let outbound_drained = CancellationToken::new();
        let mut ready = Box::pin(reaper.ready_or_graceful_close(&peer_fin, async {
            outbound_drained.cancelled().await;
            Ok(())
        }));
        peer_fin.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut ready)
                .await
                .is_err()
        );
        outbound_drained.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(100), ready)
            .await
            .expect("peer FIN plus outbound drain must permit graceful reap");
    }

    #[tokio::test]
    async fn graceful_close_does_not_poll_drain_before_peer_fin() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_presser, _writer, reaper) = channel();
        let peer_fin = CancellationToken::new();
        let drain_polled = Arc::new(AtomicBool::new(false));
        let drain = {
            let drain_polled = Arc::clone(&drain_polled);
            std::future::poll_fn(move |_| {
                drain_polled.store(true, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(()))
            })
        };
        let mut ready = Box::pin(reaper.ready_or_graceful_close(&peer_fin, drain));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut ready)
                .await
                .is_err()
        );
        assert!(
            !drain_polled.load(Ordering::SeqCst),
            "outbound drain must not be observed before peer FIN"
        );
        peer_fin.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(100), ready)
            .await
            .expect("peer FIN must release the drain phase");
        assert!(drain_polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drain_error_does_not_bypass_pending_kill() {
        let (presser, writer, reaper) = channel();
        let peer_fin = CancellationToken::new();
        peer_fin.cancel();
        presser.press_broken_pipe(PeerReset::SendKill, None);
        let attempt = writer.take_kill_attempt().expect("KILL request");
        let mut ready = Box::pin(
            reaper
                .ready_or_graceful_close(&peer_fin, async { Err(std::io::ErrorKind::BrokenPipe) }),
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut ready)
                .await
                .is_err()
        );
        drop(attempt);
        tokio::time::timeout(std::time::Duration::from_millis(100), ready)
            .await
            .expect("drain failure must wait for the best-effort KILL attempt");
    }

    #[tokio::test]
    async fn graceful_close_timeout_permits_reap() {
        let (_presser, _writer, reaper) = channel();
        let peer_fin = CancellationToken::new();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reaper.ready_or_graceful_close_with_timeout(
                &peer_fin,
                std::future::pending(),
                std::time::Duration::from_millis(10),
            ),
        )
        .await
        .expect("the graceful-close ceiling must eventually permit reap");
    }

    #[tokio::test]
    async fn graceful_close_timeout_still_applies_after_drain_error() {
        let (presser, writer, reaper) = channel();
        let peer_fin = CancellationToken::new();
        peer_fin.cancel();
        presser.press_broken_pipe(PeerReset::SendKill, None);
        let attempt = writer.take_kill_attempt().expect("KILL request");
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reaper.ready_or_graceful_close_with_timeout(
                &peer_fin,
                async { Err(std::io::ErrorKind::BrokenPipe) },
                std::time::Duration::from_millis(10),
            ),
        )
        .await
        .expect("the close ceiling must bound a stalled KILL after drain failure");
        drop(attempt);
    }
}
