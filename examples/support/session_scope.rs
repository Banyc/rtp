// Shared by the client and server example binaries. Each binary only uses a
// subset of this API, so dead-code analysis per binary is expected.
use std::future::Future;

use tokio::task::JoinSet;

#[derive(Debug)]
pub enum TransportTaskExit {
    SessionEnded(&'static str),
    #[allow(dead_code)]
    DriverFailed {
        driver: &'static str,
        detail: String,
    },
}

pub struct TransportScope {
    /// Perpetual accept drivers. These never end on their own in a healthy
    /// run; on shutdown they are aborted and drained so they never outlive
    /// the foreground operation.
    drivers: JoinSet<TransportTaskExit>,
    /// RTP/MPUDP session supervisors. Each task wraps a [`SessionHandle`]
    /// future. On shutdown these are *joined* (not aborted) so the session
    /// epilog — FIN/KILL exchange and driver reaping — runs to completion.
    sessions: JoinSet<TransportTaskExit>,
}

impl TransportScope {
    pub fn new() -> Self {
        Self {
            drivers: JoinSet::new(),
            sessions: JoinSet::new(),
        }
    }

    pub fn supervise_session(&mut self, name: &'static str, session: rtp::socket::SessionHandle) {
        self.sessions.spawn(async move {
            session.await;
            TransportTaskExit::SessionEnded(name)
        });
    }

    #[allow(dead_code)]
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = TransportTaskExit> + Send + 'static,
    {
        self.drivers.spawn(task);
    }

    pub async fn race<F, T>(&mut self, operation: F) -> T
    where
        F: Future<Output = T>,
    {
        tokio::pin!(operation);
        let value = tokio::select! {
            biased;
            value = &mut operation => value,
            joined = self.sessions.join_next(), if !self.sessions.is_empty() => {
                let joined = joined.unwrap();
                let exit = joined.unwrap();
                match exit {
                    TransportTaskExit::SessionEnded(name) => {
                        panic!("transport child exited early: session {name} ended")
                    }
                    TransportTaskExit::DriverFailed { driver, detail } => {
                        panic!("transport child exited early: driver {driver} failed: {detail}")
                    }
                }
            }
            joined = self.drivers.join_next(), if !self.drivers.is_empty() => {
                let joined = joined.unwrap();
                let exit = joined.unwrap();
                match exit {
                    TransportTaskExit::SessionEnded(name) => {
                        panic!("transport child exited early: session {name} ended")
                    }
                    TransportTaskExit::DriverFailed { driver, detail } => {
                        panic!("transport child exited early: driver {driver} failed: {detail}")
                    }
                }
            }
        };
        self.shutdown().await;
        value
    }

    /// Orderly teardown once the foreground operation has finished:
    /// abort and drain the perpetual accept drivers first, then normally
    /// join the session supervisors so each session's epilog (FIN/KILL
    /// exchange and driver reaping) is allowed to complete.
    ///
    /// Both drain loops observe their join results: only the expected
    /// cancellation of an aborted driver is ignored. A driver that failed
    /// or panicked before the abort took effect, and any session that
    /// panicked during its epilog, is surfaced rather than swallowed.
    async fn shutdown(&mut self) {
        self.drivers.abort_all();
        while let Some(result) = self.drivers.join_next().await {
            observe_driver_exit(result);
        }
        while let Some(result) = self.sessions.join_next().await {
            observe_session_exit(result);
        }
    }
}

impl Drop for TransportScope {
    fn drop(&mut self) {
        // Safety net for the abnormal path (panic / early return before
        // `race` could run `shutdown`): abort everything so no driver or
        // session supervisor outlives the scope. The normal path drains
        // both sets inside `race`, leaving them empty here.
        self.drivers.abort_all();
        self.sessions.abort_all();
    }
}

/// Observe a perpetual accept driver's exit during shutdown. The only
/// expected result is cancellation (the abort we just issued); any other
/// outcome — a driver that failed or panicked before the abort landed —
/// is surfaced rather than swallowed.
fn observe_driver_exit(result: Result<TransportTaskExit, tokio::task::JoinError>) {
    match result {
        Ok(TransportTaskExit::SessionEnded(name)) => {
            panic!("transport driver exited unexpectedly: session {name} ended")
        }
        Ok(TransportTaskExit::DriverFailed { driver, detail }) => {
            panic!("transport driver {driver} failed during shutdown: {detail}")
        }
        Err(error) if error.is_cancelled() => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("transport driver failed to join during shutdown: {error}"),
    }
}

/// Observe a session supervisor's exit during shutdown. Sessions are
/// joined, not aborted, so they must finish cleanly; a panic during the
/// epilog (FIN/KILL exchange or driver reaping) is resumed, and any
/// other failure is surfaced.
fn observe_session_exit(result: Result<TransportTaskExit, tokio::task::JoinError>) {
    match result {
        Ok(TransportTaskExit::SessionEnded(_)) => {}
        Ok(TransportTaskExit::DriverFailed { driver, detail }) => {
            panic!("transport session {driver} reported a driver failure during shutdown: {detail}")
        }
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("transport session failed to join during shutdown: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic(expected = "transport child exited early")]
    async fn early_session_exit_fails_the_operation() {
        let mut scope = TransportScope::new();
        scope.supervise_session("test", rtp::socket::SessionHandle::idle());
        scope.race(std::future::pending::<()>()).await;
    }

    #[tokio::test]
    async fn race_returns_the_operation_value() {
        let mut scope = TransportScope::new();
        assert_eq!(scope.race(async { 7_u8 }).await, 7);
    }

    #[tokio::test]
    async fn shutdown_ignores_an_aborted_driver() {
        let mut scope = TransportScope::new();
        scope.spawn(std::future::pending::<TransportTaskExit>());
        scope.race(async { 7_u8 }).await;
    }

    #[tokio::test]
    #[should_panic(expected = "transport driver rtp_accept failed during shutdown")]
    async fn shutdown_surfaces_a_driver_that_failed_before_the_abort() {
        let mut scope = TransportScope::new();
        scope.spawn(async {
            TransportTaskExit::DriverFailed {
                driver: "rtp_accept",
                detail: "listener closed".into(),
            }
        });
        // A ready foreground wins the biased select, so the already-failed
        // driver is only observed during the shutdown drain. Yield first so
        // the runtime polls the spawned driver to completion before the
        // foreground is consulted.
        scope
            .race(async {
                tokio::task::yield_now().await;
                7_u8
            })
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "injected driver panic")]
    async fn shutdown_resumes_a_driver_panic_that_beat_the_abort() {
        let mut scope = TransportScope::new();
        scope.spawn(async {
            panic!("injected driver panic");
        });
        scope.race(std::future::pending::<()>()).await;
    }

    #[tokio::test]
    async fn observe_session_exit_accepts_a_clean_session_ended() {
        observe_session_exit(Ok(TransportTaskExit::SessionEnded("rtp")));
    }

    #[tokio::test]
    #[should_panic(expected = "transport session rtp reported a driver failure")]
    async fn observe_session_exit_surfaces_an_unexpected_driver_failure() {
        observe_session_exit(Ok(TransportTaskExit::DriverFailed {
            driver: "rtp",
            detail: "epilog driver failed".into(),
        }));
    }

    #[tokio::test]
    #[should_panic(expected = "injected session epilog panic")]
    async fn observe_session_exit_resumes_a_panic() {
        let handle = tokio::spawn(async { panic!("injected session epilog panic") });
        let error = handle.await.unwrap_err();
        observe_session_exit(Err(error));
    }
}
