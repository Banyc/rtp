// Shared by the client and server example binaries. Each binary only uses a
// subset of this API, so dead-code analysis per binary is expected.
#![allow(dead_code)]

use std::future::Future;
use tokio::task::JoinSet;

#[derive(Debug)]
pub enum TransportTaskExit {
    SessionEnded(&'static str),
    DriverFailed {
        driver: &'static str,
        detail: String,
    },
}

pub struct TransportScope {
    tasks: JoinSet<TransportTaskExit>,
}

impl TransportScope {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub fn supervise_session(&mut self, name: &'static str, session: rtp::socket::SessionHandle) {
        self.tasks.spawn(async move {
            session.await;
            TransportTaskExit::SessionEnded(name)
        });
    }

    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = TransportTaskExit> + Send + 'static,
    {
        self.tasks.spawn(task);
    }

    pub async fn race<F, T>(&mut self, operation: F) -> T
    where
        F: Future<Output = T>,
    {
        tokio::pin!(operation);
        tokio::select! {
            biased;
            value = &mut operation => value,
            joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                match joined {
                    Some(Ok(exit)) => panic!("transport child exited early: {:?}", exit),
                    Some(Err(error)) if error.is_panic() => {
                        std::panic::resume_unwind(error.into_panic())
                    }
                    Some(Err(error)) => panic!("transport child failed to join: {}", error),
                    None => unreachable!("guard excludes an empty task set"),
                }
            }
        }
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
}
