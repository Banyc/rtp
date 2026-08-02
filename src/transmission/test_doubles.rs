use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;

use crate::io_err::IoErr;
use crate::transmission::transmission_layer::UnreliableWrite;

/// A write that signals `started`, blocks until `release` is notified, then
/// reports success.  A never-released instance blocks forever, so it doubles
/// as an always-pending write.
#[derive(Debug)]
pub(crate) struct BlockingWrite {
    pub(crate) started: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
}

impl BlockingWrite {
    pub(crate) fn new() -> Self {
        Self {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

#[async_trait]
impl UnreliableWrite for BlockingWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(buf.len())
    }
}

struct CancelProbe {
    cancelled: Arc<AtomicBool>,
    completed: bool,
}

impl Drop for CancelProbe {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }
}

/// A write that signals `started`, blocks until `release` is notified, and
/// records whether the in-flight send was cancelled before completing.
#[derive(Debug)]
pub(crate) struct PendingWrite {
    pub(crate) started: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
    pub(crate) cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl UnreliableWrite for PendingWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        let mut probe = CancelProbe {
            cancelled: Arc::clone(&self.cancelled),
            completed: false,
        };
        self.started.notify_one();
        self.release.notified().await;
        probe.completed = true;
        Ok(buf.len())
    }
}
