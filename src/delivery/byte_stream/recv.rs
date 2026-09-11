//! Recv-side byte-stream buffer (stock mode).

use std::collections::VecDeque;

const RECV_DATA_BUF_LEN: usize = 2 << 16;

/// Recv-side byte-stream buffer drained from the packet receive space.
///
/// Chunks are staged by ownership: the pooled `Vec<u8>` popped from the packet
/// receive space is held here verbatim and copied to the application exactly
/// once on [`Self::read`]. A fully consumed chunk is handed back through
/// [`Self::drain_recycled`] so the caller can return it to the packet-space
/// pool, keeping the payload to a single copy per byte.
#[derive(Debug)]
pub(crate) struct StockRecvStage {
    chunks: VecDeque<Vec<u8>>,
    /// Bytes already consumed from the front chunk.
    head_offset: usize,
    /// Total bytes still staged across `chunks` (excluding `head_offset`).
    len: usize,
    /// Fully consumed chunks awaiting return to the packet-space pool.
    recycled: Vec<Vec<u8>>,
}

impl StockRecvStage {
    pub(crate) fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            head_offset: 0,
            len: 0,
            recycled: Vec::new(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        RECV_DATA_BUF_LEN
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn read(&mut self, out: &mut [u8]) -> usize {
        let mut written = 0;
        while written < out.len() {
            let Some(front) = self.chunks.front() else {
                break;
            };
            let available = front.len() - self.head_offset;
            if available == 0 {
                // A degenerate empty chunk (never produced by `enqueue_owned`,
                // which only receives non-empty payloads) must still be
                // retired or the drain loop cannot make progress.
                let front = self.chunks.pop_front().expect("front chunk exists");
                self.head_offset = 0;
                self.recycled.push(front);
                continue;
            }
            let take = available.min(out.len() - written);
            out[written..written + take]
                .copy_from_slice(&front[self.head_offset..self.head_offset + take]);
            self.head_offset += take;
            self.len -= take;
            written += take;
            if self.head_offset == front.len() {
                let front = self.chunks.pop_front().expect("front chunk exists");
                self.head_offset = 0;
                self.recycled.push(front);
            }
        }
        written
    }

    /// Stage an owned payload chunk. Empty payloads are FIN signals and are
    /// handled by the caller, never staged here.
    pub(crate) fn enqueue_owned(&mut self, data: Vec<u8>) {
        debug_assert!(!data.is_empty(), "empty payloads are FIN signals, not data");
        self.len += data.len();
        self.chunks.push_back(data);
    }

    /// Hand every fully consumed chunk to `sink` (typically for recycling into
    /// the packet-space buffer pool).
    pub(crate) fn drain_recycled(&mut self, mut sink: impl FnMut(Vec<u8>)) {
        for buf in self.recycled.drain(..) {
            sink(buf);
        }
    }
}
