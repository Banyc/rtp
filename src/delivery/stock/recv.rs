//! Recv-side byte-stream buffer (stock mode).

use primitive::{
    ops::len::{Capacity, Len, LenExt},
    queue::cap_queue::CapVecQueue,
};

const RECV_DATA_BUF_LEN: usize = 2 << 16;

/// Recv-side byte-stream buffer drained from the packet receive space.
#[derive(Debug)]
pub(crate) struct StockRecvStage {
    buf: CapVecQueue<u8>,
}

impl StockRecvStage {
    pub(crate) fn new() -> Self {
        Self {
            buf: CapVecQueue::new_vec(RECV_DATA_BUF_LEN),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn read(&mut self, out: &mut [u8]) -> usize {
        let read_bytes = out.len().min(self.buf.len());
        let Some((a, b)) = self.buf.batch_dequeue(read_bytes) else {
            return 0;
        };
        out[..a.len()].copy_from_slice(a);
        if let Some(b) = b {
            out[a.len()..read_bytes].copy_from_slice(b);
        }
        read_bytes
    }

    pub(crate) fn enqueue(&mut self, data: &[u8]) {
        self.buf.batch_enqueue(data);
    }
}
