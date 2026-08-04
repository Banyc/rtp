//! Send-side byte-stream staging buffer (stock mode).

use core::num::NonZeroUsize;

use primitive::{
    ops::len::{Capacity, Len, LenExt},
    queue::cap_queue::CapVecQueue,
};

use crate::codec::data_overhead;

const SEND_DATA_BUF_LEN: usize = 8 * 1024;
const MAX_SEND_DATA_BUF_LEN: usize = 64 * 1024;

/// Send staging buffer size for a given MSS.
pub(crate) fn send_data_buf_len(mss: NonZeroUsize) -> usize {
    if mss.get() <= crate::udp::NO_FEC_MSS {
        return SEND_DATA_BUF_LEN;
    }
    let payload = mss.get() - data_overhead();
    let pkts = MAX_SEND_DATA_BUF_LEN / payload;
    pkts * payload
}

/// Send-side byte-stream staging buffer.
#[derive(Debug)]
pub(crate) struct StockSendStage {
    buf: CapVecQueue<u8>,
}

impl StockSendStage {
    pub(crate) fn new(mss: NonZeroUsize) -> Self {
        Self {
            buf: CapVecQueue::new_vec(send_data_buf_len(mss)),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Stage up to `data.len()` bytes, respecting `soft_cap`.
    pub(crate) fn stage(&mut self, data: &[u8], soft_cap: usize) -> usize {
        let free_bytes = soft_cap.saturating_sub(self.buf.len());
        let write_bytes = free_bytes.min(data.len());
        self.buf.batch_enqueue(&data[..write_bytes]);
        write_bytes
    }

    pub(crate) fn dequeue_extend(&mut self, len: usize, out: &mut Vec<u8>) {
        self.buf.batch_dequeue_extend(len, out);
    }
}
