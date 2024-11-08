use primitive::{
    arena::obj_pool::{buf_pool, ObjectPool},
    queue::seq_queue::{BTreeSeqQueue, SeqInsertResult},
    Len,
};

use crate::sack::AckQueue;

pub const MAX_NUM_RECEIVING_PACKETS: usize = 2 << 12;

#[derive(Debug)]
pub struct PacketRecvSpace {
    receiving: BTreeSeqQueue<u64, Vec<u8>>,
    reused_buf: ObjectPool<Vec<u8>>,
    ack_history: AckQueue,
}
impl PacketRecvSpace {
    pub fn new() -> Self {
        let mut receiving = BTreeSeqQueue::new();
        receiving.set_next(0, |_| {});
        Self {
            receiving,
            reused_buf: buf_pool(Some(MAX_NUM_RECEIVING_PACKETS)),
            ack_history: AckQueue::new(),
        }
    }

    pub fn ack_history(&self) -> &AckQueue {
        &self.ack_history
    }

    pub fn next_seq(&self) -> Option<u64> {
        self.receiving.next().copied()
    }

    pub fn num_received_packets(&self) -> usize {
        self.receiving.len()
    }

    pub fn reused_buf(&mut self) -> &mut ObjectPool<Vec<u8>> {
        &mut self.reused_buf
    }

    /// Return `false` if the data is rejected due to window capacity
    pub fn recv(&mut self, seq: u64, data: Vec<u8>) -> bool {
        let Some(next) = self.receiving.next() else {
            self.reused_buf.put(data);
            return false;
        };
        if next.saturating_add(MAX_NUM_RECEIVING_PACKETS as u64) < seq {
            self.reused_buf.put(data);
            return false;
        }
        let res = self
            .receiving
            .insert(seq, data, |(_, data)| self.reused_buf.put(data));
        match res {
            SeqInsertResult::Stalled => {
                panic!();
            }
            SeqInsertResult::Stale => (),
            SeqInsertResult::InOrder | SeqInsertResult::OutOfOrder => {
                self.ack_history.insert(seq);
            }
        }
        true
    }

    pub fn peak(&self) -> Option<&Vec<u8>> {
        self.receiving.peak().map(|(_, value)| value)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.receiving.pop().map(|(_, value)| value)
    }
}
impl Default for PacketRecvSpace {
    fn default() -> Self {
        Self::new()
    }
}
