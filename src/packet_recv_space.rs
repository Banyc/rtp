use std::collections::BTreeMap;

use crate::{reused_buf::ReusedBuf, sack::AckQueue};

pub const MAX_NUM_RECEIVING_PACKETS: usize = 2 << 12;

#[derive(Debug, Clone)]
pub struct PacketRecvSpace {
    next_seq: u64,
    receiving: BTreeMap<u64, Vec<u8>>,
    reused_buf: ReusedBuf<Vec<u8>>,
    ack_history: AckQueue,
}
impl PacketRecvSpace {
    pub fn new() -> Self {
        Self {
            next_seq: 0,
            receiving: BTreeMap::new(),
            reused_buf: ReusedBuf::new(MAX_NUM_RECEIVING_PACKETS),
            ack_history: AckQueue::new(),
        }
    }

    pub fn ack_history(&self) -> &AckQueue {
        &self.ack_history
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn num_received_packets(&self) -> usize {
        self.receiving.len()
    }

    pub fn reused_buf(&mut self) -> &mut ReusedBuf<Vec<u8>> {
        &mut self.reused_buf
    }

    /// Return `false` if the data is rejected due to window capacity
    pub fn recv(&mut self, seq: u64, data: Vec<u8>) -> bool {
        if self.next_seq + (MAX_NUM_RECEIVING_PACKETS as u64) < seq {
            self.reused_buf.return_buf(data);
            return false;
        }
        if seq < self.next_seq {
            self.reused_buf.return_buf(data);
            return true;
        }
        self.receiving.insert(seq, data);
        self.ack_history.insert(seq);
        true
    }

    pub fn peak(&mut self) -> Option<&Vec<u8>> {
        self.receiving.get(&self.next_seq)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        let p = self.receiving.remove(&self.next_seq)?;
        self.next_seq += 1;
        Some(p)
    }
}
impl Default for PacketRecvSpace {
    fn default() -> Self {
        Self::new()
    }
}
