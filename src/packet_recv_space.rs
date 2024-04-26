use std::collections::BTreeMap;

const MAX_NUM_RECEIVING_PACKETS: usize = 2 << 12;

#[derive(Debug, Clone)]
pub struct PacketRecvSpace {
    next_seq: u64,
    receiving: BTreeMap<u64, Vec<u8>>,
    reused_buf: Vec<Vec<u8>>,
}
impl PacketRecvSpace {
    pub fn new() -> Self {
        Self {
            next_seq: 0,
            receiving: BTreeMap::new(),
            reused_buf: vec![],
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn num_received_packets(&self) -> usize {
        self.receiving.len()
    }

    pub fn reuse_buf(&mut self) -> Option<Vec<u8>> {
        self.reused_buf.pop()
    }

    pub fn return_buf(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.reused_buf.push(buf);
    }

    /// Return `false` if the data is rejected due to window capacity
    pub fn recv(&mut self, seq: u64, data: Vec<u8>) -> bool {
        if self.next_seq + (MAX_NUM_RECEIVING_PACKETS as u64) < seq {
            self.return_buf(data);
            return false;
        }
        if seq < self.next_seq {
            self.return_buf(data);
            return true;
        }
        self.receiving.insert(seq, data);
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
