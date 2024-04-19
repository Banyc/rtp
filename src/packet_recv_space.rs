use std::collections::BTreeMap;

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

    pub fn reuse_buf(&mut self) -> Option<Vec<u8>> {
        self.reused_buf.pop()
    }

    pub fn return_buf(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.reused_buf.push(buf);
    }

    pub fn recv(&mut self, seq: u64, data: Vec<u8>) {
        self.receiving.insert(seq, data);
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
