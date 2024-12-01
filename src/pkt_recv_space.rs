use core::num::NonZeroUsize;

use primitive::{
    arena::obj_pool::{buf_pool, ObjPool},
    ops::len::Len,
    queue::seq_queue::{SeqInsertResult, SeqQueue},
};

use crate::sack::AckQueue;

pub const MAX_NUM_RECVING_PKTS: usize = 2 << 12;

#[derive(Debug)]
pub struct PktRecvSpace {
    recving: SeqQueue<u64, Vec<u8>>,
    reused_buf: ObjPool<Vec<u8>>,
    ack_history: AckQueue,
}
impl PktRecvSpace {
    pub fn new() -> Self {
        let mut recving = SeqQueue::new(NonZeroUsize::new(MAX_NUM_RECVING_PKTS).unwrap());
        recving.set_next(0, |_| {});
        Self {
            recving,
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            ack_history: AckQueue::new(),
        }
    }

    pub fn ack_history(&self) -> &AckQueue {
        &self.ack_history
    }

    pub fn next_seq(&self) -> Option<u64> {
        self.recving.next().copied()
    }

    pub fn num_recved_pkts(&self) -> usize {
        self.recving.len()
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    /// Return `false` if the data is rejected due to window capacity
    pub fn recv(&mut self, seq: u64, data: Vec<u8>) -> bool {
        if self.recving.next().is_none() {
            self.reused_buf.put(data);
            return false;
        }
        let res = self
            .recving
            .insert(seq, data, |(_, data)| self.reused_buf.put(data));
        match res {
            SeqInsertResult::Stalled => {
                panic!();
            }
            SeqInsertResult::Stale => (),
            SeqInsertResult::InOrder | SeqInsertResult::OutOfOrder => {
                self.ack_history.insert(seq);
            }
            SeqInsertResult::OutOfWindow => {
                return false;
            }
        }
        true
    }

    pub fn peek(&self) -> Option<&Vec<u8>> {
        self.recving.peek().map(|(_, value)| value)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.recving
            .pop(|(_, data)| self.reused_buf.put(data))
            .map(|(_, value)| value)
    }
}
impl Default for PktRecvSpace {
    fn default() -> Self {
        Self::new()
    }
}
