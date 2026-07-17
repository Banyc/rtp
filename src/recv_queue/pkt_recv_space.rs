use std::collections::BTreeMap;

use primitive::arena::obj_pool::{ObjPool, buf_pool};

use crate::{
    delivery::frame::{
        FrameDelivery,
        recv::{RecvPkt, RecvSlot},
    },
    sack::AckQueue,
};

pub const MAX_NUM_RECVING_PKTS: usize = 2 << 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecvDisposition {
    Rejected,
    Duplicate,
    Inserted,
}

impl RecvDisposition {
    pub(crate) fn should_ack(self) -> bool {
        matches!(self, Self::Duplicate | Self::Inserted)
    }

    pub(crate) fn is_new(self) -> bool {
        matches!(self, Self::Inserted)
    }
}

#[derive(Debug)]
pub struct PktRecvSpace {
    /// In-order cursor: the next sequence number the byte-stream delivery
    /// path expects.  Frame-delivery delivery may pop frames past this
    /// cursor; the cursor advances when contiguous tombstones (and data
    /// slots delivered via `pop`) are collapsed.
    next: Option<u64>,
    slots: BTreeMap<u64, RecvSlot>,
    reused_buf: ObjPool<Vec<u8>>,
    ack_history: AckQueue,
}

impl PktRecvSpace {
    pub fn new(_frame_delivery: FrameDelivery) -> Self {
        Self {
            next: Some(0),
            slots: BTreeMap::new(),
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            ack_history: AckQueue::new(),
        }
    }

    pub fn ack_history(&self) -> &AckQueue {
        &self.ack_history
    }

    pub fn next_seq(&self) -> Option<u64> {
        self.next
    }

    pub fn num_recved_pkts(&self) -> usize {
        self.slots.len()
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    /// Return the disposition of the received packet.
    pub fn recv(&mut self, seq: u64, data: Vec<u8>, frame_len: Option<u32>) -> bool {
        let disposition = self.recv_disposition(seq, data, frame_len);
        disposition.should_ack()
    }

    /// Return the disposition of the received packet:
    /// `Rejected` for closed/out-of-window,
    /// `Duplicate` for stale/already-present,
    /// `Inserted` only after slot insertion and ack-history insertion.
    pub(crate) fn recv_disposition(
        &mut self,
        seq: u64,
        data: Vec<u8>,
        frame_len: Option<u32>,
    ) -> RecvDisposition {
        let Some(next) = self.next else {
            self.reused_buf.put(data);
            return RecvDisposition::Rejected;
        };

        // Stale: seq is already behind the in-order cursor.
        if seq < next {
            self.reused_buf.put(data);
            return RecvDisposition::Duplicate;
        }

        // Window check.
        if seq - next >= MAX_NUM_RECVING_PKTS as u64 {
            self.reused_buf.put(data);
            return RecvDisposition::Rejected;
        }

        // In-window duplicate: already have this slot.
        if self.slots.contains_key(&seq) {
            self.reused_buf.put(data);
            return RecvDisposition::Duplicate;
        }

        self.slots
            .insert(seq, RecvSlot::Data(RecvPkt { data, frame_len }));
        self.ack_history.insert(seq);
        RecvDisposition::Inserted
    }

    /// Pop one complete frame (possibly out of order past sequence holes).
    /// The consumed slots are replaced with `Tombstone`s, and a contiguous
    /// tombstone prefix at the in-order cursor is collapsed.  The scan and
    /// extraction logic lives in [`crate::delivery::frame::recv`].
    pub fn pop_complete_frame(&mut self) -> Option<Vec<u8>> {
        let frame_bytes = crate::delivery::frame::recv::pop_complete_frame(
            &mut self.slots,
            &mut self.reused_buf,
        )?;

        self.collapse_tombstone_prefix();

        // After collapsing, check if a FIN has been received: an empty-payload
        // packet with no frame_len that is now at the in-order head after
        // all earlier slots are consumed or tombstoned.  We don't set a flag
        // here — that's done by the caller (reliable_layer) when it sees an
        // empty-payload non-frame packet at pop/peek.

        Some(frame_bytes)
    }

    /// Advance `next` past contiguous tombstone(s) at the head of the slot
    /// map, removing them.  This is what bounds memory: tombstones count
    /// toward the window until collapsed.
    fn collapse_tombstone_prefix(&mut self) {
        let Some(mut next) = self.next else {
            return;
        };
        while let Some(RecvSlot::Tombstone) = self.slots.get(&next) {
            self.slots.remove(&next);
            next += 1;
        }
        self.next = Some(next);
    }

    pub fn peek(&self) -> Option<&Vec<u8>> {
        let next = self.next?;
        match self.slots.get(&next)? {
            RecvSlot::Data(pkt) => Some(&pkt.data),
            RecvSlot::Tombstone => None,
        }
    }

    /// In frame-delivery mode, check whether a FIN (empty-payload packet
    /// with no `frame_len`) is sitting at the in-order head of the receive
    /// queue — i.e. all earlier packets have been consumed or tombstoned.
    /// This is the EOF signal for frame mode: `move_recv_data` is bypassed
    /// in frame mode, so the `recv_fin_buf` flag is never set by that path.
    /// Instead, the caller checks this method directly to surface EOF.
    ///
    /// Returns `true` only when:
    /// - `next` is `Some(n)` and a `Data` slot exists at `n`, and
    /// - that slot's payload is empty (`data.is_empty()`), and
    /// - that slot has no `frame_len` (it's a FIN, not a zero-length frame).
    pub fn fin_at_head(&self) -> bool {
        crate::delivery::frame::recv::fin_at_head(self.next, &self.slots)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        loop {
            let next = self.next?;
            match self.slots.remove(&next) {
                Some(RecvSlot::Data(pkt)) => {
                    self.next = Some(next + 1);
                    self.collapse_tombstone_prefix();
                    return Some(pkt.data);
                }
                Some(RecvSlot::Tombstone) => {
                    self.next = Some(next + 1);
                    continue;
                }
                None => {
                    return None;
                }
            }
        }
    }
}

impl Default for PktRecvSpace {
    fn default() -> Self {
        Self::new(FrameDelivery::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stock_recv_pop_in_order() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert!(space.recv(1, b"world".to_vec(), None));
        assert_eq!(space.peek().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
        assert!(space.pop().is_none());
    }

    #[test]
    fn stock_recv_out_of_order() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        assert!(space.recv(1, b"world".to_vec(), None));
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
    }

    #[test]
    fn stale_seq_is_acked_but_not_inserted() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        assert!(space.recv(0, b"a".to_vec(), None));
        let ack_count_after = space.ack_history().balls().count();
        space.pop();
        // seq 0 is now behind next == Some(1)
        assert!(space.recv(0, b"stale".to_vec(), None));
        // Should not be in the slot map.
        assert!(!space.slots.contains_key(&0));
        // But it should still be acked (balls count unchanged — it's already
        // merged into the same ball).
        assert_eq!(space.ack_history().balls().count(), ack_count_after);
    }

    #[test]
    fn window_capacity_rejects() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        let far_seq = MAX_NUM_RECVING_PKTS as u64; // seq 0 is next, so seq 8192 is out of window
        assert!(!space.recv(far_seq, b"x".to_vec(), None));
    }

    #[test]
    fn duplicate_acked_but_not_reinserted() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        assert!(space.recv(0, b"a".to_vec(), None));
        assert!(space.recv(0, b"b".to_vec(), None));
        let pkt = space.slots.get(&0).unwrap();
        match pkt {
            RecvSlot::Data(p) => assert_eq!(p.data, b"a"),
            _ => panic!("expected data"),
        }
    }

    #[test]
    fn fin_empty_payload_no_frame_len() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        // FIN: empty payload, no frame_len.
        assert!(space.recv(0, vec![], None));
        assert_eq!(space.peek().unwrap().len(), 0);
        let popped = space.pop().unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn tombstone_prefix_collapses() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        // Insert tombstones ahead of the cursor.
        space.slots.insert(0, RecvSlot::Tombstone);
        space.slots.insert(1, RecvSlot::Tombstone);
        space.slots.insert(
            2,
            RecvSlot::Data(RecvPkt {
                data: b"hello".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(0);
        space.collapse_tombstone_prefix();
        assert_eq!(space.next, Some(2));
        assert_eq!(space.pop().unwrap(), b"hello");
    }

    #[test]
    fn pop_skips_head_tombstones() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        space.slots.insert(0, RecvSlot::Tombstone);
        space.slots.insert(
            1,
            RecvSlot::Data(RecvPkt {
                data: b"world".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(0);
        // First pop skips tombstone at 0.
        assert_eq!(space.pop().unwrap(), b"world");
        assert_eq!(space.next, Some(2));
    }

    // Frame delivery tests — enabled mode.

    #[test]
    fn ooo_complete_frame_delivers_past_sequence_hole() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Hole at seq 0; deliver a complete 1-packet frame at seq 1 first.
        assert!(space.recv(1, b"frame1".to_vec(), Some(6)));
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"frame1");
        // Frame was delivered; seq 1 is now a tombstone.
        assert!(matches!(space.slots.get(&1), Some(RecvSlot::Tombstone)));
        // Now fill the hole at seq 0 to verify the window still works.
        assert!(space.recv(0, b"late".to_vec(), None));
        assert_eq!(space.pop().unwrap(), b"late");
    }

    #[test]
    fn multi_packet_frame_waits_for_all_its_packets() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Frame of 11 bytes across 3 packets: seq 1 (start, 6 bytes), seq 2 (3 bytes), seq 3 (2 bytes).
        // Send all but the last.
        assert!(space.recv(1, b"hello ".to_vec(), Some(11)));
        assert!(space.recv(2, b"wor".to_vec(), None));
        // Frame is incomplete (11 bytes not collected yet).
        assert!(space.pop_complete_frame().is_none());
        // Send the last packet.
        assert!(space.recv(3, b"ld".to_vec(), None));
        // Now complete.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"hello world");
    }

    #[test]
    fn frame_reassembly_survives_shuffled_arrival() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Frame A: seq 0 (start, len 4), seq 1 (cont)
        // Frame B: seq 2 (start, len 4), seq 3 (cont)
        // Deliver in reverse order to test contiguity handling.
        assert!(space.recv(3, b"B2".to_vec(), None));
        assert!(space.recv(2, b"B1".to_vec(), Some(4)));
        assert!(space.recv(1, b"A2".to_vec(), None));
        assert!(space.recv(0, b"A1".to_vec(), Some(4)));

        // Both frames should deliver in order of their start sequences.
        let frame_a = space.pop_complete_frame().unwrap();
        let frame_b = space.pop_complete_frame().unwrap();

        assert_eq!(frame_a, b"A1A2");
        assert_eq!(frame_b, b"B1B2");

        // All tombstones were collapsed by pop_complete_frame's internal
        // collapse_tombstone_prefix.  The in-order cursor should be at 4.
        assert_eq!(space.next, Some(4));
        assert!(space.slots.is_empty());
    }

    #[test]
    fn frame_continuation_with_own_frame_len_starts_new_run() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Frame: seq 1 (start, len 6)
        // But seq 2 is a new frame start (len 3), not a continuation — gap in seq 1's run.
        // Then seq 3 would be continuation of seq 2's frame.
        assert!(space.recv(1, b"fram".to_vec(), Some(6)));
        assert!(space.recv(2, b"new".to_vec(), Some(3)));
        assert!(space.recv(3, b"foo".to_vec(), None));

        // seq 2+3 should form a complete frame (3 bytes each, but frame_len is 3 so only seq 2 needed? No — frame_len is the total frame len, and seq 2 alone has 3 bytes = frame_len of 3, so it IS complete).
        // Actually the test scenario: seq 1 frame is incomplete (need 6, only have 3 from seq 1).
        // seq 2 is a complete frame (len 3, data "new" = 3 bytes).
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"new");
    }

    #[test]
    fn ack_generation_unaffected_by_early_delivery() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Insert packets out of order and verify ack_history is stock.
        assert!(space.recv(2, b"c".to_vec(), None));
        assert!(space.recv(0, b"a".to_vec(), None));
        assert!(space.recv(1, b"b".to_vec(), None));

        // ack_history should contain all three (merged into one ball 0..3).
        let balls: Vec<_> = space.ack_history().balls().collect();
        assert_eq!(balls.len(), 1);
        assert_eq!(balls[0].start, 0);
        assert_eq!(balls[0].size.get(), 3);

        // After pop_complete_frame or pop, ack_history is NOT touched.
        space.pop();
        let balls_after: Vec<_> = space.ack_history().balls().collect();
        assert_eq!(balls_after.len(), 1);
        assert_eq!(balls_after[0].start, 0);
        assert_eq!(balls_after[0].size.get(), 3);
    }

    #[test]
    fn pop_complete_frame_tombstones_count_toward_window() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Deliver a frame at seq 1, leaving a hole at seq 0.
        assert!(space.recv(1, b"x".to_vec(), Some(1)));
        space.pop_complete_frame();
        // seq 1 is now a tombstone; seq 0 is a hole.
        assert!(matches!(space.slots.get(&1), Some(RecvSlot::Tombstone)));
        assert!(!space.slots.contains_key(&0));
        // The tombstone at seq 1 keeps `next` pinned at 0 (hole at 0 prevents
        // collapse), so the window is [0, 8191].  seq 8192 is out of window.
        let out_of_window = MAX_NUM_RECVING_PKTS as u64;
        assert!(!space.recv(out_of_window, b"reject".to_vec(), None));
        // seq 8191 is the last in-window seq.
        let last_in_window = out_of_window - 1;
        assert!(space.recv(last_in_window, b"ok".to_vec(), None));
    }

    #[test]
    fn mode_off_is_stock_byte_identical() {
        let mut space = PktRecvSpace::new(FrameDelivery::default());
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert!(space.recv(2, b"world".to_vec(), None));
        assert!(space.recv(1, b" ".to_vec(), None));

        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b" ");
        assert_eq!(space.pop().unwrap(), b"world");
        assert!(space.pop().is_none());
    }

    /// Fix #8: `fin_at_head_after_all_data_delivered` — a FIN (empty-payload
    /// packet with no `frame_len`) delivered in frame mode after all data
    /// surfaces EOF via `fin_at_head()`.  Before the fix, `recv_data_pkt`
    /// returned early in frame mode bypassing `move_recv_data` (the only
    /// place that set `recv_fin_buf`), so an in-order FIN yielded
    /// `WouldBlock` forever (connections hung on close).
    #[test]
    fn fin_at_head_after_all_data_delivered() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Deliver a data frame at seq 0, then a FIN at seq 1.
        assert!(space.recv(0, b"data".to_vec(), Some(4)));
        assert!(space.recv(1, vec![], None));

        // Pop the data frame first.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"data");

        // Now the FIN is at the in-order head (seq 1, after seq 0 was
        // tombstoned and collapsed).  `fin_at_head()` must return true.
        // First, the tombstone at seq 0 must be collapsed.
        space.collapse_tombstone_prefix();
        assert!(
            space.fin_at_head(),
            "FIN at in-order head must be detected after all data delivered"
        );
    }

    /// Fix #8 (inverse): `fin_behind_gap_does_not_surface_eof` — a FIN
    /// behind an out-of-order gap does NOT surface EOF until the gap fills.
    #[test]
    fn fin_behind_gap_does_not_surface_eof() {
        let mut space = PktRecvSpace::new(FrameDelivery::enabled());
        // Gap at seq 0; FIN at seq 1 (but seq 0 is missing).
        assert!(space.recv(1, vec![], None));

        // The FIN is NOT at the in-order head (seq 0 is missing).
        assert!(
            !space.fin_at_head(),
            "FIN behind a gap must not surface EOF"
        );

        // Fill the gap with a data frame.
        assert!(space.recv(0, b"late".to_vec(), Some(4)));
        // Pop the data frame.
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"late");
        space.collapse_tombstone_prefix();

        // Now the FIN is at the in-order head.
        assert!(
            space.fin_at_head(),
            "FIN must surface EOF after the gap fills"
        );
    }
}
