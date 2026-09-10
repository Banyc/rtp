use primitive::arena::obj_pool::{ObjPool, buf_pool};

use crate::{
    ack::AckHistory,
    delivery::frame::recv::{RecvPkt, RecvSlot},
    sequence::{SequenceMap, SequenceNumber, SequencePosition, SequenceVacancyError, min},
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
    next: Option<SequenceNumber>,
    slots: SequenceMap<RecvSlot>,
    scan_start: SequenceNumber,
    reused_buf: ObjPool<Vec<u8>>,
    ack_history: AckHistory,
}

impl PktRecvSpace {
    /// Zero-seeded receive space (connections opened without the handshake).
    pub fn new() -> Self {
        Self::new_at(SequenceNumber::ZERO)
    }

    /// Receive space seeded at `initial_seq` (handshake-derived directional
    /// start): the in-order cursor, the slot window, and the ACK history all
    /// derive from the same initial value.
    pub fn new_at(initial_seq: SequenceNumber) -> Self {
        Self {
            next: Some(initial_seq),
            slots: SequenceMap::new(initial_seq, MAX_NUM_RECVING_PKTS as u64),
            scan_start: initial_seq,
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            ack_history: AckHistory::new_at(initial_seq),
        }
    }

    pub fn ack_history(&self) -> &AckHistory {
        &self.ack_history
    }

    pub fn next_seq(&self) -> Option<SequenceNumber> {
        self.next
    }

    pub fn num_recved_pkts(&self) -> usize {
        self.slots.len()
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    #[cfg(test)]
    pub fn recv(&mut self, seq: u64, data: Vec<u8>, frame_len: Option<u32>) -> bool {
        self.recv_bytes(SequenceNumber::from_wire(seq), &data, frame_len)
            .should_ack()
    }

    #[cfg(test)]
    fn recv_disposition(
        &mut self,
        seq: SequenceNumber,
        data: Vec<u8>,
        frame_len: Option<u32>,
    ) -> RecvDisposition {
        self.recv_bytes(seq, &data, frame_len)
    }

    pub(crate) fn recv_bytes(
        &mut self,
        seq: SequenceNumber,
        data: &[u8],
        frame_len: Option<u32>,
    ) -> RecvDisposition {
        if let Some(frame_len) = frame_len
            && !crate::delivery::frame::send::is_valid_frame_len(frame_len)
        {
            return RecvDisposition::Rejected;
        }
        if self.next.is_none() {
            return RecvDisposition::Rejected;
        }
        let reused_buf = &mut self.reused_buf;
        match self.slots.insert_vacant_with(seq, || {
            let mut owned = reused_buf.take();
            owned.extend(data);
            RecvSlot::Data(RecvPkt {
                data: owned,
                frame_len,
            })
        }) {
            Ok(()) => {}
            Err(SequenceVacancyError::Occupied)
            | Err(SequenceVacancyError::Outside(SequencePosition::Stale)) => {
                return RecvDisposition::Duplicate;
            }
            Err(SequenceVacancyError::Outside(_)) => return RecvDisposition::Rejected,
        }
        self.scan_start = min(self.scan_start, seq);
        self.ack_history.insert_in_window(seq);
        RecvDisposition::Inserted
    }

    pub fn pop_complete_frame(&mut self) -> Option<Vec<u8>> {
        let frame_bytes = crate::delivery::frame::recv::pop_complete_frame(
            &mut self.slots,
            &mut self.reused_buf,
            &mut self.scan_start,
        );
        // Collapse the head-tombstone prefix whether or not a frame was
        // found: the scan may have tombstoned abandoned frames (a hostile
        // peer's overlapping frames) with no complete frame to return, and
        // the cursor must still advance past them or the receiver wedges.
        self.collapse_tombstone_prefix();
        frame_bytes
    }

    /// Advance `next` past contiguous tombstone(s) at the head of the slot
    /// map, removing them.  This is what bounds memory: tombstones count
    /// toward the window until collapsed.  The slot window anchor moves with
    /// `next`, and `scan_start` is reset when it leaves the window.
    fn collapse_tombstone_prefix(&mut self) {
        let Some(mut next) = self.next else {
            return;
        };
        while let Some(RecvSlot::Tombstone) = self.slots.get(&next) {
            self.slots.remove(&next);
            next = next.advance(1);
        }
        self.next = Some(next);
        if self.slots.window().anchor() == next {
            return;
        }
        self.slots.advance_anchor(next);
        if next.forward_distance_to(self.scan_start) > crate::sequence::HALF_SEQUENCE_SPACE {
            self.scan_start = next;
        }
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
                    self.next = Some(next.advance(1));
                    self.collapse_tombstone_prefix();
                    return Some(pkt.data);
                }
                Some(RecvSlot::Tombstone) => {
                    self.next = Some(next.advance(1));
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    #[test]
    fn stock_recv_pop_in_order() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert!(space.recv(1, b"world".to_vec(), None));
        assert_eq!(space.peek().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
        assert!(space.pop().is_none());
    }

    #[test]
    fn stock_recv_out_of_order() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(1, b"world".to_vec(), None));
        assert!(space.recv(0, b"hello".to_vec(), None));
        assert_eq!(space.pop().unwrap(), b"hello");
        assert_eq!(space.pop().unwrap(), b"world");
    }

    #[test]
    fn stale_seq_is_acked_but_not_inserted() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"a".to_vec(), None));
        let ack_count_after = space.ack_history().blocks().count();
        space.pop();
        assert_eq!(
            space.recv_disposition(seq(0), b"stale".to_vec(), None),
            RecvDisposition::Duplicate
        );
        assert!(!space.slots.contains_key(&seq(0)));
        assert_eq!(space.ack_history().blocks().count(), ack_count_after);
    }

    #[test]
    fn window_capacity_rejects() {
        let mut space = PktRecvSpace::new();
        let far_seq = MAX_NUM_RECVING_PKTS as u64; // seq 0 is next, so seq 8192 is out of window
        assert!(!space.recv(far_seq, b"x".to_vec(), None));
    }

    #[test]
    fn duplicate_acked_but_not_reinserted() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"a".to_vec(), None));
        assert_eq!(
            space.recv_disposition(seq(0), b"b".to_vec(), None),
            RecvDisposition::Duplicate
        );
        let pkt = space.slots.get(&seq(0)).unwrap();
        match pkt {
            RecvSlot::Data(p) => assert_eq!(p.data, b"a"),
            _ => panic!("expected data"),
        }
    }

    #[test]
    fn fin_empty_payload_no_frame_len() {
        let mut space = PktRecvSpace::new();
        // FIN: empty payload, no frame_len.
        assert!(space.recv(0, vec![], None));
        assert_eq!(space.peek().unwrap().len(), 0);
        let popped = space.pop().unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn tombstone_prefix_collapses() {
        let mut space = PktRecvSpace::new();
        // Insert tombstones ahead of the cursor.
        space.slots.insert(seq(0), RecvSlot::Tombstone);
        space.slots.insert(seq(1), RecvSlot::Tombstone);
        space.slots.insert(
            seq(2),
            RecvSlot::Data(RecvPkt {
                data: b"hello".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(seq(0));
        space.collapse_tombstone_prefix();
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.pop().unwrap(), b"hello");
    }

    #[test]
    fn pop_skips_head_tombstones() {
        let mut space = PktRecvSpace::new();
        space.slots.insert(seq(0), RecvSlot::Tombstone);
        space.slots.insert(
            seq(1),
            RecvSlot::Data(RecvPkt {
                data: b"world".to_vec(),
                frame_len: None,
            }),
        );
        space.next = Some(seq(0));
        // First pop skips tombstone at 0.
        assert_eq!(space.pop().unwrap(), b"world");
        assert_eq!(space.next, Some(seq(2)));
    }

    // Frame delivery tests — enabled mode.

    #[test]
    fn ooo_complete_frame_delivers_past_sequence_hole() {
        let mut space = PktRecvSpace::new();
        // Hole at seq 0; deliver a complete 1-packet frame at seq 1 first.
        assert!(space.recv(1, b"frame1".to_vec(), Some(6)));
        let frame = space.pop_complete_frame().unwrap();
        assert_eq!(frame, b"frame1");
        // Frame was delivered; seq 1 is now a tombstone.
        assert!(matches!(
            space.slots.get(&seq(1)),
            Some(RecvSlot::Tombstone)
        ));
        // Now fill the hole at seq 0 to verify the window still works.
        assert!(space.recv(0, b"late".to_vec(), None));
        assert_eq!(space.pop().unwrap(), b"late");
    }

    #[test]
    fn multi_packet_frame_waits_for_all_its_packets() {
        let mut space = PktRecvSpace::new();
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
        let mut space = PktRecvSpace::new();
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
        assert_eq!(space.next, Some(seq(4)));
        assert_eq!(space.slots.len(), 0);
    }

    #[test]
    fn interrupted_frame_does_not_wedge_the_receiver() {
        let mut space = PktRecvSpace::new();
        // Frame A (seqs 0-3, frame_len 80) is interrupted by a hostile
        // frame-start at seq 3 (frame B, seqs 3-6): only A's first three
        // packets (60 bytes) have arrived, so A is incomplete when B's start
        // lands inside its range. B completes and is popped (tombstoning
        // 3-6); A's retransmissions of 3-5 are then swallowed by the
        // tombstones (occupied slots read as duplicates). The receiver must
        // tombstone A's collected seqs (0-2) so the in-order cursor advances
        // instead of wedging on A forever.
        assert!(space.recv(0, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), Some(80)));
        assert!(space.recv(1, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(2, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        assert!(space.recv(3, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), Some(80)));
        assert!(space.recv(4, b"EEEEEEEEEEEEEEEEEEEE".to_vec(), None));
        assert!(space.recv(5, b"FFFFFFFFFFFFFFFFFFFF".to_vec(), None));
        assert!(space.recv(6, b"GGGGGGGGGGGGGGGGGGGG".to_vec(), None));
        // B completes first (the scan restarts at the colliding frame-start).
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"DDDDDDDDDDDDDDDDDDDDEEEEEEEEEEEEEEEEEEEEFFFFFFFFFFFFFFFFFFFFGGGGGGGGGGGGGGGGGGGG"
        );
        // A's retransmissions of 3-5 are acked as duplicates (occupied
        // slots) but never inserted — they cannot repair A.
        assert!(space.recv(3, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), None));
        assert!(space.recv(4, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(5, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        // No complete frame remains, but the abandoned frame A (0-2) is
        // tombstoned and the cursor advances past 0-6 instead of wedging.
        assert!(space.pop_complete_frame().is_none());
        assert_eq!(space.next, Some(seq(7)));
    }

    #[test]
    fn legitimate_out_of_order_frames_still_reassemble_after_a_collision() {
        let mut space = PktRecvSpace::new();
        // Frame A (seqs 0-3, frame_len 80) and frame B (seqs 4-6, frame_len
        // 60) are contiguous from the sender's perspective, but B's packets
        // arrive before A's continuation at seq 3. The scan abandons A at
        // the collision with B (no tombstone inside A's range), B completes,
        // and A must still reassemble when its packet arrives at a vacant
        // slot.
        assert!(space.recv(0, b"AAAAAAAAAAAAAAAAAAAA".to_vec(), Some(80)));
        assert!(space.recv(1, b"BBBBBBBBBBBBBBBBBBBB".to_vec(), None));
        assert!(space.recv(2, b"CCCCCCCCCCCCCCCCCCCC".to_vec(), None));
        assert!(space.recv(4, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), Some(60)));
        assert!(space.recv(5, b"EEEEEEEEEEEEEEEEEEEE".to_vec(), None));
        assert!(space.recv(6, b"FFFFFFFFFFFFFFFFFFFF".to_vec(), None));
        // B completes first.
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"DDDDDDDDDDDDDDDDDDDDEEEEEEEEEEEEEEEEEEEEFFFFFFFFFFFFFFFFFFFF"
        );
        // A's continuation arrives at a vacant slot (B never overlapped A).
        assert!(space.recv(3, b"DDDDDDDDDDDDDDDDDDDD".to_vec(), None));
        // A reassembles: the tombstone at 4-6 is AFTER A's range, so the
        // contiguity run 0-3 is intact.
        assert_eq!(
            space.pop_complete_frame().unwrap(),
            b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBCCCCCCCCCCCCCCCCCCCCDDDDDDDDDDDDDDDDDDDD"
        );
        assert_eq!(space.next, Some(seq(7)));
    }

    #[test]
    fn frame_continuation_with_own_frame_len_starts_new_run() {
        let mut space = PktRecvSpace::new();
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
        let mut space = PktRecvSpace::new();
        // Insert packets out of order and verify ack_history is stock.
        assert!(space.recv(2, b"c".to_vec(), None));
        assert!(space.recv(0, b"a".to_vec(), None));
        assert!(space.recv(1, b"b".to_vec(), None));

        // ack_history should have folded the in-order run into the
        // cumulative front (no selective ranges remain).
        assert_eq!(space.ack_history().next(), seq(3));
        assert_eq!(space.ack_history().blocks().count(), 0);

        // After pop_complete_frame or pop, ack_history is NOT touched.
        space.pop();
        assert_eq!(space.ack_history().next(), seq(3));
        assert_eq!(space.ack_history().blocks().count(), 0);
    }

    #[test]
    fn pop_complete_frame_tombstones_count_toward_window() {
        let mut space = PktRecvSpace::new();
        // Deliver a frame at seq 1, leaving a hole at seq 0.
        assert!(space.recv(1, b"x".to_vec(), Some(1)));
        space.pop_complete_frame();
        // seq 1 is now a tombstone; seq 0 is a hole.
        assert!(matches!(
            space.slots.get(&seq(1)),
            Some(RecvSlot::Tombstone)
        ));
        assert!(!space.slots.contains_key(&seq(0)));
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
        let mut space = PktRecvSpace::new();
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
        let mut space = PktRecvSpace::new();
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

    #[test]
    fn a_frame_is_delivered_at_its_declared_length() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"ABCDEFGH".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ABCD");
    }

    #[test]
    fn a_multi_packet_frame_stops_at_its_declared_length() {
        let mut space = PktRecvSpace::new();
        assert!(space.recv(0, b"AB".to_vec(), Some(5)));
        assert!(space.recv(1, b"CDEFGH".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"ABCDE");
    }

    #[test]
    fn oversize_frame_len_cannot_pin_the_in_order_cursor() {
        let mut space = PktRecvSpace::new();
        assert_eq!(
            space.recv_disposition(seq(0), b"poison".to_vec(), Some(u32::MAX)),
            RecvDisposition::Rejected
        );
        assert!(space.recv(0, b"real".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"real");
        assert_eq!(space.next, Some(seq(1)));
        assert_eq!(space.slots.len(), 0);
    }

    #[test]
    fn zero_frame_len_is_rejected() {
        let mut space = PktRecvSpace::new();
        assert_eq!(
            space.recv_disposition(seq(0), vec![], Some(0)),
            RecvDisposition::Rejected
        );
        assert_eq!(space.slots.len(), 0);
        assert!(space.pop_complete_frame().is_none());
    }

    #[test]
    fn max_frame_len_is_still_accepted() {
        let mut space = PktRecvSpace::new();
        let max = crate::delivery::frame::send::MAX_FRAME_LEN;
        assert_eq!(
            space.recv_disposition(seq(0), vec![0u8; 1], Some(max as u32)),
            RecvDisposition::Inserted
        );
    }

    /// Fix #8 (inverse): `fin_behind_gap_does_not_surface_eof` — a FIN
    /// behind an out-of-order gap does NOT surface EOF until the gap fills.
    #[test]
    fn fin_behind_gap_does_not_surface_eof() {
        let mut space = PktRecvSpace::new();
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

    #[test]
    fn a_late_packet_below_the_scan_cursor_still_completes_its_frame() {
        let mut space = PktRecvSpace::new();
        for s in 2..64 {
            assert!(space.recv(s, b"x".to_vec(), Some(1)));
        }
        assert!(space.recv(1, b"lo".to_vec(), None));
        for _ in 2..64 {
            assert!(space.pop_complete_frame().is_some());
        }
        assert!(space.pop_complete_frame().is_none());
        assert!(space.recv(0, b"hel".to_vec(), Some(5)));
        assert_eq!(
            space.pop_complete_frame().as_deref(),
            Some(&b"hello"[..]),
            "the frame waiting on the late packet was never handed up"
        );
        space.collapse_tombstone_prefix();
        assert_eq!(
            space.next_seq(),
            Some(seq(64)),
            "the in-order cursor did not advance past the filled hole"
        );
    }

    #[test]
    fn receive_and_cumulative_ack_advance_across_u64_wrap() {
        // A receive space seeded just before the wrap: in-order receipts
        // advance both the in-order cursor and the ACK cumulative front
        // across u64::MAX → 0 without raw-order comparisons.
        let mut space = PktRecvSpace::new_at(seq(u64::MAX - 1));
        assert_eq!(space.next_seq(), Some(seq(u64::MAX - 1)));
        assert_eq!(space.ack_history().next(), seq(u64::MAX - 1));
        assert!(space.recv(u64::MAX - 1, b"a".to_vec(), None));
        assert!(space.recv(u64::MAX, b"b".to_vec(), None));
        assert!(space.recv(0, b"c".to_vec(), None));
        assert_eq!(
            space.ack_history().next(),
            seq(1),
            "the cumulative ACK front must fold the in-order run across the wrap"
        );
        assert_eq!(
            space.next_seq(),
            Some(seq(u64::MAX - 1)),
            "the in-order cursor advances on pop"
        );
        assert_eq!(space.pop().unwrap(), b"a");
        assert_eq!(space.pop().unwrap(), b"b");
        assert_eq!(space.pop().unwrap(), b"c");
        assert_eq!(
            space.next_seq(),
            Some(seq(1)),
            "the in-order cursor advanced across the wrap"
        );
        // Stale (pre-wrap) duplicates are still ACKable as Duplicate.
        assert_eq!(
            space.recv_disposition(seq(u64::MAX), b"dup".to_vec(), None),
            RecvDisposition::Duplicate
        );
    }

    #[test]
    fn frame_reassembly_crosses_u64_wrap_in_logical_order() {
        let mut space = PktRecvSpace::new_at(seq(u64::MAX - 1));
        // A two-packet frame straddling u64::MAX → 0: continuation must
        // follow the wrapped sequence, not raw order.
        assert!(space.recv(u64::MAX - 1, b"A1".to_vec(), Some(4)));
        assert!(space.recv(u64::MAX, b"A2".to_vec(), None));
        assert_eq!(space.pop_complete_frame().unwrap(), b"A1A2");
        // A frame wrapping the boundary itself.
        assert!(space.recv(1, b"B2".to_vec(), None));
        assert!(space.recv(0, b"B1".to_vec(), Some(4)));
        assert_eq!(space.pop_complete_frame().unwrap(), b"B1B2");
        assert_eq!(space.next, Some(seq(2)));
        assert_eq!(space.slots.len(), 0);
    }

    fn ooo_pop_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut space = PktRecvSpace::new();
            for s in 1..=outstanding {
                assert!(space.recv(s, b"x".to_vec(), Some(1)));
            }
            let start = std::time::Instant::now();
            for _ in 0..outstanding {
                assert!(space.pop_complete_frame().is_some());
            }
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    fn in_order_pop_cost(outstanding: u64) -> f64 {
        let mut best = f64::MAX;
        for _ in 0..3 {
            let mut space = PktRecvSpace::new();
            for s in 0..outstanding {
                assert!(space.recv(s, b"x".to_vec(), None));
            }
            let start = std::time::Instant::now();
            for _ in 0..outstanding {
                assert!(space.pop().is_some());
            }
            best = best.min(start.elapsed().as_secs_f64() / outstanding as f64 * 1e9);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/frame ratio; run with cargo test --release -- --ignored"]
    fn delivering_past_a_hole_costs_no_more_per_frame() {
        let few = ooo_pop_cost(64);
        let many = ooo_pop_cost(4096);
        eprintln!(
            "out-of-order frame delivery: {few:.1} ns/frame at 64 outstanding, {many:.1} ns/frame at 4096"
        );
        assert!(
            many < few * 8.0,
            "{many:.1} ns/frame at 4096 outstanding against {few:.1} ns at 64: the per-frame cost grows with the number of frames delivered past the hole"
        );
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/packet ratio; run with cargo test --release -- --ignored"]
    fn advancing_the_receive_window_costs_no_more_per_packet() {
        let few = in_order_pop_cost(64);
        let many = in_order_pop_cost(4096);
        eprintln!(
            "in-order receive-window advance: {few:.1} ns/packet at 64 outstanding, {many:.1} ns/packet at 4096"
        );
        assert!(
            many < few * 8.0,
            "{many:.1} ns/packet at 4096 outstanding against {few:.1} ns at 64: advancing the receive window scans the outstanding map"
        );
    }
}
