//! Sender-side frame staging and frame-aligned packetization.

use std::collections::VecDeque;

use primitive::{ops::len::Len, queue::cap_queue::CapVecQueue};

/// Maximum application bytes in a single frame in frame-delivery mode.
/// Set equal to the stock `MAX_SEND_DATA_BUF_LEN` so a frame can occupy
/// the whole staging buffer.
pub(crate) const MAX_FRAME_LEN: usize = 64 * 1024;

const MAX_PENDING_FRAMES: usize = 1024;

#[derive(Debug, Clone)]
struct PendingFrame {
    total_len: usize,
    remaining: usize,
}

/// The next frame-aligned packet payload to emit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameChunk {
    pub(crate) take_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct FrameSendStage {
    data: CapVecQueue<u8>,
    pending_frames: VecDeque<PendingFrame>,
}

impl Default for FrameSendStage {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameSendStage {
    pub(crate) fn new() -> Self {
        Self {
            data: CapVecQueue::new_vec(MAX_FRAME_LEN),
            pending_frames: VecDeque::with_capacity(MAX_PENDING_FRAMES),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending_frames.is_empty()
    }

    pub(crate) fn pending_bytes(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn stage_frame(
        &mut self,
        frame: &[u8],
        soft_cap: usize,
    ) -> Result<(), std::io::ErrorKind> {
        validate_frame(frame)?;
        let pending_bytes = self.pending_bytes();
        if self.pending_frames.len() == MAX_PENDING_FRAMES {
            return Err(std::io::ErrorKind::WouldBlock);
        }
        let free_bytes = if pending_bytes == 0 {
            MAX_FRAME_LEN
        } else {
            soft_cap
                .saturating_sub(pending_bytes)
                .min(MAX_FRAME_LEN - pending_bytes)
        };
        if free_bytes < frame.len() {
            return Err(std::io::ErrorKind::WouldBlock);
        }
        self.data.batch_enqueue(frame);
        self.pending_frames.push_back(PendingFrame {
            total_len: frame.len(),
            remaining: frame.len(),
        });
        Ok(())
    }

    pub(crate) fn next_chunk_len(
        &self,
        first_pkt_max_payload: usize,
        normal_max_payload: usize,
    ) -> Option<FrameChunk> {
        let pf = self.pending_frames.front()?;
        let cap = if pf.remaining == pf.total_len {
            first_pkt_max_payload
        } else {
            normal_max_payload
        };
        Some(FrameChunk {
            take_bytes: pf.remaining.min(cap),
        })
    }

    /// Consume the chunk and return `Some(frame_len)` iff this is the first
    /// packet of its frame (caller emits `FRAME_DATA_TS`).
    pub(crate) fn pop_chunk(&mut self, chunk: FrameChunk, out: &mut Vec<u8>) -> Option<u32> {
        let take_bytes = chunk.take_bytes;
        let pf = self.pending_frames.front_mut().unwrap();
        assert!(take_bytes <= pf.remaining);
        let is_first = pf.remaining == pf.total_len;
        self.data.batch_dequeue_extend(take_bytes, out);
        pf.remaining -= take_bytes;
        let frame_done = pf.remaining == 0;
        let frame_len = is_first.then_some(pf.total_len as u32);
        if frame_done {
            self.pending_frames.pop_front();
        }
        frame_len
    }
}

/// Validate a frame for staging: rejects empty frames and frames larger than
/// [`MAX_FRAME_LEN`] with `InvalidInput`.
pub(crate) fn validate_frame(frame: &[u8]) -> Result<(), std::io::ErrorKind> {
    if frame.is_empty() {
        return Err(std::io::ErrorKind::InvalidInput);
    }
    if frame.len() > MAX_FRAME_LEN {
        return Err(std::io::ErrorKind::InvalidInput);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stage_admits_any_legal_frame_past_soft_cap() {
        let mut stage = FrameSendStage::new();
        let frame = vec![0u8; 10_000];
        stage.stage_frame(&frame, 100).unwrap();
        assert_eq!(stage.pending_bytes(), 10_000);
    }

    #[test]
    fn non_empty_stage_respects_soft_cap() {
        let mut stage = FrameSendStage::new();
        stage.stage_frame(&[1u8; 100], 1_000).unwrap();
        let err = stage.stage_frame(&[2u8; 1_000], 1_000).unwrap_err();
        assert_eq!(err, std::io::ErrorKind::WouldBlock);
        stage.stage_frame(&[3u8; 900], 1_000).unwrap();
    }

    #[test]
    fn pending_frame_count_is_bounded() {
        let mut stage = FrameSendStage::new();
        for _ in 0..MAX_PENDING_FRAMES {
            stage.stage_frame(&[1], usize::MAX).unwrap();
        }
        assert_eq!(
            stage.stage_frame(&[1], usize::MAX).unwrap_err(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(stage.pending_bytes(), MAX_PENDING_FRAMES);
    }

    #[test]
    fn pending_payload_bytes_are_bounded() {
        let mut stage = FrameSendStage::new();
        stage
            .stage_frame(&vec![0; MAX_FRAME_LEN], usize::MAX)
            .unwrap();
        assert_eq!(
            stage.stage_frame(&[1], usize::MAX).unwrap_err(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(stage.pending_bytes(), MAX_FRAME_LEN);
    }

    #[test]
    fn rejects_empty_and_oversize_frames() {
        let mut stage = FrameSendStage::new();
        assert_eq!(
            stage.stage_frame(&[], 1_000).unwrap_err(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            stage
                .stage_frame(&vec![0u8; MAX_FRAME_LEN + 1], 1_000)
                .unwrap_err(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn packetization_is_frame_aligned_and_marks_first_packet() {
        let mut stage = FrameSendStage::new();
        stage.stage_frame(&[1u8; 10], usize::MAX).unwrap();
        stage.stage_frame(&[2u8; 3], usize::MAX).unwrap();

        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 4);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), Some(10));
        assert_eq!(out, [1u8; 4]);

        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 6);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), None);
        assert_eq!(out, [1u8; 6]);

        let chunk = stage.next_chunk_len(4, 6).unwrap();
        assert_eq!(chunk.take_bytes, 3);
        let mut out = Vec::new();
        assert_eq!(stage.pop_chunk(chunk, &mut out), Some(3));
        assert_eq!(out, [2u8; 3]);

        assert!(stage.is_empty());
        assert!(stage.next_chunk_len(4, 6).is_none());
    }
}
