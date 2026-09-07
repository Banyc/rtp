//! Handshake datagram padding, sourced from the connection's MSS.
//!
//! Every handshake packet is sent with a random-length tail so the opening
//! exchange does not fingerprint as a run of tiny fixed-size datagrams. The
//! tail length is uniform in `0..=Mss::max_handshake_pad()`, so a handshake
//! datagram can be as large as a data datagram. All padding wire-format
//! logic lives here: a later format change touches only this module.

use super::wire::PACKET_LEN;

/// Padding tail layout: `[core 18][pad_len u16][padding]`.
pub(crate) const PAD_LEN_LEN: usize = 2;
pub(crate) const PAD_LEN_OFFSET: usize = PACKET_LEN;
pub(crate) const PADDED_HEADER_LEN: usize = PACKET_LEN + PAD_LEN_LEN;

/// A connection's MSS, typed so the padding bound cannot be confused with
/// an arbitrary size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mss(usize);

impl Mss {
    pub(crate) const fn new(mss: usize) -> Self {
        Self(mss)
    }

    /// Largest padding tail: the padded packet may reach a full MSS-sized
    /// datagram.
    pub(crate) const fn max_handshake_pad(self) -> usize {
        self.0.saturating_sub(PADDED_HEADER_LEN)
    }

    /// Largest padded handshake packet (the send and receive buffer size).
    pub(crate) const fn max_padded_len(self) -> usize {
        PADDED_HEADER_LEN + self.max_handshake_pad()
    }
}

/// Pad an 18-byte handshake packet with a random-length tail:
/// `[core][pad_len u16][random padding]`. Writes into `out` (which must
/// hold at least `mss.max_padded_len()` bytes) and returns the padded
/// length.
pub(crate) fn pad_handshake(core: &[u8], out: &mut [u8], mss: Mss) -> usize {
    debug_assert_eq!(core.len(), PACKET_LEN);
    debug_assert!(out.len() >= mss.max_padded_len());
    let max_pad = mss.max_handshake_pad();
    let pad_len = rand::random_range(0..=max_pad);
    out[..PACKET_LEN].copy_from_slice(core);
    out[PAD_LEN_OFFSET..PADDED_HEADER_LEN].copy_from_slice(&(pad_len as u16).to_be_bytes());
    // Zero-filled tail; the chacha20 keystream randomizes it on the wire.
    out[PADDED_HEADER_LEN..PADDED_HEADER_LEN + pad_len].fill(0);
    PADDED_HEADER_LEN + pad_len
}

/// Strip the padding tail from a received handshake datagram, returning the
/// 18-byte core. Accepts both the unpadded 18-byte form and the padded
/// `[core][pad_len u16][padding]` form; a padded packet whose length does
/// not match its declared `pad_len` is rejected.
pub(crate) fn strip_padding(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < PACKET_LEN {
        return None;
    }
    if bytes.len() == PACKET_LEN {
        return Some(bytes);
    }
    if bytes.len() < PADDED_HEADER_LEN {
        return None;
    }
    let pad_len =
        u16::from_be_bytes(bytes[PAD_LEN_OFFSET..PADDED_HEADER_LEN].try_into().ok()?) as usize;
    if bytes.len() != PADDED_HEADER_LEN + pad_len {
        return None;
    }
    Some(&bytes[..PACKET_LEN])
}

#[cfg(test)]
mod tests {
    use super::super::wire::{Kind, Packet};
    use super::*;

    fn core() -> [u8; PACKET_LEN] {
        Packet {
            kind: Kind::Ready,
            nonce: 0x0123_4567_89ab_cdef,
        }
        .encode()
    }

    #[test]
    fn pad_round_trips_through_strip() {
        let core = core();
        let mss = Mss::new(220);
        let mut padded = vec![0u8; mss.max_padded_len()];
        let n = pad_handshake(&core, &mut padded, mss);
        assert!(
            (PADDED_HEADER_LEN..=mss.max_padded_len()).contains(&n),
            "padded length {n} must be in [{}, {}]",
            PADDED_HEADER_LEN,
            mss.max_padded_len()
        );
        assert_eq!(padded[..PACKET_LEN], core);
        assert_eq!(
            u16::from_be_bytes(
                padded[PAD_LEN_OFFSET..PADDED_HEADER_LEN]
                    .try_into()
                    .unwrap()
            ) as usize,
            n - PADDED_HEADER_LEN,
            "the pad_len field must match the tail length"
        );
        assert_eq!(
            strip_padding(&padded[..n]),
            Some(core.as_slice()),
            "a padded packet must strip back to the core"
        );
        let mut other = vec![0u8; mss.max_padded_len()];
        let m = pad_handshake(&core, &mut other, mss);
        assert_ne!(
            &padded[..n],
            &other[..m],
            "two pads of the same core must differ"
        );
    }

    #[test]
    fn strip_accepts_unpadded_and_rejects_mismatched() {
        let core = core();
        assert_eq!(strip_padding(&core), Some(core.as_slice()));
        assert_eq!(strip_padding(&core[..PACKET_LEN - 1]), None);
        // 20 bytes with pad_len=1: claims one padding byte but none follow.
        let mut mismatched = core.to_vec();
        mismatched.extend_from_slice(&1u16.to_be_bytes());
        assert_eq!(strip_padding(&mismatched), None);
        // 20 bytes with pad_len=0 is a valid (zero-padded) packet.
        let mut zero_padded = core.to_vec();
        zero_padded.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(strip_padding(&zero_padded), Some(core.as_slice()));
    }

    #[test]
    fn pad_lengths_are_uniform() {
        const MAX_PAD: usize = 200;
        const SAMPLES: usize = 201_000;
        let mss = Mss::new(PADDED_HEADER_LEN + MAX_PAD);
        let mut counts = [0usize; MAX_PAD + 1];
        let core = core();
        let mut out = vec![0u8; mss.max_padded_len()];
        for _ in 0..SAMPLES {
            let n = pad_handshake(&core, &mut out, mss);
            counts[n - PADDED_HEADER_LEN] += 1;
        }
        let expected = SAMPLES as f64 / (MAX_PAD + 1) as f64;
        let chi2: f64 = counts
            .iter()
            .map(|&c| {
                let d = c as f64 - expected;
                d * d / expected
            })
            .sum();
        assert!(
            chi2 < 300.0,
            "pad lengths are not uniform: chi2 = {chi2:.1} (expected < 300 for 201 dof)"
        );
    }

    #[test]
    fn max_pad_tracks_mss() {
        assert_eq!(Mss::new(17).max_handshake_pad(), 0);
        assert_eq!(Mss::new(20).max_handshake_pad(), 0);
        assert_eq!(Mss::new(21).max_handshake_pad(), 1);
        assert_eq!(Mss::new(1424).max_handshake_pad(), 1404);
        assert_eq!(Mss::new(1424).max_padded_len(), 1424);
        assert_eq!(Mss::new(17).max_padded_len(), 20);
    }
}
