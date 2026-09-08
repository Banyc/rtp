//! Handshake datagram padding, sourced from the connection's MSS.
//!
//! Every handshake packet is sent with a random-length tail so the opening
//! exchange does not fingerprint as a run of tiny fixed-size datagrams. The
//! tail length is drawn from [`PaddingSettings`] with a uniform random
//! target over `PACKET_LEN..=Mss::max_padded_len()`, so a handshake
//! datagram can be as large as a data datagram. The payload size (the
//! 18-byte core) is known to both sides, so the padding uses the static
//! payload-sized mode: `[core][padding]` with no length field. All padding
//! wire-format logic lives in [`crate::obfuscate::padding`]; this module
//! only derives the settings from the MSS.

use super::wire::PACKET_LEN;
use crate::obfuscate::padding::{self, PaddingSettings, PayloadSized, TargetKind};

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
        self.0.saturating_sub(PACKET_LEN)
    }

    /// Largest padded handshake packet (the send and receive buffer size).
    pub(crate) const fn max_padded_len(self) -> usize {
        self.0
    }

    /// The handshake's padding settings: uniform random over the
    /// MSS-derived range, static payload-sized (the core size is known to
    /// both sides, so no length field rides on the wire).
    pub(crate) fn settings(self) -> PaddingSettings {
        PaddingSettings {
            target: TargetKind::Uniform {
                lo: PACKET_LEN,
                hi: PACKET_LEN + self.max_handshake_pad(),
            },
            payload_sized: PayloadSized::Static,
        }
    }
}

/// The handshake's decode settings: static payload-sized, so the decode
/// reads the first `PACKET_LEN` bytes and ignores the padding tail. The
/// target is irrelevant to the decode.
pub(crate) const HANDSHAKE_DECODE_SETTINGS: PaddingSettings = PaddingSettings {
    target: TargetKind::Fixed(0),
    payload_sized: PayloadSized::Static,
};

/// Pad an 18-byte handshake packet with a random-length tail:
/// `[core][random padding]` (static payload-sized). Writes into `out`
/// (which must hold at least `mss.max_padded_len()` bytes) and returns the
/// padded length.
pub(crate) fn pad_handshake(core: &[u8], out: &mut [u8], mss: Mss) -> usize {
    debug_assert_eq!(core.len(), PACKET_LEN);
    debug_assert!(out.len() >= mss.max_padded_len());
    padding::encode_plaintext(core, out, Some(mss.settings()))
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
    fn pad_round_trips_through_decode() {
        let core = core();
        let mss = Mss::new(220);
        let mut padded = vec![0u8; mss.max_padded_len()];
        let n = pad_handshake(&core, &mut padded, mss);
        assert!(
            (PACKET_LEN..=mss.max_padded_len()).contains(&n),
            "padded length {n} must be in [{}, {}]",
            PACKET_LEN,
            mss.max_padded_len()
        );
        assert_eq!(padded[..PACKET_LEN], core);
        // The static decode reads the known core size and ignores the tail.
        let mut decoded = [0u8; PACKET_LEN];
        let len =
            padding::decode_plaintext(&padded[..n], &mut decoded, Some(HANDSHAKE_DECODE_SETTINGS))
                .unwrap();
        assert_eq!(len, PACKET_LEN);
        assert_eq!(&decoded, &core);
        let mut other = vec![0u8; mss.max_padded_len()];
        let m = pad_handshake(&core, &mut other, mss);
        assert_ne!(
            &padded[..n],
            &other[..m],
            "two pads of the same core must differ"
        );
    }

    #[test]
    fn decode_accepts_unpadded_and_ignores_the_tail() {
        let core = core();
        let mut decoded = [0u8; PACKET_LEN];
        assert_eq!(
            padding::decode_plaintext(&core, &mut decoded, Some(HANDSHAKE_DECODE_SETTINGS)),
            Some(PACKET_LEN)
        );
        assert_eq!(&decoded, &core);
        assert_eq!(
            padding::decode_plaintext(
                &core[..PACKET_LEN - 1],
                &mut decoded,
                Some(HANDSHAKE_DECODE_SETTINGS)
            ),
            None
        );
        // Any tail is padding: the core size is known, so the tail is
        // ignored (the static payload-sized mode has no length field).
        let mut padded = core.to_vec();
        padded.extend_from_slice(&[0xAB; 7]);
        assert_eq!(
            padding::decode_plaintext(&padded, &mut decoded, Some(HANDSHAKE_DECODE_SETTINGS)),
            Some(PACKET_LEN)
        );
        assert_eq!(&decoded, &core);
    }

    #[test]
    fn pad_lengths_follow_the_uniform_draw() {
        const MAX_PAD: usize = 200;
        const SAMPLES: usize = 201_000;
        let mss = Mss::new(PACKET_LEN + MAX_PAD);
        let mut counts = [0usize; MAX_PAD + 1];
        let core = core();
        let mut out = vec![0u8; mss.max_padded_len()];
        for _ in 0..SAMPLES {
            let n = pad_handshake(&core, &mut out, mss);
            counts[n - PACKET_LEN] += 1;
        }
        // The draw is uniform over the MSS-derived range.
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
        assert_eq!(Mss::new(18).max_handshake_pad(), 0);
        assert_eq!(Mss::new(19).max_handshake_pad(), 1);
        assert_eq!(Mss::new(1424).max_handshake_pad(), 1406);
        assert_eq!(Mss::new(1424).max_padded_len(), 1424);
        assert_eq!(Mss::new(17).max_padded_len(), 17);
    }
}
