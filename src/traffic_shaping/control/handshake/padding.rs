//! Handshake datagram padding, sourced from the connection's MSS.
//!
//! Every handshake packet is sent with a random-length tail so the opening
//! exchange does not fingerprint as a run of tiny fixed-size datagrams. The
//! tail length is drawn from [`PaddingSettings`] with a uniform random
//! target over `PACKET_LEN..=mss`, so a handshake datagram can be as large
//! as a data datagram. The payload size (the 18-byte core) is known to both
//! sides, so the padding uses the static payload-sized mode: `[core]
//! [padding]` with no length field. All padding wire-format logic lives in
//! [`crate::obfuscate::padding`]; this module only derives the settings
//! from the MSS value.

use super::wire::PACKET_LEN;
use crate::mss::Mss;
use crate::obfuscate::padding::{self, PaddingSettings, TargetKind};

/// Largest padding tail for an MSS: the padded packet may reach a full
/// MSS-sized datagram minus the truncated-datagram detection headroom (see
/// [`crate::mss::Mss::max_datagram_size`]).
pub(crate) const fn max_handshake_pad(mss: Mss) -> usize {
    mss.max_datagram_size().saturating_sub(PACKET_LEN)
}

/// The handshake's padding settings for an MSS: uniform random over the
/// MSS-derived range, static payload-sized (the core size is known to both
/// sides, so no length field rides on the wire).
pub(crate) fn handshake_settings(mss: Mss) -> PaddingSettings {
    PaddingSettings::static_target(TargetKind::Uniform {
        lo: PACKET_LEN,
        hi: PACKET_LEN + max_handshake_pad(mss),
    })
}

/// The handshake's decode settings: static payload-sized, so the decode
/// reads the first `PACKET_LEN` bytes and ignores the padding tail. The
/// target is irrelevant to the decode.
pub(crate) const HANDSHAKE_DECODE_SETTINGS: PaddingSettings =
    PaddingSettings::static_target(TargetKind::Fixed(0));

/// Pad an 18-byte handshake packet with a random-length tail:
/// `[core][random padding]` (static payload-sized). Writes into `out`
/// (which must hold at least `mss.get()` bytes) and returns the padded
/// length.
pub(crate) fn pad_handshake(core: &[u8], out: &mut [u8], mss: Mss) -> usize {
    debug_assert_eq!(core.len(), PACKET_LEN);
    debug_assert!(out.len() >= mss.get());
    padding::encode_plaintext(core, out, Some(handshake_settings(mss)))
        .expect("handshake core fits in u16")
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
        let mss = Mss::try_new(220).unwrap();
        let mut padded = vec![0u8; mss.get()];
        let n = pad_handshake(&core, &mut padded, mss);
        assert!(
            (PACKET_LEN..=mss.get()).contains(&n),
            "padded length {n} must be in [{}, {}]",
            PACKET_LEN,
            mss.get()
        );
        assert_eq!(padded[..PACKET_LEN], core);
        // The static decode reads the known core size and ignores the tail.
        let mut decoded = [0u8; PACKET_LEN];
        let len =
            padding::decode_plaintext(&padded[..n], &mut decoded, Some(HANDSHAKE_DECODE_SETTINGS))
                .unwrap();
        assert_eq!(len, PACKET_LEN);
        assert_eq!(&decoded, &core);
        let mut other = vec![0u8; mss.get()];
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
        // The padded packet may reach `max_datagram_size` (the MSS minus the
        // truncated-datagram detection headroom), so the MSS must be one
        // byte larger than `PACKET_LEN + MAX_PAD` for the pad range to span
        // `[0, MAX_PAD]`.
        const MAX_PAD: usize = 199;
        const SAMPLES: usize = 200_000;
        let mss = Mss::try_new(PACKET_LEN + MAX_PAD + 1).unwrap();
        let mut counts = [0usize; MAX_PAD + 1];
        let core = core();
        let mut out = vec![0u8; mss.get()];
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
            "pad lengths are not uniform: chi2 = {chi2:.1} (expected < 300 for 200 dof)"
        );
    }

    #[test]
    fn max_pad_tracks_mss() {
        assert_eq!(max_handshake_pad(Mss::try_new(17).unwrap()), 0);
        assert_eq!(max_handshake_pad(Mss::try_new(18).unwrap()), 0);
        assert_eq!(max_handshake_pad(Mss::try_new(19).unwrap()), 0);
        assert_eq!(max_handshake_pad(Mss::try_new(20).unwrap()), 1);
        assert_eq!(max_handshake_pad(Mss::try_new(1424).unwrap()), 1405);
    }
}
