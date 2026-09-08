//! The obfuscated-plaintext padding format: `[len u16][payload][padding]`
//! when a padding profile is set, `[payload]` otherwise. The real payload
//! length rides inside the ciphertext so a datagram can be padded to a
//! target size without a wire length field; the padding is zero-filled and
//! the chacha20 keystream randomizes it on the wire.

/// The length-prefix size (u16) inside the obfuscated plaintext.
pub(crate) const LEN_LEN: usize = 2;

/// A fixed single-mode target profile for datagram padding: every datagram
/// is padded to a size drawn from a triangular distribution peaked at
/// `mode` and falling to zero at `mode ± spread`. The draw is independent of
/// the traffic, so the wire size distribution converges to one mode without
/// any traffic-correlated feedback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetProfile {
    pub mode: usize,
    pub spread: usize,
}

impl TargetProfile {
    /// Draw a target size: triangular around `mode` in `[mode - spread,
    /// mode + spread]` (the difference of two uniforms is triangular).
    pub fn draw(&self) -> usize {
        let u = rand::random_range(0..=self.spread) as isize;
        let v = rand::random_range(0..=self.spread) as isize;
        (self.mode as isize + u - v).max(0) as usize
    }

    /// The largest target size the profile can draw.
    pub fn max(&self) -> usize {
        self.mode + self.spread
    }
}

/// The padding decision for one plaintext: the profile (policy) and the
/// target plaintext size to pad to (drawn by the caller). `profile` is
/// `None` for the historical unpadded format.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PadSettings {
    pub profile: Option<TargetProfile>,
    pub target: usize,
}

/// The largest plaintext a datagram can carry for a caller buffer of
/// `plaintext_capacity`: the profile's max target (when padding) or the
/// buffer alone (when not). Callers size their buffers to this.
pub(crate) fn max_plaintext(plaintext_capacity: usize, profile: Option<TargetProfile>) -> usize {
    match profile {
        Some(profile) => profile.max().max(plaintext_capacity + LEN_LEN),
        None => plaintext_capacity,
    }
}

/// Encode `payload` as the obfuscated plaintext into `out` (which must be
/// sized to at least [`max_plaintext`]): `[len u16][payload][zero padding]`
/// padded to the target (floored at the payload, never shrunk) when a
/// profile is set, or the payload alone otherwise. Returns the plaintext
/// length.
pub(crate) fn encode_plaintext(payload: &[u8], out: &mut [u8], settings: PadSettings) -> usize {
    let plaintext_len = match settings.profile {
        Some(_) => settings.target.max(payload.len() + LEN_LEN),
        None => payload.len(),
    };
    let mut pos = 0;
    if settings.profile.is_some() {
        out[..LEN_LEN].copy_from_slice(&(payload.len() as u16).to_be_bytes());
        pos = LEN_LEN;
    }
    out[pos..pos + payload.len()].copy_from_slice(payload);
    // Zero the padding tail explicitly (the buffer is reused, so stale bytes
    // from a previous send must not leak into the padding).
    out[pos + payload.len()..plaintext_len].fill(0);
    plaintext_len
}

/// Decode the obfuscated plaintext `plaintext` into `buf`: read the length
/// prefix (when padding), strip the padding, and copy the payload out.
/// Returns the payload length, or `None` when the plaintext is not valid
/// (shorter than the length prefix, or the payload is too large for `buf`).
pub(crate) fn decode_plaintext(
    plaintext: &[u8],
    buf: &mut [u8],
    profile: Option<TargetProfile>,
) -> Option<usize> {
    match profile {
        Some(_) => {
            if plaintext.len() < LEN_LEN {
                return None;
            }
            let len = u16::from_be_bytes(plaintext[..LEN_LEN].try_into().unwrap()) as usize;
            if len > buf.len() || LEN_LEN + len > plaintext.len() {
                return None;
            }
            buf[..len].copy_from_slice(&plaintext[LEN_LEN..LEN_LEN + len]);
            Some(len)
        }
        None => {
            if plaintext.len() > buf.len() {
                return None;
            }
            buf[..plaintext.len()].copy_from_slice(plaintext);
            Some(plaintext.len())
        }
    }
}

/// Decode the obfuscated plaintext in `buf[..n]` in place: read the length
/// prefix (when padding), strip the padding, and move the payload to the
/// front. Returns the payload length, or `None` when the plaintext is not
/// valid.
pub(crate) fn decode_plaintext_in_place(
    buf: &mut [u8],
    n: usize,
    profile: Option<TargetProfile>,
) -> Option<usize> {
    match profile {
        Some(_) => {
            if n < LEN_LEN {
                return None;
            }
            let len = u16::from_be_bytes(buf[..LEN_LEN].try_into().unwrap()) as usize;
            if LEN_LEN + len > n {
                return None;
            }
            buf.copy_within(LEN_LEN..LEN_LEN + len, 0);
            Some(len)
        }
        None => Some(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> TargetProfile {
        TargetProfile {
            mode: 200,
            spread: 50,
        }
    }

    #[test]
    fn encode_decode_round_trips_with_and_without_a_profile() {
        let payload = b"hello";
        let mut buf = [0u8; 512];
        for (profile, target) in [(None, 0), (Some(profile()), 200)] {
            let settings = PadSettings { profile, target };
            let mut plaintext = vec![0u8; max_plaintext(payload.len(), profile)];
            let n = encode_plaintext(payload, &mut plaintext, settings);
            assert_eq!(
                decode_plaintext(&plaintext[..n], &mut buf, profile),
                Some(payload.len())
            );
            assert_eq!(&buf[..payload.len()], payload);
        }
    }

    #[test]
    fn a_profile_pads_to_the_target_and_never_shrinks() {
        let profile = profile();
        let payload = b"tiny";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(profile))];
        let n = encode_plaintext(
            payload,
            &mut plaintext,
            PadSettings {
                profile: Some(profile),
                target: 200,
            },
        );
        assert_eq!(n, 200);
        assert_eq!(&plaintext[..LEN_LEN], &(payload.len() as u16).to_be_bytes());
        assert_eq!(&plaintext[LEN_LEN..LEN_LEN + payload.len()], payload);
        assert!(
            plaintext[LEN_LEN + payload.len()..n]
                .iter()
                .all(|&b| b == 0)
        );
        // A payload larger than the target is sent at its natural size (plus
        // the length prefix) — never shrunk.
        let big = vec![0xAB; 300];
        let mut out = vec![0u8; max_plaintext(big.len(), Some(profile))];
        let n = encode_plaintext(
            &big,
            &mut out,
            PadSettings {
                profile: Some(profile),
                target: 200,
            },
        );
        assert_eq!(n, big.len() + LEN_LEN);
    }

    #[test]
    fn decode_rejects_an_invalid_plaintext() {
        let payload = b"hello";
        let mut buf = [0u8; 4];
        // Without a profile, a payload larger than the buffer is rejected.
        let mut plaintext = vec![0u8; payload.len()];
        plaintext.copy_from_slice(payload);
        assert_eq!(decode_plaintext(&plaintext, &mut buf, None), None);
        // With a profile, a plaintext shorter than the length prefix is
        // rejected.
        assert_eq!(decode_plaintext(&[0x00], &mut buf, Some(profile())), None);
    }

    #[test]
    fn in_place_decode_moves_the_payload_to_the_front() {
        let profile = profile();
        let payload = b"hello";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(profile))];
        let n = encode_plaintext(
            payload,
            &mut plaintext,
            PadSettings {
                profile: Some(profile),
                target: 200,
            },
        );
        let len = decode_plaintext_in_place(&mut plaintext, n, Some(profile)).unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(&plaintext[..len], payload);
    }
}
