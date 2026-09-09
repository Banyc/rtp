//! The obfuscated-plaintext padding format. The wire format is
//! `[len u16][payload][padding]` when the payload size is unknown to the
//! receiver (dynamic), `[payload][padding]` when it is known (static), and
//! `[payload]` when unpadded. The real payload length rides inside the
//! ciphertext (dynamic) or is implied by the protocol (static); the
//! padding is zero-filled and the chacha20 keystream randomizes it on the
//! wire.
//!
//! All padding options (the target policy and the payload-sized mode) and
//! actions (draw, encode, decode) live here; every user in the crate
//! passes a [`PaddingSettings`] to the encode/decode functions.

/// The length-prefix size (u16) inside the obfuscated plaintext.
pub(crate) const LEN_LEN: usize = 2;

/// How the target plaintext size is chosen for each datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    /// Pad every datagram to exactly this size.
    Fixed(usize),
    /// Pad every datagram to a uniform random size in `[lo, hi]`.
    Uniform { lo: usize, hi: usize },
    /// Pad every datagram to a triangular random size peaked at `mode`,
    /// falling to zero at `mode ± spread`.
    Triangular { mode: usize, spread: usize },
}

/// Whether the payload size is known to the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSized {
    /// Known payload size: the wire format is `[payload][padding]` with
    /// no length field.
    Static,
    /// Unknown payload size: the wire format is `[len u16][payload]
    /// [padding]` with a length field.
    Dynamic,
}

/// The DPI-hiding padding policy for the obfuscation layer: how datagrams
/// are padded to hide the protocol's shape from a passive observer. One
/// three-variant choice replaces the old `padding_profile` + `ack_padding`
/// pair, so the invalid combinations (a profile AND fitted ACK padding at
/// once) are unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HarmfulPaddingPolicy {
    /// No padding: every datagram goes out at its natural size.
    #[default]
    None,
    /// Pad every datagram (data and ACK) to a fixed plaintext size.
    AllFixed(usize),
    /// Pad standalone ACK datagrams to a triangular-random target fitted
    /// from recent sent data-packet sizes, hiding them among data packets.
    /// Data packets stay at their natural size.
    AckMimicsData,
}

impl HarmfulPaddingPolicy {
    /// Resolve the policy into its two consumers: the obfuscation wrapper's
    /// padding settings (the profile it pads every datagram with) and the
    /// write half's fitted-ACK-padding toggle. Exactly one of the two is
    /// ever active: `AllFixed` sets the profile, `AckMimicsData` sets the
    /// toggle, `None` sets neither.
    pub(crate) fn resolve(self) -> (Option<PaddingSettings>, bool) {
        match self {
            HarmfulPaddingPolicy::None => (None, false),
            HarmfulPaddingPolicy::AllFixed(size) => (
                Some(PaddingSettings::dynamic_target(TargetKind::Fixed(size))),
                false,
            ),
            HarmfulPaddingPolicy::AckMimicsData => (None, true),
        }
    }
}

/// The padding settings for one encode/decode: the target policy and the
/// payload-sized mode. The fields are private: the payload-sized mode is a
/// per-channel invariant (the data channel's receiver never knows the
/// payload size, so it must be dynamic; the handshake's core size is known,
/// so it is static), and the constructors make the invalid combinations
/// unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaddingSettings {
    target: TargetKind,
    payload_sized: PayloadSized,
}

impl PaddingSettings {
    /// Dynamic payload-sized settings: the payload size rides in a u16
    /// prefix. The data channel's receiver never knows the payload size (the
    /// reliable layer's segments vary), so this is the only public
    /// constructor — a static data-channel setting is unrepresentable. Also
    /// used by the probe channel and the probe echo.
    pub const fn dynamic_target(target: TargetKind) -> Self {
        Self {
            target,
            payload_sized: PayloadSized::Dynamic,
        }
    }

    /// Static payload-sized settings: the payload size is known to the
    /// receiver (the decode buffer is exactly the payload size). Used by the
    /// handshake, whose 18-byte core is known to both sides.
    pub(crate) const fn static_target(target: TargetKind) -> Self {
        Self {
            target,
            payload_sized: PayloadSized::Static,
        }
    }

    /// Draw a target size for one datagram: the fixed size, a uniform draw
    /// in `[lo, hi]`, or a triangular draw peaked at `mode` (the difference
    /// of two uniforms is triangular).
    pub fn draw(&self) -> usize {
        match self.target {
            TargetKind::Fixed(size) => size,
            TargetKind::Uniform { lo, hi } => {
                debug_assert!(lo <= hi, "uniform padding range must not be inverted");
                rand::random_range(lo..=hi)
            }
            TargetKind::Triangular { mode, spread } => {
                let u = rand::random_range(0..=spread) as isize;
                let v = rand::random_range(0..=spread) as isize;
                // Clamped at zero: when `mode < spread` the lower tail is
                // truncated (the draw is still bounded by `mode + spread`).
                (mode as isize + u - v).max(0) as usize
            }
        }
    }

    /// The largest target size the settings can draw.
    pub fn max(&self) -> usize {
        match self.target {
            TargetKind::Fixed(size) => size,
            TargetKind::Uniform { hi, .. } => hi,
            TargetKind::Triangular { mode, spread } => mode + spread,
        }
    }
}

/// The largest plaintext a datagram can carry for a caller buffer of
/// `plaintext_capacity`: the settings' max target (when padding) or the
/// buffer alone (when not). Callers size their buffers to this.
pub(crate) fn max_plaintext(plaintext_capacity: usize, settings: Option<PaddingSettings>) -> usize {
    match settings {
        Some(settings) => {
            let payload_plus_header = match settings.payload_sized {
                PayloadSized::Dynamic => plaintext_capacity + LEN_LEN,
                PayloadSized::Static => plaintext_capacity,
            };
            settings.max().max(payload_plus_header)
        }
        None => plaintext_capacity,
    }
}

/// Encode `payload` as the obfuscated plaintext into `out` (which must be
/// sized to at least [`max_plaintext`]): `[len u16][payload][zero padding]`
/// (dynamic) or `[payload][zero padding]` (static), padded to a drawn
/// target (floored at the payload, never shrunk) when settings are given,
/// or the payload alone otherwise. Returns the plaintext length.
pub(crate) fn encode_plaintext(
    payload: &[u8],
    out: &mut [u8],
    settings: Option<PaddingSettings>,
) -> usize {
    match settings {
        Some(settings) => {
            let target = settings.draw();
            let header_len = match settings.payload_sized {
                PayloadSized::Dynamic => LEN_LEN,
                PayloadSized::Static => 0,
            };
            let plaintext_len = target.max(payload.len() + header_len);
            let mut pos = 0;
            if settings.payload_sized == PayloadSized::Dynamic {
                debug_assert!(
                    payload.len() <= u16::MAX as usize,
                    "the u16 length prefix cannot carry a payload larger than 65535 bytes"
                );
                out[..LEN_LEN].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                pos = LEN_LEN;
            }
            out[pos..pos + payload.len()].copy_from_slice(payload);
            // Zero the padding tail explicitly (the buffer is reused, so
            // stale bytes from a previous send must not leak into the
            // padding).
            out[pos + payload.len()..plaintext_len].fill(0);
            plaintext_len
        }
        None => {
            out[..payload.len()].copy_from_slice(payload);
            payload.len()
        }
    }
}

/// Read the u16 length prefix from the front of `plaintext`. Returns `None`
/// when the plaintext is shorter than the prefix.
fn read_len_prefix(plaintext: &[u8]) -> Option<usize> {
    if plaintext.len() < LEN_LEN {
        return None;
    }
    Some(u16::from_be_bytes([plaintext[0], plaintext[1]]) as usize)
}

/// Decode the obfuscated plaintext `plaintext` into `buf`: read the length
/// prefix (dynamic), strip the padding, and copy the payload out. Returns
/// the payload length, or `None` when the plaintext is not valid (shorter
/// than the length prefix, or the payload is too large for `buf`).
pub(crate) fn decode_plaintext(
    plaintext: &[u8],
    buf: &mut [u8],
    settings: Option<PaddingSettings>,
) -> Option<usize> {
    match settings {
        Some(settings) => match settings.payload_sized {
            PayloadSized::Dynamic => {
                let len = read_len_prefix(plaintext)?;
                if len > buf.len() || LEN_LEN + len > plaintext.len() {
                    return None;
                }
                buf[..len].copy_from_slice(&plaintext[LEN_LEN..LEN_LEN + len]);
                Some(len)
            }
            PayloadSized::Static => {
                // The payload size is known: the caller's buffer is the
                // payload size.
                if plaintext.len() < buf.len() {
                    return None;
                }
                buf.copy_from_slice(&plaintext[..buf.len()]);
                Some(buf.len())
            }
        },
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
/// prefix (dynamic), strip the padding, and move the payload to the front.
/// Returns the payload length, or `None` when the plaintext is not valid.
pub(crate) fn decode_plaintext_in_place(
    buf: &mut [u8],
    n: usize,
    settings: Option<PaddingSettings>,
) -> Option<usize> {
    match settings {
        Some(settings) => match settings.payload_sized {
            PayloadSized::Dynamic => {
                let len = read_len_prefix(&buf[..n])?;
                if LEN_LEN + len > n {
                    return None;
                }
                buf.copy_within(LEN_LEN..LEN_LEN + len, 0);
                Some(len)
            }
            PayloadSized::Static => {
                // The payload size is known: the caller's buffer is the
                // payload size.
                if n < buf.len() {
                    return None;
                }
                Some(buf.len())
            }
        },
        None => Some(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triangular() -> PaddingSettings {
        PaddingSettings::dynamic_target(TargetKind::Triangular {
            mode: 200,
            spread: 50,
        })
    }

    fn uniform() -> PaddingSettings {
        PaddingSettings::dynamic_target(TargetKind::Uniform { lo: 150, hi: 250 })
    }

    #[test]
    fn encode_decode_round_trips_with_and_without_settings() {
        let payload = b"hello";
        for settings in [
            None,
            Some(triangular()),
            Some(PaddingSettings::dynamic_target(TargetKind::Fixed(300))),
            Some(PaddingSettings::static_target(TargetKind::Fixed(300))),
        ] {
            let mut plaintext = vec![0u8; max_plaintext(payload.len(), settings)];
            let n = encode_plaintext(payload, &mut plaintext, settings);
            // Static mode treats the decode buffer as the payload size, so
            // decode into a buffer of exactly the payload size.
            let mut buf = vec![0u8; payload.len()];
            assert_eq!(
                decode_plaintext(&plaintext[..n], &mut buf, settings),
                Some(payload.len())
            );
            assert_eq!(&buf[..payload.len()], payload);
        }
    }

    #[test]
    fn a_profile_pads_to_the_target_and_never_shrinks() {
        let settings = triangular();
        let payload = b"tiny";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(settings))];
        let n = encode_plaintext(payload, &mut plaintext, Some(settings));
        assert!(
            (150..=250).contains(&n),
            "a triangular draw must land in the band, got {n}"
        );
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
        let mut out = vec![0u8; max_plaintext(big.len(), Some(settings))];
        let n = encode_plaintext(&big, &mut out, Some(settings));
        assert_eq!(n, big.len() + LEN_LEN);
    }

    #[test]
    fn a_fixed_profile_pads_every_datagram_to_the_same_size() {
        let settings = PaddingSettings::dynamic_target(TargetKind::Fixed(250));
        let payload = b"tiny";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(settings))];
        for _ in 0..16 {
            let n = encode_plaintext(payload, &mut plaintext, Some(settings));
            assert_eq!(n, 250, "every datagram must pad to the fixed size");
        }
        assert_eq!(&plaintext[..LEN_LEN], &(payload.len() as u16).to_be_bytes());
        assert_eq!(&plaintext[LEN_LEN..LEN_LEN + payload.len()], payload);
        assert!(
            plaintext[LEN_LEN + payload.len()..250]
                .iter()
                .all(|&b| b == 0)
        );
        // A payload larger than the fixed size is sent at its natural size
        // (plus the length prefix) — never shrunk.
        let big = vec![0xAB; 300];
        let mut out = vec![0u8; max_plaintext(big.len(), Some(settings))];
        let n = encode_plaintext(&big, &mut out, Some(settings));
        assert_eq!(n, big.len() + LEN_LEN);
    }

    #[test]
    fn static_mode_carries_no_length_field() {
        let settings = PaddingSettings::static_target(TargetKind::Fixed(250));
        let payload = b"hello";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(settings))];
        let n = encode_plaintext(payload, &mut plaintext, Some(settings));
        assert_eq!(n, 250);
        // The payload is at the front with no length prefix.
        assert_eq!(&plaintext[..payload.len()], payload);
        assert!(
            plaintext[payload.len()..n].iter().all(|&b| b == 0),
            "the padding tail must be zero-filled"
        );
        // The decode reads the known payload size from the front.
        let mut buf = [0u8; 5];
        assert_eq!(
            decode_plaintext(&plaintext[..n], &mut buf, Some(settings)),
            Some(payload.len())
        );
        assert_eq!(&buf, payload);
        // A plaintext shorter than the known payload size is rejected.
        assert_eq!(
            decode_plaintext(&plaintext[..4], &mut buf, Some(settings)),
            None
        );
    }

    #[test]
    fn decode_rejects_an_invalid_plaintext() {
        let payload = b"hello";
        let mut buf = [0u8; 4];
        // Without settings, a payload larger than the buffer is rejected.
        let mut plaintext = vec![0u8; payload.len()];
        plaintext.copy_from_slice(payload);
        assert_eq!(decode_plaintext(&plaintext, &mut buf, None), None);
        // With dynamic settings, a plaintext shorter than the length prefix
        // is rejected.
        assert_eq!(
            decode_plaintext(&[0x00], &mut buf, Some(triangular())),
            None
        );
    }

    #[test]
    fn in_place_decode_moves_the_payload_to_the_front() {
        let settings = triangular();
        let payload = b"hello";
        let mut plaintext = vec![0u8; max_plaintext(payload.len(), Some(settings))];
        let n = encode_plaintext(payload, &mut plaintext, Some(settings));
        let len = decode_plaintext_in_place(&mut plaintext, n, Some(settings)).unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(&plaintext[..len], payload);
    }

    #[test]
    fn uniform_and_triangular_draws_cover_the_band() {
        for settings in [uniform(), triangular()] {
            let mut sizes = std::collections::HashSet::new();
            for _ in 0..256 {
                sizes.insert(settings.draw());
            }
            assert!(
                sizes.iter().all(|&n| (150..=250).contains(&n)),
                "draws must stay in the band, got {sizes:?}"
            );
            assert!(sizes.len() > 1, "draws must vary, got {sizes:?}");
        }
    }
}
