//! Per-session control-plane session tags for no-handshake transports.
//!
//! Connections opened WITHOUT the opening handshake (keyed-udp sessions,
//! mpudp sessions) get no tag from the handshake, so the codec's
//! [`crate::codec`] `require_tag` would no-op and untagged spoofed control
//! datagrams (`KILL`, `ACK`, `ECHO_TS`) would be honoured. When an
//! obfuscation key is configured it IS the shared secret both sides hold, so
//! each side derives the same per-session tag from it and the codec rejects
//! untagged / wrongly-tagged control datagrams as `Unauthenticated` (dropped,
//! not fatal).
//!
//! Derivation (the same splitmix64-style finalizer the opening handshake
//! uses for its nonce-derived tags):
//!
//! ```text
//! tag = splitmix64(fold(obfuscation_key) ^ splitmix64(fold(dispatch_key)))
//! ```
//!
//! where `fold` is a byte-wise mixer over the key material and the dispatch
//! key (keyed-udp only; mpudp folds just the obfuscation key) is its
//! wire-encoded form — the same bytes the listener demuxes on, so client and
//! server derive the same value from the same secrets. Mixing the dispatch
//! key in gives sessions sharing one obfuscation key distinct tags.
//!
//! When there is NO obfuscation key there is no shared secret, so the
//! deployment stays on legacy behaviour (`session_tag` stays `None`): a key
//! is required for control-plane authenticity.

use crate::obfuscate::KEY_LEN;

/// The splitmix64 finalizer: a bijective avalanche mixer over 64 bits.
fn splitmix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Fold `bytes` into a 64-bit word: byte `i` lands in byte `i % 8` of a
/// little-endian word, and the accumulator is re-mixed after every chunk so
/// the fold is avalanche-sensitive to every byte position.
fn fold_to_u64(bytes: &[u8]) -> u64 {
    let mut acc = 0x9e37_79b9_7f4a_7c15u64;
    for chunk in bytes.chunks(8) {
        let mut word = 0u64;
        for (i, &b) in chunk.iter().enumerate() {
            word |= u64::from(b) << (8 * i);
        }
        acc = splitmix64(acc ^ word);
    }
    acc
}

/// Derive the per-session control-plane tag from the obfuscation key and
/// (for keyed-udp sessions) the wire-encoded dispatch key.  Both peers know
/// both secrets for their session, so both derive the same tag.
pub(crate) fn control_plane_tag(
    obfuscation_key: [u8; KEY_LEN],
    dispatch_key: Option<&[u8]>,
) -> u64 {
    let obfuscation = fold_to_u64(&obfuscation_key);
    match dispatch_key {
        Some(bytes) => splitmix64(obfuscation ^ splitmix64(fold_to_u64(bytes))),
        None => splitmix64(obfuscation),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tags are a pure function of the two secrets: deterministic, and the
    /// dispatch key both changes the tag (the `Some` arm must actually mix
    /// it in) and separates sessions sharing one obfuscation key.
    #[test]
    fn tags_are_deterministic_and_dispatch_keys_separate_sessions() {
        let obf = [7u8; KEY_LEN];
        let other_obf = [8u8; KEY_LEN];
        let none = control_plane_tag(obf, None);
        let keyed_a = control_plane_tag(obf, Some(&[1, 2, 3]));
        let keyed_b = control_plane_tag(obf, Some(&[9, 9, 9]));
        assert_ne!(
            none, keyed_a,
            "mixing in a dispatch key must change the tag"
        );
        assert_ne!(
            keyed_a, keyed_b,
            "different dispatch keys must separate sessions sharing one obfuscation key"
        );
        assert_ne!(none, control_plane_tag(other_obf, None));
        assert_eq!(
            control_plane_tag(obf, Some(&[1, 2, 3])),
            keyed_a,
            "deterministic"
        );
        assert_eq!(control_plane_tag(obf, None), none, "deterministic");
    }

    /// The exact derivation is pinned so a refactor cannot silently change
    /// every session's control-plane tag: peers derive the same tag only by
    /// running the same function over the same secrets.
    #[test]
    fn the_tag_derivation_matches_the_recorded_vector() {
        let obf = [7u8; KEY_LEN];
        assert_eq!(control_plane_tag(obf, None), 0xe4e2_66e0_1171_34bb);
        assert_eq!(control_plane_tag(obf, Some(&[42])), 0x3fae_37ba_2cbe_e6d2);
    }
}
