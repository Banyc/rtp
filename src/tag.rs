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
