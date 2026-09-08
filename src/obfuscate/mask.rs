//! The masking layer: the nonce scheme and the chacha20 keystream that
//! hides a datagram's plaintext. The wire shape is `[nonce 24][ciphertext]`
//! — no authentication tag — so a passive observer sees only random bytes
//! and cannot distinguish the traffic from any other encrypted UDP protocol
//! (QUIC, WireGuard, DTLS). The keystream covers the plaintext as-given; the
//! plaintext format (length prefix + padding) is composed by the wrapper
//! ([`super::wrapper`]) which is the only layer that touches both masking
//! and padding.

use tokio_chacha20::cipher::StreamCipher;

/// The nonce length: 24 bytes (XChaCha20).
pub(crate) const NONCE_LEN: usize = tokio_chacha20::X_NONCE_BYTES;

/// The chacha20 key length: 32 bytes.
pub(crate) const KEY_LEN: usize = tokio_chacha20::KEY_BYTES;

/// Apply the chacha20 keystream for `key`/`nonce` to `buf` in place.
/// Encryption and decryption are the same XOR operation, so this single
/// helper serves the wrapper halves and the obfuscated path-probe
/// side channel (which must use the same wire shape so a passive observer
/// cannot tell probes from data).
pub(crate) fn apply_keystream(key: [u8; KEY_LEN], nonce: [u8; NONCE_LEN], buf: &mut [u8]) {
    let mut cipher = StreamCipher::new_x(key, nonce);
    cipher.encrypt(buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keystream_round_trips_and_hides_the_plaintext() {
        let key = [7; KEY_LEN];
        let nonce = [9; NONCE_LEN];
        let plaintext = b"secret payload";
        let mut buf = plaintext.to_vec();
        apply_keystream(key, nonce, &mut buf);
        assert_ne!(&buf, plaintext, "the keystream must mask the plaintext");
        apply_keystream(key, nonce, &mut buf);
        assert_eq!(
            &buf, plaintext,
            "encryption and decryption are the same XOR"
        );
    }

    #[test]
    fn a_wrong_key_or_nonce_does_not_round_trip() {
        let key = [7; KEY_LEN];
        let nonce = [9; NONCE_LEN];
        let plaintext = b"secret payload";
        let mut buf = plaintext.to_vec();
        apply_keystream(key, nonce, &mut buf);
        for wrong in [([8; KEY_LEN], nonce), (key, [8; NONCE_LEN])] {
            let mut wrong_buf = buf.clone();
            apply_keystream(wrong.0, wrong.1, &mut wrong_buf);
            assert_ne!(
                &wrong_buf, plaintext,
                "a wrong key or nonce must not recover the plaintext"
            );
        }
    }
}
