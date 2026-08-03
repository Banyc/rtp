// Policy: Latest-only protocol, stale peer = attacker, lockstep deploy.
pub(crate) const MAGIC: [u8; 8] = [0xf7, b'R', b'T', b'P', b'O', b'P', 1, 0];
pub(crate) const FEC_GUARD: u8 = 0xff;
pub(crate) const PACKET_LEN: usize = 18;
pub(crate) const SEND_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

pub(crate) type RecoveryResponse = [u8; PACKET_LEN];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Hello = 1,
    HelloAck = 2,
    Confirm = 3,
    ConfirmAck = 4,
    Ready = 5,
}

impl Kind {
    pub(crate) fn decode(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Hello),
            2 => Some(Self::HelloAck),
            3 => Some(Self::Confirm),
            4 => Some(Self::ConfirmAck),
            5 => Some(Self::Ready),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Packet {
    pub(crate) kind: Kind,
    pub(crate) nonce: u64,
}

impl Packet {
    pub(crate) fn encode(self) -> [u8; PACKET_LEN] {
        let mut bytes = [0; PACKET_LEN];
        bytes[..MAGIC.len()].copy_from_slice(&MAGIC);
        bytes[8] = FEC_GUARD;
        bytes[9] = self.kind as u8;
        bytes[10..].copy_from_slice(&self.nonce.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PACKET_LEN || bytes[..MAGIC.len()] != MAGIC || bytes[8] != FEC_GUARD {
            return None;
        }
        Some(Self {
            kind: Kind::decode(bytes[9])?,
            nonce: u64::from_be_bytes(bytes[10..].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_packet() -> Packet {
        Packet {
            kind: Kind::Ready,
            nonce: 0x0123_4567_89ab_cdef,
        }
    }

    #[test]
    fn a_single_byte_mutation_of_every_fixed_field_is_rejected() {
        let valid = valid_packet().encode();
        // Every MAGIC byte: changing any single byte must fail to decode.
        for index in 0..MAGIC.len() {
            let mut mutated = valid;
            mutated[index] ^= 0x01;
            assert_eq!(
                Packet::decode(&mutated),
                None,
                "a change to MAGIC byte {index} was accepted"
            );
        }
        // The FEC_GUARD byte.
        let mut mutated = valid;
        mutated[MAGIC.len()] ^= 0x01;
        assert_eq!(
            Packet::decode(&mutated),
            None,
            "a changed FEC_GUARD byte was accepted"
        );
        // The kind byte: value 0 and every value outside the valid 1..=5
        // range must be rejected.
        for kind in [0u8].into_iter().chain(6..=255) {
            let mut mutated = valid;
            mutated[MAGIC.len() + 1] = kind;
            assert_eq!(
                Packet::decode(&mutated),
                None,
                "kind byte {kind} was accepted as a valid handshake kind"
            );
        }
    }

    #[test]
    fn a_packet_one_byte_short_or_long_is_rejected() {
        let valid = valid_packet().encode();
        assert_eq!(
            Packet::decode(&valid[..PACKET_LEN - 1]),
            None,
            "a {}-byte packet must not decode as an {PACKET_LEN}-byte packet",
            PACKET_LEN - 1
        );
        let mut padded = valid.to_vec();
        padded.push(0);
        assert_eq!(
            Packet::decode(&padded),
            None,
            "a {}-byte packet must not decode as an {PACKET_LEN}-byte packet",
            PACKET_LEN + 1
        );
    }
}
