use rtp::testing::{DecodedDataPkt, SplitMix64, decode};

const ROUNDS: usize = 400_000;

fn packet(rng: &mut SplitMix64) -> Vec<u8> {
    let mut out = vec![];
    for _ in 0..1 + rng.below(6) {
        match rng.below(6) {
            0 => {
                out.push(0);
                out.extend((0..16).map(|_| rng.byte()));
            }
            1 => {
                out.push(1);
                out.extend((0..8).map(|_| rng.byte()));
                let len = rng.below(64);
                out.extend((len as u16).to_be_bytes());
                out.extend((0..len).map(|_| rng.byte()));
            }
            2 => out.push(2),
            3 => {
                out.push(3);
                out.extend((0..12).map(|_| rng.byte()));
                let len = rng.below(64);
                out.extend((len as u16).to_be_bytes());
                out.extend((0..len).map(|_| rng.byte()));
            }
            4 => {
                out.push(4);
                out.extend((0..4).map(|_| rng.byte()));
            }
            _ => {
                out.push(5);
                out.extend((0..16).map(|_| rng.byte()));
                let len = rng.below(64);
                out.extend((len as u16).to_be_bytes());
                out.extend((0..len).map(|_| rng.byte()));
            }
        }
    }
    for _ in 0..rng.below(3) {
        if out.is_empty() {
            break;
        }
        let at = rng.below(out.len());
        out[at] = rng.byte();
    }
    match rng.below(4) {
        0 if !out.is_empty() => {
            let n = rng.below(out.len());
            out.truncate(n);
        }
        _ => {}
    }
    out
}

#[test]
fn a_hostile_datagram_never_yields_a_range_outside_it() {
    let mut rng = SplitMix64::new(0x5eed);
    let mut acks = Vec::new();
    let mut decoded_count = 0_usize;
    let mut decode_attempts = 0_usize;
    for _ in 0..ROUNDS {
        let pkt = packet(&mut rng);
        acks.clear();
        decode_attempts += 1;
        let Ok(decoded) = decode(&pkt, &mut acks) else {
            continue;
        };
        decoded_count += 1;
        if let Some(DecodedDataPkt { buf_range, .. }) = decoded.data {
            assert!(
                buf_range.start <= buf_range.end && buf_range.end <= pkt.len(),
                "{buf_range:?} is outside a {}-byte packet {pkt:02x?}",
                pkt.len(),
            );
            let _ = &pkt[buf_range];
        }
        for ack in &acks {
            assert!(ack.end() >= ack.start, "{ack:?} wrapped");
        }
    }
    assert!(
        decode_attempts >= ROUNDS,
        "only {decode_attempts}/{ROUNDS} mutation cases executed; the fuzz budget was truncated"
    );
    assert!(
        decoded_count * 4 > ROUNDS,
        "only {decoded_count}/{ROUNDS} packets decoded; the generator went stale"
    );
}
