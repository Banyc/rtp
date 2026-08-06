//! libFuzzer target for `rtp::codec::decode` (the hostile-input decoder used
//! by the reliable layer's read path).
//!
//! Run recipe (from a checkout root that contains the `rtp/` crate):
//!
//! ```sh
//! mkdir -p fuzz/corpus/codec_decode
//! cp -n fuzz/seeds/codec_decode/* fuzz/corpus/codec_decode/
//! cargo fuzz run --sanitizer=none codec_decode -- -max_total_time=300
//! ```
//!
//! The `mkdir -p` creates the ignored working corpus so the `cp -n` never
//! fails on a fresh checkout.  `--sanitizer=none` is a command-line flag, not
//! a code change: this crate does not wire up the Cargo/libFuzzer sanitizer
//! runtimes.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut acks = Vec::new();
    let Ok(decoded) = rtp::testing::decode(data, &mut acks) else {
        return;
    };
    if let Some(pkt) = decoded.data {
        let range = pkt.buf_range;
        assert!(
            range.start <= range.end && range.end <= data.len(),
            "{range:?} is outside a {}-byte packet",
            data.len(),
        );
        let _ = &data[range];
        assert!(
            pkt.frame_len.is_none() || pkt.send_ts.is_some(),
            "frame_len without send_ts"
        );
    }
    for ack in &acks {
        assert!(ack.end() >= ack.start, "{ack:?} wrapped");
    }
});
