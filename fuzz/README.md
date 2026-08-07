# `rtp-fuzz`

Standalone libFuzzer workspace for the `rtp` crate.  The empty `[workspace]`
table makes this its own workspace, so the fuzz-only `libfuzzer-sys`
dependency and this crate's `Cargo.lock` never enter the main `rtp` workspace
resolution.

The `cargo-fuzz = true` marker lives in `[package.metadata]` (not `[package]`)
in `Cargo.toml` — the location the current cargo-fuzz tool requires.

- `seeds/` — the tracked starting corpus.  Committed.
- `corpus/`, `artifacts/`, `coverage/`, `target/` — growing/derived fuzz
  artifacts.  Ignored by `.gitignore`, never committed.

## Run

From a checkout root that contains the `rtp/` crate:

```sh
mkdir -p fuzz/corpus/codec_decode
cp -n fuzz/seeds/codec_decode/* fuzz/corpus/codec_decode/
cargo fuzz run --sanitizer=none codec_decode -- -max_total_time=300
```

`--sanitizer=none` avoids the sanitizer runtimes (not wired up here; it is a
command-line flag, not a code change).

## Note for this host

cargo-fuzz needs a CIS (CrowdStrike Falcon) policy exclusion on this host:
Falcon SIGKILLs libFuzzer binaries at exec.  Until that exclusion is granted,
the in-repo deterministic hostile-input harnesses in
`rtp/tests/fuzz_codec.rs` (and the `rtp` crate's own `a_hostile_datagram_*`
unit tests) are the only coverage that runs on this host.

The `Cargo.lock` is tracked: it pins the fuzz-only dependency graph (`rtp`
with the `testing` feature plus `libfuzzer-sys`) so the standalone workspace
builds reproducibly.  Rebuild it with `cargo build` inside `rtp/fuzz` only when
the dependency graph intentionally changes.
