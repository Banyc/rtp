# The rtp opt-in test inventory

This file records every `#[ignore]`d test in `src/`. `cargo test` silently
skips them all, so each one's opt-in classification is named here and
machine-checked by `python3 tools/check-ignored.py`, which fails if a test is
added, removed, renamed, or reclassified without this manifest being updated
— an unnoticed `#[ignore]` skip is impossible.

Run the checker after touching any `#[ignore]`d test (including its doc and
its reason string):

```sh
python3 tools/check-ignored.py
```

## Classifications

- **perf-lane** — an *asserting* test kept opt-in because its assertion is a
  wall-clock sub-linear-scaling ratio (`many < few * 8.0`: the per-operation
  cost at thousands of outstanding entries must stay within a constant factor
  of the cost at tens of entries). These are honest gates — they fail on an
  algorithmic-complexity regression — but the assertion measures wall-clock
  time, so it is environment-dependent and only meaningful under `--release`.
  Each one's `#[ignore]` reason documents its run command. To run the four
  perf lanes (a few seconds in release):

  ```sh
  cargo test --release -p rtp --lib -- --ignored withholding_frames_behind_a_hole_costs_no_more_per_pop
  cargo test --release -p rtp --lib -- --ignored advancing_the_receive_window_costs_no_more_per_packet
  cargo test --release -p rtp --lib -- --ignored deferred_loss_cancellation_does_not_rescan_the_pending_set
  cargo test --release -p rtp --lib -- --ignored applying_many_sacks_remains_linear_in_the_send_window
  ```

  The wall-clock basis is exactly why they are not in the default gate: the
  default suite runs in debug on possibly-loaded machines, and a timing-ratio
  assertion there would be a flake source, not a signal. The checker keeps
  them honest — a perf lane whose body loses its assertion token, or whose
  reason stops documenting the run command, is an error.
- **probe** — a *report-only* in-process measurement probe: it prints the
  sender parity/gate counters, echo-latency percentiles, or repair-path
  classification, and asserts nothing. Run them explicitly when the property
  they observe is in scope (each names its wall-clock cost in the `#[ignore]`
  reason):

  ```sh
  cargo test --release -p rtp --lib -- --ignored probe_single_symbol_interactive_fec_repair
  cargo test --release -p rtp --lib -- --ignored probe_fresh_tail_armor_latency
  cargo test --release -p rtp --lib -- --ignored probe_fresh_tail_burst_loss_latency
  cargo test --release -p rtp --lib -- --ignored probe_armor_copy_cell
  ```

  They cannot be mistaken for a gate: the names start with `probe_`, the docs
  describe the measurement, and the checker refuses an assertion token in a
  probe body — a check silently added under the ignore flag is the exact hole
  this inventory exists to close.

The whole opt-in battery is `cargo test --release -p rtp --lib -- --ignored`
(about four minutes; the probes dominate).

## Ignored-test manifest

Each line is `src/RELATIVE_PATH::fn = classification`. The set must equal the
`#[ignore]`d tests found in `src/` by `tools/check-ignored.py`.

```ignored-manifest
src/recv_queue/pkt_recv_space.rs::advancing_the_receive_window_costs_no_more_per_packet = perf-lane
src/recv_queue/pkt_recv_space.rs::withholding_frames_behind_a_hole_costs_no_more_per_pop = perf-lane
src/socket/stream.rs::probe_armor_copy_cell = probe
src/socket/stream.rs::probe_fresh_tail_armor_latency = probe
src/socket/stream.rs::probe_fresh_tail_burst_loss_latency = probe
src/socket/stream.rs::probe_single_symbol_interactive_fec_repair = probe
src/traffic_shaping/recovery/pkt_send_space.rs::applying_many_sacks_remains_linear_in_the_send_window = perf-lane
src/traffic_shaping/recovery/rtx_index.rs::deferred_loss_cancellation_does_not_rescan_the_pending_set = perf-lane
```

## Residual limitations

The checker is regex-and-brace-counting, the same tool level as the netem_test
gate: it sees a `#[ignore]` attribute followed by a `fn` in the same file, and
an assertion token only inside the fn's brace-balanced body. An assertion
hidden behind a macro alias, a trait object, or a function pointer is
invisible to it, exactly as documented for the harness gate. It also cannot
see `#[ignore]`-like attributes written in a macro. Nothing here substitutes
for running the perf lanes when the property they assert is in scope — the
manifest guarantees the classification and the set, not the measurements.