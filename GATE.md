# The rtp test gates

This file is the authoritative scope of rtp's opt-in test inventory and its
scenario gate. `cargo test` silently skips every `#[ignore]`d test, so both
opt-in sets are recorded here and machine-checked:

- `python3 tools/check-ignored.py` re-derives the `#[ignore]`d tests in `src/`
  **and** the relocated scenario targets in `tests/` from the source files and
  fails if a test is added, removed, renamed, or reclassified without this
  manifest being updated — an unnoticed `#[ignore]` skip is impossible.
- `python3 ../netem_test/tools/check-gate.py --crate . rtp tests GATE.md`
  (from this checkout; the shared per-crate checker) enforces the scenario
  gate: the `gate-manifest`/`gate-asserting`/`gate-default-required`
  blocks below, plus the report-only perf tier's reach declared in
  `gate-perf-guard-helpers`.

## In-crate classifications

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

The whole in-crate opt-in battery is `cargo test --release -p rtp --lib --
--ignored` (about four minutes; the probes dominate).

## Scenario gate tiers

The rtp performance scenarios relocated here from the harness
(`netem_test/tests`) so that every rtp transport floor is asserted by rtp's own
test invocation; the harness keeps only the impairment instrument and points at
the owning crates. They consume the harness's `netem-test` kit (local `git`
release tag, `test-kit` feature) for impairment and the generic helpers, and
rtp's own layer kit (`rtp::testkit`, behind the `testing` feature).

The relocated `tests/` set is: the earlier `rtp_bufferbloat`, `rtp_burst_loss`,
`rtp_fec`, `rtp_gentle`, `rtp_liveness`, `rtp_loss`, `rtp_mss` and
`rtp_padding_bench` suites, plus the raw-`rtp` arms of the DualMux-v4 A/B
comparison (`hol_verify4.rs`, the `rtp` half whose `mux` half lives in
`mux/tests/hol_verify4.rs`) and the shared-bottleneck fairness/latency battery
(`shared_bottleneck.rs`, which drives `rtp` flows through a shared
`BottleneckShaper`).

- **default** — not `#[ignore]`d, so a plain `cargo test -p rtp` runs it.
  Every scenario here is seeded (deterministic impairment) and finishes in a
  few seconds. This is the gate that runs on every `cargo test`.
- **standard** — `#[ignore]`d, runs in well under a minute per target and
  asserts a correctness or transport-floor property. Run with
  `cargo test -p rtp -- --ignored --test-threads=1`.
- **full** — `#[ignore]`d, minutes per target; still asserts a property, but
  too slow for the default gate. Run the target explicitly
  (`cargo test --release -p rtp --test rtp_burst_loss -- --ignored
  --nocapture --test-threads=1`).
- **perf** — `#[ignore]`d, report-only A/B measurement; these produce numbers,
  they do not assert a gate floor (the padding distribution/ACK-hiding floors
  they touch are asserted by the default-tier padding tests). A `perf`
  scenario must not contain an assertion in its own body, nor reach an
  assertion through a helper: `check-gate.py --crate . rtp` fails with the
  scenario name, its file, and the token if it does.
- **standard** / **full** asserting scenarios may keep their assertion in a
  helper defined in the same target file (the shared-bottleneck `full` arms
  assert inside `rr_under_bulk_ab`); `tools/check-ignored.py` follows those
  crate-local calls and fails when the reachable closure loses its last
  assertion token, so such a scenario cannot silently stop being a gate.

`gate-default-required` names the asserting scenarios that must stay in the
default tier; the checker fails if one is re-`#[ignore]`d or removed.
`gate-asserting` records the full report-only/asserting split.

## Performance

The operator's product constitution is **three mandates**, and each is an
asserting gate in the crate that owns the lane it constrains. The production
dual-lane topology is owned by `rtp_mux` (its `GATE.md` states the mandate
bounds there); rtp owns the **transport layer** — the reliable byte stream
over the impaired link — so rtp's gate asserts the per-layer facts the
mandates depend on. Each bound below is derived from the topology it runs on,
not a magic constant; the harness never restates one.

1. **Low latency of the interactive lane** — the interactive lane's tail
   latency stays at its floor through the transport under impairment. rtp's
   part is the sparse-message tail through a burst-loss transport, asserted
   by `rtp_burst_loss::rtp_sparse_message_tail_latency_under_burst_loss`
   (opt-in `full`; the messages' own 300 ms cadence keeps the connection
   alive, so the tail is recovery, not broken-pipe):
   - p50 ≤ **300 ms** — the one-way floor is the topology's `OWD = 50 ms`
     (100 ms RTT) plus a recovery margin; stock behaviour is ~55 ms, and the
     bound leaves ~5.5x headroom so it is a regression floor, not a
     measurement pin.
   - p99 ≤ **2500 ms** — under GE burst loss (5 % long-term, mean burst
     length 3) the tail is RTO-quantized recovery episodes; 2.5 s is the
     documented bound a recovery regression must respect.
   - delivery ≥ **98 %** of offered sparse messages (`received / sent`),
     the offered-payload definition of the mandate at the rtp layer.
   The bufferbloat ping floor (`rtp_bufferbloat`, opt-in `standard`) pins the
   same mandate under a queue-bound bottleneck: the topology's one-way delay
   floor (20 ms) plus the bottleneck queue-residence (256-packet limit at
   10 Mbit/s) and a repair margin ⇒ p50 ≤ 800 ms.
2. **Reasonable goodput of the interactive lane** — the lane delivers what it
   is offered (`delivery = 1.000`) without inflating its own wire. At the
   rtp layer `delivery = 1.000` is the offered payload arriving byte-exact,
   asserted in the **default tier**: `rtp_clean` (clean link, small and
   400 KiB echoes byte-exact, zero proxy drops), `rtp_loss`
   (mild-loss 400 KiB byte-exact), `rtp_fec` (~3 % loss, byte-exact via
   parity recovery), and `rtp_mss` (the MSS boundary classes). The sparse
   burst-loss probe's 98 % floor is the same delivered-vs-offered ratio under
   burst impairment. The wire-budget half of the mandate is owned by
   `rtp_mux` (the interactive lane's own wire is a topology fact, not a
   transport fact).
3. **High goodput of the bulk lane** — the bulk lane keeps a high fraction of
   the link's capacity. rtp's transport floors:
   - `rtp_bufferbloat` (opt-in `standard`): bulk goodput ≥ **0.35 × the
     configured bottleneck rate** (10 Mbit/s → ≥ ~0.42 MiB/s), the
     `rtp_bufferbloat` precedent for the shape of a capacity-relative floor,
     plus zero overflow drops (the 256-packet queue exceeds the in-flight
     bound) and a bounded c2s queue depth.
   - `rtp_burst_loss::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random`
     (opt-in `full`): burst-loss bulk must not collapse below
     `MIN_BURST_VS_RANDOM_RATIO = 0.60` of the same-rate random-loss baseline
     (aggregate of 6 seeded reps, concurrent arms), and each arm keeps an
     absolute anti-stall floor of **1 MiB/s** so the ratio cannot pass on two
     collapsed arms.

The asserting tests are named in `gate-asserting`; each has been vacuity-checked
in its new home (break the guard → the failure names the guard).

## Ignored-test manifest

Each line is `RELATIVE_PATH::fn = classification`. The set must equal the
`#[ignore]`d tests found in `src/` and `tests/` by `tools/check-ignored.py`.
`perf-lane`/`probe` classify the in-crate (`src/`) tests; `standard`/`full`/
`perf` are the scenario tiers of the relocated `tests/` targets (same names
as the scenario gate below).

```ignored-manifest
src/recv_queue/pkt_recv_space.rs::advancing_the_receive_window_costs_no_more_per_packet = perf-lane
src/recv_queue/pkt_recv_space.rs::withholding_frames_behind_a_hole_costs_no_more_per_pop = perf-lane
src/socket/stream.rs::probe_armor_copy_cell = probe
src/socket/stream.rs::probe_fresh_tail_armor_latency = probe
src/socket/stream.rs::probe_fresh_tail_burst_loss_latency = probe
src/socket/stream.rs::probe_single_symbol_interactive_fec_repair = probe
src/traffic_shaping/recovery/pkt_send_space.rs::applying_many_sacks_remains_linear_in_the_send_window = perf-lane
src/traffic_shaping/recovery/rtx_index.rs::deferred_loss_cancellation_does_not_rescan_the_pending_set = perf-lane
tests/rtp_bufferbloat.rs::rtp_bulk_bounded_buffer_goodput_and_queue_bound = standard
tests/rtp_burst_loss.rs::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random = full
tests/rtp_burst_loss.rs::rtp_sparse_message_tail_latency_under_burst_loss = full
tests/rtp_fec.rs::rtp_max_diversity_fec_covers_single_packet_messages_under_loss = standard
tests/rtp_gentle.rs::gentle_mode_exits_via_gate_open_after_a_standing_queue_drains = standard
tests/rtp_liveness.rs::rtp_fresh_sacks_beyond_permanent_mtu_hole_do_not_keep_connection_alive = standard
tests/rtp_liveness.rs::rtp_permanent_hole_liveness_smoke = standard
tests/rtp_padding_bench.rs::ab_bulk_throughput_ack_padding = perf
tests/rtp_padding_bench.rs::ab_bulk_throughput_across_presets = perf
tests/rtp_padding_bench.rs::ab_small_echo_latency = perf
tests/rtp_padding_bench.rs::ab_small_echo_latency_ack_padding = perf
tests/rtp_padding_bench.rs::ab_small_echo_latency_across_presets = perf
tests/rtp_padding_bench.rs::padding_throughput_overhead = perf
tests/hol_verify4.rs::v4_clean_rawbulk = perf
tests/hol_verify4.rs::v4_ge5_rawbulk = perf
tests/shared_bottleneck.rs::shared_bneck_fairness_longrun = full
tests/shared_bottleneck.rs::shared_bneck_fairness_sweep = full
tests/shared_bottleneck.rs::shared_bneck_late_joiner_fairness = full
tests/shared_bottleneck.rs::shared_bneck_reorder_tolerant_fairness = full
tests/shared_bottleneck.rs::shared_bneck_rr_under_bulk_10mbps = full
tests/shared_bottleneck.rs::shared_bneck_rr_under_bulk_2mbps = full
tests/shared_bottleneck.rs::shared_bneck_rr_under_dedicated_bulk_10mbps = full
```

## Scenario manifest

Each line is `target::test_name = tier`. The set must equal the set of
non-`support` tests reported by `cargo test -p rtp --test <target> -- --list
--ignored`.

```gate-manifest
hol_verify4::v4_clean_rawbulk = perf
hol_verify4::v4_ge5_rawbulk = perf
rtp_bufferbloat::rtp_bulk_bounded_buffer_goodput_and_queue_bound = standard
rtp_burst_loss::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random = full
rtp_burst_loss::rtp_sparse_message_tail_latency_under_burst_loss = full
rtp_fec::rtp_max_diversity_fec_covers_single_packet_messages_under_loss = standard
rtp_gentle::gentle_mode_exits_via_gate_open_after_a_standing_queue_drains = standard
rtp_liveness::rtp_fresh_sacks_beyond_permanent_mtu_hole_do_not_keep_connection_alive = standard
rtp_liveness::rtp_permanent_hole_liveness_smoke = standard
rtp_padding_bench::ab_bulk_throughput_ack_padding = perf
rtp_padding_bench::ab_bulk_throughput_across_presets = perf
rtp_padding_bench::ab_small_echo_latency = perf
rtp_padding_bench::ab_small_echo_latency_ack_padding = perf
rtp_padding_bench::ab_small_echo_latency_across_presets = perf
rtp_padding_bench::padding_throughput_overhead = perf
shared_bottleneck::shared_bneck_fairness_longrun = full
shared_bottleneck::shared_bneck_fairness_sweep = full
shared_bottleneck::shared_bneck_late_joiner_fairness = full
shared_bottleneck::shared_bneck_reorder_tolerant_fairness = full
shared_bottleneck::shared_bneck_rr_under_bulk_10mbps = full
shared_bottleneck::shared_bneck_rr_under_bulk_2mbps = full
shared_bottleneck::shared_bneck_rr_under_dedicated_bulk_10mbps = full
```

The `gate-default-required` block below pins the asserting scenarios that must
stay in the default tier (`cargo test -p rtp`); the checker fails if one is
re-`#[ignore]`d or removed. The `gate-asserting` block records the full
report-only/asserting split (all default-required plus every `standard` and
`full` scenario); the per-tier `perf` scenarios are report-only by definition.

```gate-default-required
shared_bottleneck::a_slow_reply_resynchronizes_instead_of_ending_the_phase
shared_bottleneck::absolute_starvation_floor_fires_on_a_jain_perfect_collapse
rtp_clean::rtp_over_netem_clean_link_delivers_400kib
rtp_clean::rtp_over_netem_clean_link_delivers_data
rtp_clean::rtp_over_netem_latency_is_observable
rtp_fec::rtp_with_fec_recovers_under_netem_loss
rtp_liveness::reverse_traffic_recency_advances_only_on_new_packets
rtp_loss::rtp_over_netem_survives_mild_loss_400kib
rtp_mss::rtp_custom_mss_clean_link_delivers_200kib
rtp_mss::rtp_small_mss_clean_link_delivers_data
rtp_mss::rtp_tiny_mss_survives_mild_loss
rtp_padding_bench::ack_padding_hides_ack_packets_among_data
rtp_padding_bench::padded_wire_sizes_converge_to_one_peak
rtp_padding_bench::unpadded_wire_sizes_stay_multimodal
```

```gate-asserting
shared_bottleneck::a_slow_reply_resynchronizes_instead_of_ending_the_phase
shared_bottleneck::absolute_starvation_floor_fires_on_a_jain_perfect_collapse
shared_bottleneck::shared_bneck_fairness_longrun
shared_bottleneck::shared_bneck_fairness_sweep
shared_bottleneck::shared_bneck_late_joiner_fairness
shared_bottleneck::shared_bneck_reorder_tolerant_fairness
shared_bottleneck::shared_bneck_rr_under_bulk_10mbps
shared_bottleneck::shared_bneck_rr_under_bulk_2mbps
shared_bottleneck::shared_bneck_rr_under_dedicated_bulk_10mbps
rtp_bufferbloat::rtp_bulk_bounded_buffer_goodput_and_queue_bound
rtp_burst_loss::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random
rtp_burst_loss::rtp_sparse_message_tail_latency_under_burst_loss
rtp_clean::rtp_over_netem_clean_link_delivers_400kib
rtp_clean::rtp_over_netem_clean_link_delivers_data
rtp_clean::rtp_over_netem_latency_is_observable
rtp_fec::rtp_max_diversity_fec_covers_single_packet_messages_under_loss
rtp_fec::rtp_with_fec_recovers_under_netem_loss
rtp_gentle::gentle_mode_exits_via_gate_open_after_a_standing_queue_drains
rtp_liveness::reverse_traffic_recency_advances_only_on_new_packets
rtp_liveness::rtp_fresh_sacks_beyond_permanent_mtu_hole_do_not_keep_connection_alive
rtp_liveness::rtp_permanent_hole_liveness_smoke
rtp_loss::rtp_over_netem_survives_mild_loss_400kib
rtp_mss::rtp_custom_mss_clean_link_delivers_200kib
rtp_mss::rtp_small_mss_clean_link_delivers_data
rtp_mss::rtp_tiny_mss_survives_mild_loss
rtp_padding_bench::ack_padding_hides_ack_packets_among_data
rtp_padding_bench::padded_wire_sizes_converge_to_one_peak
rtp_padding_bench::unpadded_wire_sizes_stay_multimodal
```

The padding `perf` scenarios round-trip data through `run_transfer`/
`run_transfer_preset`, whose `assert_eq!` is an integrity guard, not a gate
(the round-trip property is asserted by the default-tier padding tests). The
checker derives every asserting helper reachable from the `perf` tier and
requires it declared here with its assertion-token count:

```gate-perf-guard-helpers
netem_test/netem-test/src/kit/payload.rs::with_timeout = 1
netem_test/netem-test/src/kit/presets.rs::gilbert_elliott_loss = 2
netem_test/netem-test/src/kit/task_scope.rs::run = 1
netem_test/netem-test/src/kit/task_scope.rs::submit_test_task = 2
netem_test/netem-test/src/kit/task_scope.rs::submit_test_task_required = 1
rtp/src/testkit/rtp.rs::spawn_rtp_byte_sink_server_core = 1
tests/rtp_padding_bench.rs::run_transfer = 1
tests/rtp_padding_bench.rs::run_transfer_preset = 1
```

## Residual limitations

The checkers are regex-and-brace-counting, the same tool level as the
netem_test gate: they see an `#[ignore]` attribute followed by a `fn` in the
same file (with comments stripped first, so a doc that mentions `#[ignore]`
cannot fake a skip), and an assertion token only inside the fn's
brace-balanced body. An assertion hidden behind a macro alias, a trait
object, or a function pointer is invisible to them, exactly as documented for
the harness gate. The scenario gate additionally cannot see an edge created by
passing a function by name, through a trait object, or through a macro alias
(its call graph is name-based and over-approximates where it cannot narrow).
Nothing here substitutes for running the perf lanes when the property they
assert is in scope — the manifests guarantee the classification and the set,
not the measurements.