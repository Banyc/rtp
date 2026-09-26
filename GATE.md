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
- **probe** — a *self-validating, report-only* in-process measurement probe:
  it prints the sender parity/gate counters, echo-latency percentiles, or
  repair-path classification, and asserts **its own measurement's integrity** —
  that the arm ran its whole load, that no echo missed its deadline, that the
  counters it prints were actually delivered and agree with the arm's label —
  and nothing about the product: no bound from this file's *Performance*
  section appears in a probe. A broken instrument (zero samples, a counter that
  never arrived, a "clean" arm that dropped datagrams) fails the test instead
  of printing a table of zeros that a reader can mistake for a measurement.
  Run them explicitly when the property they observe is in scope (each names
  its wall-clock cost in the `#[ignore]` reason):

  ```sh
  cargo test --release -p rtp --lib -- --ignored probe_single_symbol_interactive_fec_repair
  cargo test --release -p rtp --lib -- --ignored probe_fresh_tail_armor_latency
  cargo test --release -p rtp --lib -- --ignored probe_fresh_tail_burst_loss_latency
  cargo test --release -p rtp --lib -- --ignored probe_lone_tail_repair_deadline_latency
  cargo test --release -p rtp --lib -- --ignored probe_lone_tail_repair_ladder
  cargo test --release -p rtp --lib -- --ignored probe_armor_copy_cell
  ```

  They cannot be mistaken for a gate: the names start with `probe_`, the docs
  describe the measurement, and the assertion token each probe validates its
  measurement with is recorded in the `gate-probe-selfchecks` block below.
  `tools/check-ignored.py` fails when a probe's body reaches no assertion (a
  probe that stopped validating its measurement), when its recorded count and
  its body disagree (a check added, removed or moved under the ignore flag is
  visible in this file), and when the block names a test that is not a probe.

The whole in-crate opt-in battery is `cargo test --release -p rtp --lib --
--ignored`; the six probes' own `#[ignore]` reasons sum to ~388 s (~6.5
minutes), plus a few seconds for the four perf lanes. Neither that sum nor any
single probe has been measured: the perf declaration below cites those reasons
and records the measurement itself as a gap.

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
`rtp_mux/tests/hol_verify4.rs`) and the shared-bottleneck fairness/latency battery
(`shared_bottleneck.rs`, which drives `rtp` flows through a shared
`BottleneckShaper`).

- **default** — not `#[ignore]`d, so a plain `cargo test -p rtp` runs it.
  Every scenario here is seeded (deterministic impairment). This is the gate
  that runs on every `cargo test`: measured on this checkout it is 11.9-18.1 s
  end to end, of which the `lib` target is 4.08-9.25 s, `fuzz_codec`
  2.57-2.60 s and `rtp_padding_bench` 3.53-4.09 s (see the perf declaration
  below for the method, the load and the per-row costs).
- **standard** — `#[ignore]`d, runs in well under a minute per target and
  asserts a correctness or transport-floor property. Run the target
  explicitly, e.g. `cargo test --release -p rtp --test rtp_bufferbloat --
  --ignored --nocapture --test-threads=1` (a bare `cargo test -p rtp --
  --ignored` runs the whole opt-in inventory, including the `full` tier and
  the 90-minute `longrun`).
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

   The lone tail's *repair deadline* is measured report-only by
   `probe_lone_tail_repair_deadline_latency`, which seeds the RTT estimate
   from the handshake (the production shape, unlike
   `probe_fresh_tail_burst_loss_latency`, whose handshake-less construction
   makes its cold start a pre-first-RTT-sample tail) and sweeps a burst that
   is exactly the six-datagram fresh-tail cover versus one that also eats the
   first tail-loss probe.  The first two arms bracket the deadline on a
   clean-delay 100 ms one-way path: the first repairs at the first PTO
   (`2*srtt + RTT`); the second loses that PTO too, and repairs once the burst
   being crossed has taken its remaining datagrams — one or two rungs, not the
   multi-second ladder the field reported before the three departures below.
   A third arm — `burst8_owd95_jitter100` — adds `sch_netem`'s ±100 ms
   per-direction delay jitter, the regime where the estimator's variance term
   dominates and the post-probe floor stops binding; its round-trip floor is
   the one-way delay, because per-packet jitter makes the two-way minimum
   zero.  No bound is asserted here: the bars above are the mandate's, and
   this probe attributes which of them a regression moves.
   The *ladder* itself — the rung spacing the field stall is made of — is
   measured deterministically and load-independently by
   `probe_lone_tail_repair_ladder`, whose four arms are two constant-RTT
   scales (50 ms, 190 ms) and two of the `sch_netem` `delay TIME JITTER`
   draw (25 ms one-way ± 25 ms, and the field's own 95 ms ± 100 ms at
   ~190 ms RTT), because the jitter-dominated regime is where the post-probe
   floor stops binding.  The ladder's **height** — what one loss event costs
   — is measured by `probe_lone_tail_finite_loss_ladder`, which drops a run
   of consecutive datagrams on the request direction and reports the rungs
   the send space fires and the wait to first delivery.  Its accounting, read
   off the code: a lone-tail lane is the only source on its direction, so a
   burst is consumed one datagram per datagram of our own traffic; every tail
   transmission emits `1 + cover` datagrams (the primary plus the armour
   copies a repair re-sends it, `PktSendSpace::cover_copies`), and
   transmission `j` — `0` the original — covers datagrams
   `m*j ..= m*j + m - 1` and is delivered iff `m*j + m > burst`.  The burst
   therefore costs exactly `floor(burst / m)` rungs, and the wait is the
   delivered rung's offset plus one RTT.  **The rung *count* is the burst's
   length in our datagrams divided by what one transmission offers; it is not
   a property of the ladder's cadence, and only the wall clock is the count
   times the step.**  Measured, at the two constant-RTT scales, for `m = 1`
   (no cover) and `m = 6` (the interactive single-symbol tail at the
   burst-cover tier: primary + four armour copies + the message-sized parity,
   or primary + five copies with the FEC parity gate closed — which is also
   why the smoke arm's `lone_wire_x` reads ≈ 6.1):

   | arm | 1 datagram | 2 | 3 | 5 | 6 | 12 | 18 |
   |---|---|---|---|---|---|---|---|
   | 50 ms, m=1 | 1 rung, 150 ms | 2, 250 | 3, 550 | 5, 1150 | 6, 1450 | 12, 3250 | 18, 5050 |
   | 50 ms, m=6 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 1, 150 | 2, 250 | 3, 550 |
   | 190 ms, m=1 | 1, 570 | 2, 870 | 3, 1170 | 5, 1770 | 6, 2070 | 12, 3870 | 18, 5670 |
   | 190 ms, m=6 | 0, 0 | 0, 0 | 0, 0 | 0, 0 | 1, 570 | 2, 870 | 3, 1170 |

   Two consequences the smoke arms already carry.  A burst of five datagrams —
   exactly the cover the interactive tail's armour exists to absorb — costs
   the interactive lane **no rung at all**, so a lone-tail excursion cannot be
   a short burst: the arm's observed maxima are `floor(burst / 6)` for a burst
   of 6–18+ datagrams.  And the first two rungs are the prober's two
   `2 * sRTT` windows rather than the floor, so only the rungs after them are
   floor-shaped.

   This transport's tail-recovery timing departs from RFC 8985 in three
   places.  All are recorded here because the latency floors above are built
   on them, and all were chosen from measurement rather than from taste:

   - **The pre-first-RTT-sample probe window is `TAIL_PROBED_MIN_RTO`
     (300 ms), not §7.2's `PTO = 1 s` (nor §7.3's skip).**  With no sample the
     estimator's sRTT *is* the `MIN_RTO` floor, so §7.2's formula degenerates
     to a full second of silence before the first probe of an unmeasured
     path.  The rule buys no safety in this protocol: a probe is a duplicate
     of an already-sent, unacked packet, which the receiver de-duplicates by
     sequence number, so an early probe costs one datagram while the §7.2
     wait costs the *entire* recovery of a tail lost before the first sample.
     Measured as the ~1 s cold-start echo in
     `probe_fresh_tail_burst_loss_latency`'s burst arm (max 1049 ms → 351 ms
     with the cap).  Nothing else moves: the general RTO path — and so every
     retransmission and non-tail timer — keeps the 1 s `MIN_RTO` floor, and
     the window still abstains entirely when every packet is acked.
   - **A repair re-sends the tail's own armour cover.**  A probe or
     window-expiry retransmission of an interactive single-symbol tail fires
     only after that packet's whole original transmission — primary plus its
     cover — was lost, so the link has just demonstrated it drops that many
     consecutive datagrams.  A lone repair datagram is then *known* to be
     dropped while the burst is still pending, and the episode climbs one
     rung per dropped datagram; re-sending the recorded cover spends the
     burst's remaining drop budget inside one rung.  The count is the
     packet's own recorded cover, so a repair never sends more than the
     original did, and the episode ends when the tail is acked — its datagram
     count is bounded by the burst being crossed, not by the number of rungs.
     Bulk/stock repairs record no cover and are byte-for-byte unchanged.
     Measured on `probe_lone_tail_repair_deadline_latency`'s burst-8 arm: max
     1611 ms → 919 ms, p99 1523 ms → 917 ms, p90 921 ms → 615 ms, with
     `rto_reason` 33 → 3 and `tail_probes` 46 → 26.
   - **A corroborated tail repair is not armed at RFC 6298's RTO.**  Two
     departures, both scoped to the prober's spent-budget path so that no
     unmeasured or merely-idle connection is affected: the deadline drops the
     1 s `MIN_RTO` floor (the two unanswered probes have already corroborated
     the loss that floor exists to wait out), and it takes the path's measured
     reorder margin `max(rttvar, srtt / 4)` in place of RFC 6298's
     `K * rttvar` (K = 4).  The floor alone still left the field's regime
     un-reached: at 190 ms RTT with `sch_netem`'s ±100 ms per-direction delay
     jitter `raw_rto = srtt + 4 * rttvar` is ~507 ms, so the 300 ms floor
     never binds and the rung stays the estimator's own variance bound — the
     case where the corroboration buys nothing where it is worth most.  The
     margin is never later than the general RTO, so a long-RTT low-jitter path
     (where the `srtt / 4` floor dominates and would otherwise *loosen* every
     rung) is unchanged, and it is never earlier than the measured sRTT.  The
     probe window's cap uses the same number, so the probe cadence tightens
     with the deadline.  Measured on `probe_lone_tail_repair_ladder`: rung
     spacing 507 ms → 300 ms on the 190 ms RTT / ±100 ms one-way-jitter arm,
     with the two constant-RTT arms unchanged at 300 ms and the low-RTT
     jittered arm unchanged at 300 ms (`raw_rto` there is 128 ms, below the
     floor).  The two pins that encoded the superseded behaviour are rewritten
     to name the new one —
     `rto::tests::reorder_window_tracks_variance_and_the_corroborated_repair_deadline_tracks_the_reorder_margin`
     and `tlp::tests::post_probe_rto_tightens_to_the_corroborated_margin_on_a_jittery_link`
     — and each goes red when the corroborated margin is reverted.

     What the tightening does *not* buy, measured rather than assumed: on the
     two end-to-end arms that reach the field regime the tail is
     indistinguishable between the two rungs.  On
     `probe_lone_tail_repair_deadline_latency`'s `burst8_owd95_jitter100` arm
     (median of three runs, reverted → landed) p50 250 → 240 ms, p90 382 →
     378, p99 733 → 754, max 783 → 824 ms, with >300 ms samples 56 → 46 and
     >600 ms 11 → 13 and the RTO rung firing 0 → 2 times per 200 messages —
     every difference inside the arm's own run-to-run spread (the reverted
     arm's p90 alone ranged 358-557 ms), because at ±100 ms per-direction
     jitter the path's own delay spread is the same order as the ~207 ms the
     rung gives up.  On the dual-lane `rtp_mux` jitter gate re-run at the
     field regime (95 ms one way, ±100 ms jitter) the interactive lane's own
     wire multiple moved 3.83× → 4.01× (median of three reverted and five
     landed runs; ranges 3.76-4.04 and 3.76-4.25) with p99 167 → 174 ms, still
     inside the 6× budget.  The tightening is therefore landed as a
     mechanism-correct, structurally bounded change whose end-to-end payoff at
     the field's jitter level is **not demonstrated** by any arm here; it is
     provable only on the deterministic ladder, where the rung is the binding
     term.  **A rung *below* `TAIL_PROBED_MIN_RTO` was then tried and is
     rejected on measurement.**  The variant let the corroborated margin also
     govern the deadline and the probe's own armed RTO, so the ladder stepped
     by the margin from its third rung (`probe_lone_tail_finite_loss_ladder`:
     50 ms RTT, `m = 1` — rungs at 100, 200, 251, 302, 353 ms instead of 100,
     200, 500, 800, 1100; 190 ms RTT — 380, 680, 871, 1062 instead of 380,
     680, 980, 1280 — and the 18-datagram burst's wait 5050 → 1066 ms and
     5670 → 3926 ms).  On the `rtp_mux` mandate smoke set, three reverted and
     three landed runs: the lone-tail maximum moved from 325 / 733 / 628 ms
     (median 628) to 798 / 875 / **2299** ms (median 875) — disjoint — with
     `> 250 ms` samples 2 / 4 / 2 → 4 / 3 / 4; the lane's own wire multiple
     rose on **both** impaired arms — lone 6.09 / 6.17 / 6.10 → 6.49 / 6.24 /
     6.23 (disjoint, and `lone_wire_x ≈ 6` *is* the six-datagram per-message
     budget, so any increase there is a repair datagram) and hostile 3.97 /
     4.24 / 4.63 → 4.83 / 4.85 / 4.63 (median 4.24 → 4.83).  A rung that fires
     before the previous transmission's ACK is back is a duplicate, and on
     these arms it also made the tail **worse**, so the floor stays and the
     step is treated as settled.  The clean arm — M1's asserting arm and M2's
     real budget — was unmoved: `clean_p99` 91.3 / 86.3 / 90.9 → 90.9 / 90.4 /
     91.2 ms with zero samples `> 250 ms` on both sides, and `clean_wire_x`
     2.11 / 2.34 / 2.09 → 2.24 / 2.16 / 2.21 against the 6× budget.  M3 read
     0.958 → 0.958 and M4 was unchanged and PASS, so no mandate moved in
     either direction except the lone tail's guards.
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
   - `rtp_burst_loss::rtp_bulk_goodput_under_iid_loss_keeps_a_high_fraction_of_the_loss_free_pipe`
     (opt-in `full`): on the perf battery's `deterministic-iid-loss-fat-pipe`
     topology (100 Mbit/s, 150 ms OWD, 16k queue, 8192-byte MSS, ~1 % seeded
     iid loss), the loss arm must keep
     `MIN_IID_LOSS_VS_LOSS_FREE_RATIO = 0.85` of a concurrently measured
     loss-free arm on the identical topology, with each arm above a 1 MiB/s
     anti-stall floor. Stock aggregate 0.985-1.017 (per-rep 0.972-1.035 over
     two 3-rep x 20 s runs after a 40 s convergence warmup); the guard-drop
     fast-loss re-fire collapses it to 0.775. The burst-vs-random ratio above cannot see this
     shape: it compares two *impaired* arms, so a collapse common to both
     leaves its ratio at ~1.0.
   - `rtp_burst_loss::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random`
     (opt-in `full`): burst-loss bulk must not collapse below
     `MIN_BURST_VS_RANDOM_RATIO = 0.60` of the same-rate random-loss baseline
     (aggregate of 6 seeded reps, concurrent arms), and each arm keeps an
     absolute anti-stall floor of **1 MiB/s** so the ratio cannot pass on two
     collapsed arms.

The asserting tests are named in `gate-asserting`; each has been vacuity-checked
in its new home (break the guard → the failure names the guard).

## Perf declaration — declared time, declared coverage

The dual mandate (`AGENTS.md`, "The perf-test dual mandate — time and
coverage") requires every perf test to name its tier, its cost and the
coverage cells it provides, so the trade between the two is visible and
checkable rather than assumed. The three blocks below are rtp's declaration;
`python3 ../netem_test/tools/check-gate.py --crate . rtp tests GATE.md`
enforces it. Each row's relation to its family's baseline is re-derived from
the row's own cells, so a row that varies several dimensions must be labelled a
`composite` and a row that varies none must say why it repeats the point.

### The always-run tier, measured

The tier that runs on every `cargo test` is `cargo test -p rtp`: the 729-test
`--lib` target, the 14 `gate-default-required` scenarios, and `fuzz_codec`.
Measured on this checkout in **debug** (the mode the default gate runs in),
three reps per binary, reading the harness's own `finished in` figure:

| binary | run | wall clock (3 reps) |
| --- | --- | --- |
| `lib` | 719 (+10 ignored) | 4.10 / 9.25 / 4.08 s |
| `fuzz_codec` | 1 | 2.59 / 2.57 / 2.60 s |
| `rtp_padding_bench` | 3 (+6 ignored) | 3.53 / 3.71 / 4.09 s |
| `shared_bottleneck` | 2 (+7 ignored) | 0.71 / 0.71 / 0.71 s |
| `rtp_mss` | 3 | 0.56 / 0.66 / 0.67 s |
| `rtp_loss` | 1 | 0.50 / 0.48 / 0.50 s |
| `rtp_fec` | 1 (+1 ignored) | 0.43 / 0.29 / 0.31 s |
| `rtp_clean` | 3 | 0.13 / 0.13 / 0.13 s |
| `rtp_liveness` | 1 (+2 ignored) | 0.00 / 0.00 / 0.00 s |
| `hol_verify4`, `rtp_bufferbloat`, `rtp_burst_loss`, `rtp_gentle` | 0 (every test `#[ignore]`d) | 0.00 s |
| **`cargo test -p rtp` end to end** | **734** | **12.85 / 18.11 / 13.39 s** |

**How it was measured.** The per-binary and end-to-end figures are wall clock
around the cargo invocation, read from the harness's own `finished in` line. A
per-row cost is the **harness's own per-test time**, not a position in the
output stream: each test binary is run directly with libtest's `--report-time`,
which stamps `test <name> ... ok <T s>` from an instant taken around that test's
own execution, so a row's cost stays its own while libtest runs the target's
tests concurrently. `--report-time` (like `--format json`) is gated behind
`-Z unstable-options` on a nightly compiler, so the measurement passes that flag
to the binary the gate itself built, with `RUSTC_BOOTSTRAP=1` set at runtime —
a flag gate, not a different build. Every target is measured three times at the
default 10-thread parallelism (the mode the gate runs) and three times at
`--test-threads=1`, which takes the other tests' contention out of a row's own
cost and makes the **fit check** exact: *the per-test costs of a target must fit
that target's total.* They do, in every serialized rep: `--lib` sums
38.40 / 34.34 / 37.79 s of per-test time against 38.41 / 34.35 / 37.80 s
reported, `fuzz_codec` 2.62 / 2.60 / 2.60 s exactly, `rtp_padding_bench`
3.89 / 3.98 / 3.95 s exactly, `rtp_mss` 0.57 / 0.70 / 0.67 s exactly. Under the
default parallelism the `--lib` per-test sum is 31.89 / 35.70 / 38.47 s against
a 3.74-9.14 s wall clock — the expected shape for ten tests at a time, several
of them sleeping rather than burning CPU. Both figures are stated because both
are true and they are not the same quantity: the serialized one is what a row
costs, the parallel one is what it has to compete with.

That fit check is also what exposes a cost that was never measured. The
per-row costs declared here previously came from the mandate-runner's streamed
report, whose per-test duration is bracketed between consecutive completions
**in a shared output stream** while the harness runs tests concurrently — a
duration that is an artefact of the interleaving rather than a measurement. It
produced two rows that cannot fit their own target at all (the `fuzz_codec` row
at 5.50 s in a target that takes 2.58-2.68 s, the padding family's fitted-ACK
arm at 5.60 s in a target that takes 3.46-3.71 s), and it is the same defect
that gave the two decoder fuzzes 6.17 s and 8.49 s where they cost 1.92 s and
1.93 s. The per-test map the rows above are drawn from comes from the harness;
the runner in `netem_test` computes per-test durations from stream position and
must take them from the harness instead before a declaration cites the report
again.

**Load.** The host is shared and loaded throughout: 1-minute load average
3.1-5.0 and 15-minute 6.7-9.1 on 10 cores. These are costs measured *under
load*, not properties of the tests, and the rep-to-rep spread says how much of
each number is the machine: inside the tier's own three
invocations `rtp_padding_bench` is 3.53-4.09 s (±7 %) and `fuzz_codec`
2.57-2.60 s (±1 %), while the `lib` target is bimodal at 4.08-4.10 s or 9.25 s —
not machine speed but one test: `socket::stream::tests::test_fec_recovers_under_loss`
measures 2.80 / 5.01 / 9.03 s in the parallel reps of those same three runs, so
the `lib` target's wall clock is quantised by whether its unlucky losses land
before a probe can repair them. The rungs of that quantisation, measured rather
than inferred, are the ~300 ms tail-loss probe (the common one), 600 ms, the 1 s
`MIN_RTO` and a rare ~4 s multi-rung ladder — not only `MIN_RTO` waits.
Run standalone the same binaries spread wider — `rtp_padding_bench` 3.44-4.39 s,
`fuzz_codec` 2.57-3.16 s. Every declared cost is the **max of its three reps**,
so a later run is compared against a deliberately conservative figure rather
than a lucky one. Where the two modes disagree materially the row is a
timing-cadence-sensitive one (the two padding-shape rows: 0.26 s and 0.18 s
serialized against 0.11 s each contended) and the serialized figure is declared,
because it is the one that fits the target's total.

**The `lib` target's largest row is irreducible, and its cost is coverage.**
`socket::stream::tests::test_fec_recovers_under_loss` exchanges 512 one-packet
messages one at a time in each direction under 8 % iid loss per wrapper (15 %
effective per datagram), so each message's delivery is either recovered by the
sender's tail-flush parity or left to ARQ. Twelve uninstrumented serialized reps
at 1-minute load 2.5-3.1 and 15-minute 3.3-3.8 on 10 cores span **1.03-9.37 s**
(median ~2.4 s), so the declared max-of-three 9.44 s is a draw from the row's own
tail rather than a reproducible figure. A phase probe (`Instant` around setup,
and around both the server's `recv` and the client's round trip) puts 0.014 s in
socket and payload setup and the rest in the exchange: with the same 512-message
exchange but the impairment zeroed it delivers byte-exact in 0.17-0.24 s (three
runs) and fails only on its own `recovered > 0` assertion, while the 8 % arms
spend 0.69-0.86 s outside their stalls (nine probed runs; ~0.2 s of that the
transfer the zero-loss probe measures, the rest parity datagrams, their decode
and repairs too short to register) and put everything else into 1-4 stalls
longer than 250 ms, each at one of the rungs above. Those stalls are the losses
the single parity copy did not cover:
with the receiver's parity recovery disabled (the FEC decode callback delivering
nothing, all else unchanged) an 8 % run goes red on `FEC should recover >0
symbols under 8 % loss, got 0` after **8** stalls summing 4.5 s instead of the
usual 1-4. Nothing here is slack — the wait is a production repair timer, the
parity datagrams and their decode are the FEC path, and 512 messages at 15 %
loss is what makes `recovered > 0` near-certain — so this row is recorded as
irreducible rather than shortened, and the always-run tier's cost is what its
assertions require.

Two consequences stated rather than left implicit. First, the 17 declared
`default` rows are rows, not the tier: they sum to 14.30 s of serialized
per-test cost, the whole `--lib` target sums to 34.3-38.4 s measured the same
way, and the tier's wall clock is 11.9-18.1 s at its own parallelism — three
different quantities, and none is presented as another. The block's largest
single costs are not the two fuzzes but
`socket::stream::tests::test_fec_recovers_under_loss` (9.44 s serialized,
2.80-9.03 s contended), then
`socket::session::tests::a_stuck_underlay_still_resolves_the_session_handle`
and `socket::session::tests::the_post_terminal_kill_tail_is_bounded` (3.00 s each,
re-measured here at 3.00-3.01 s), then the two decoder fuzzes at 1.84 s and 1.85 s,
then
`traffic_shaping::control::handshake::opening::tests::lost_ready_is_recovered_by_a_duplicate_confirmation`
at 1.65 s, then
`traffic_shaping::control::handshake::opening::tests::post_open_timer_recovers_without_another_client_confirmation`
(1.51 s),
`traffic_shaping::control::handshake::opening::tests::every_handshake_leg_recovers_from_one_lost_datagram`
(1.22 s) and the watchdog row at 1.20 s.
`mpudp::tests::a_session_wider_than_the_cap_is_refused`, recorded in this paragraph
as a 3.11 s cost, now measures 0.21 s: its refusal assertion waited out a 3 s
negative-control deadline against an admission measured at 0.67-1.46 ms, and
100 ms now bounds it. Second, the in-crate opt-in battery is **not** "about
four minutes" — the six probes' own `#[ignore]` reasons sum to ~388 s — and
neither figure has been measured, which is recorded as a gap below.

**Neither FEC decoder-fuzz row is a single-test target, and neither cost is
recoverable by shortening.** Both rows are two of the 719 tests in the `--lib`
target, which libtest runs at its own parallelism — 10 threads here, and
47.92 / 26.63 / 15.13 / 9.25 / 6.28 s at `--test-threads=1/2/4/8/16` on the same
revision — so no core sits idle for them and there is no spare thread for an
in-test overlap to take. (The serialized reps reproduce the first of those
figures: 46.95 / 47.08 / 47.04 s.) Deleting both outright, which is the ceiling on any
shortening, is not even resolvable above the target's own run-to-run variance:
at load average 2.8-3.4 the `--lib` wall clock measures 8.01-9.86 s with both
rows present and 7.39-7.68 s with both skipped, and in release — where the two
rows cost 0.06 s each, 0.12 s of CPU in total — the same A/B measures 8.03 s
against 7.17 s, a spread larger than the whole of the work removed. What does
hold is the bound. The target is gated by its multi-second sleeping tests
rather than by CPU — it is 8.0-9.9 s in debug and 7.2-8.0 s in release, while
the two rows' own cost falls from 1.85 s to 0.06 s — so their entire debug
cost, ~3.7 s of CPU across 50,000 rounds each, is absorbed, and 0.37 s is the
most it could ever buy even packed perfectly across the 10 threads the target
runs on. The remainder of the cost is not recoverable either. A
three-rep phase probe inside the rounds (load average 4.0-4.1) puts each fuzz
at 0.49 s generating the hostile datagram stream, 1.35 s inside the decoder
under test, 0.008 s building the per-round wire packet and 0.001 s draining
recoveries; the guards the production path adds per round (`catch_unwind`, the
thread-local flag, the cached panic hook) calibrate at 0.0018 s for the same
50,000 rounds. `wire_pkt`'s per-round buffer, at 0.45 %, is therefore the
whole of what a cheaper per-round equivalent could take. The rounds are not
independent besides — the `SplitMix64` stream is sequential and the decoder
carries group state across packets — so overlapping them would hand round *n*
different bytes than the serial pass feeds it, which is a coverage change and
not a shortening. Both rows' declared costs above are **measured isolated in debug** — the mode the
always-run gate runs (`cargo test -p rtp --lib -- --exact --test-threads=1`, 1.88 s
and 1.87 s) — not read from a streamed per-test report. The serialized
harness-native pass reports 1.92 s and 1.93 s, within 3 % of them, so the two
methods agree on these rows. That distinction is not
cosmetic here: the streamed numbers previously recorded for these two rows
(**6.17 s** and **8.49 s**) are artefacts of bracketing completions in a shared
output stream while the harness runs tests concurrently, and they were provably
wrong before anyone measured them properly — **their sum, 14.66 s, exceeds the
entire 719-test lib target, which is 8.12 s in debug**. The two rows are real
costs and significant ones (~3.75 s of that 8.12 s), but they are 3–4× smaller
than the report claimed, and a tier budget computed from the report would have
been wrong by that much.

Both fuzzes were re-shown to fail when the property each guards is broken.
Making the decoder return a payload one byte longer than its packet — the
range the first fuzz exists to forbid — fails it at round 4 with "a 1423-byte
packet yielded 1424 payload bytes". Removing the `encodable_wire_pkt`
precondition so unguarded hostile input reaches the direct decoder call fails
the second with a third-party `TooManyShards` panic propagated out of
`fec::de`, which is the panic its docs say must not be swallowed.

### Cost provenance

No cost below is invented; each is one of two things.

- **measured** — a default-tier row's cost is the max of the three serialized
  harness-native reps described above, in seconds, rounded up to the two
  decimals the block uses. A test the harness resolves below its 10 ms grain is
  recorded as `0.01`, the grain, not as zero: a zero cost makes the checker's
  drift test divide by zero. Where the parallel figure is materially lower the
  row is contended rather than cheaper, and the serialized figure is the one
  declared; the two decoder-fuzz rows are the exception, declared from their
  isolated measurement and confirmed by the serialized pass (1.92 s and 1.93 s
  against 1.88 s and 1.87 s), because a CPU-bound row's contended time is the
  other tests' cost as much as its own.
- **cited** — an opt-in row's cost is the wall clock its own `#[ignore]`
  reason states: `probe_single_symbol_interactive_fec_repair` ~45 s,
  `probe_fresh_tail_armor_latency` ~25 s,
  `probe_fresh_tail_burst_loss_latency` ~145 s,
  `probe_lone_tail_repair_deadline_latency` ~2.7 min (162 s),
  `probe_armor_copy_cell` ~10 s for its one cell,
  `probe_lone_tail_repair_ladder` <1 s (1 s), and the two standard-tier
  liveness arms 65 s and 5 s.

A row whose wall clock appears in no document *and* was not measured is not
given a number: it is recorded as a gap below, so an unmeasured cost is
visibly pending instead of plausibly guessed.

The declared sums are `default` 14.30 s of a 60 s budget, `standard` 70 s of
300 s, `perf` 388 s of 450 s, and nothing in `full`, whose 6000 s ceiling is
declared so a later row cannot be added without one — `full` is the tier the
~90-minute `shared_bneck_fairness_longrun` lives in.

```gate-perf-design
rtp_clean::rtp_over_netem_clean_link_delivers_data = default | 0.01 | baseline | transport-delivery@impairment=clean+mss=default+scale=small+metric=byte-exact+shape=bulk+layer=rtp
rtp_clean::rtp_over_netem_clean_link_delivers_400kib = default | 0.06 | orthogonal | transport-delivery@impairment=clean+mss=default+scale=400KiB+metric=byte-exact+shape=bulk+layer=rtp
rtp_clean::rtp_over_netem_latency_is_observable = default | 0.14 | composite(impairment,metric) | transport-latency@impairment=delayed-60ms+mss=default+scale=small+metric=latency-floor+shape=bulk+layer=rtp
rtp_fec::rtp_with_fec_recovers_under_netem_loss = default | 0.32 | composite(fec,impairment,scale) | transport-delivery@impairment=loss-3pct+mss=default+scale=1MiB+metric=byte-exact+shape=bulk+layer=rtp+fec=on
rtp_loss::rtp_over_netem_survives_mild_loss_400kib = default | 0.51 | composite(impairment,scale) | transport-delivery@impairment=mild-loss-5pct+mss=default+scale=400KiB+metric=byte-exact+shape=bulk+layer=rtp
rtp_mss::rtp_small_mss_clean_link_delivers_data = default | 0.01 | orthogonal | transport-delivery@impairment=clean+mss=512+scale=small+metric=byte-exact+shape=bulk+layer=rtp
rtp_mss::rtp_custom_mss_clean_link_delivers_200kib = default | 0.06 | composite(mss,scale) | transport-delivery@impairment=clean+mss=1024+scale=200KiB+metric=byte-exact+shape=bulk+layer=rtp
rtp_mss::rtp_tiny_mss_survives_mild_loss = default | 0.79 | composite(impairment,mss,scale) | transport-delivery@impairment=mild-loss-5pct+mss=256+scale=100KiB+metric=byte-exact+shape=bulk+layer=rtp
fuzz_codec::a_hostile_datagram_never_yields_a_range_outside_it = default | 3.17 | composite(impairment,layer,metric,scale) | codec-fuzz@impairment=hostile-datagram+metric=range-safety+layer=codec+scale=400k-rounds
rtp_padding_bench::padded_wire_sizes_converge_to_one_peak = default | 0.26 | baseline@padding | padding-wire@padding=profile+impairment=clean+metric=size-distribution+scale=256KiB+layer=rtp
rtp_padding_bench::unpadded_wire_sizes_stay_multimodal = default | 0.18 | orthogonal@padding | padding-wire@padding=none+impairment=clean+metric=size-distribution+scale=256KiB+layer=rtp
rtp_padding_bench::ack_padding_hides_ack_packets_among_data = default | 4.30 | composite(padding,metric)@padding | padding-wire@padding=ack-mimics-data+impairment=clean+metric=ack-obscurity+scale=256KiB+layer=rtp
shared_bottleneck::a_slow_reply_resynchronizes_instead_of_ending_the_phase = default | 0.72 | baseline@contested | contested-instrument@impairment=none+metric=sample-retention+layer=shared-bottleneck+scale=unit
shared_bottleneck::absolute_starvation_floor_fires_on_a_jain_perfect_collapse = default | 0.01 | orthogonal@contested | contested-instrument@impairment=none+metric=starvation-floor+layer=shared-bottleneck+scale=unit
rtp_liveness::reverse_traffic_recency_advances_only_on_new_packets = default | 0.01 | baseline@liveness | transport-liveness@impairment=clean+metric=recency-advance+layer=rtp
rtp_liveness::rtp_permanent_hole_liveness_smoke = standard | 5 | composite(impairment,metric,scale)@liveness | transport-liveness@impairment=permanent-mtu-hole+metric=connection-liveness+layer=rtp+scale=short-watchdog
rtp_liveness::rtp_fresh_sacks_beyond_permanent_mtu_hole_do_not_keep_connection_alive = standard | 65 | composite(fresh-sacks,impairment,metric)@liveness | transport-liveness@impairment=permanent-mtu-hole+fresh-sacks=on+metric=connection-liveness+layer=rtp
lib::traffic_shaping::redundancy::fec::tests::a_hostile_datagram_never_escapes_the_fec_decoder = default | 1.88 | baseline@decoder-fuzz | decoder-fuzz@impairment=hostile-datagram+metric=no-panic+layer=fec+scale=50k-rounds
lib::traffic_shaping::redundancy::fec::tests::a_guarded_hostile_datagram_never_panics_the_fec_decoder = default | 1.87 | re-measurement(direct-decoder-path-bypasses-catch-unwind-so-a-panicking-hostile-datagram-fails-the-test-instead-of-being-counted-malformed)@decoder-fuzz | decoder-fuzz@impairment=hostile-datagram+metric=no-panic+layer=fec+scale=50k-rounds
lib::traffic_shaping::recovery::pkt_send_space::tests::probe_lone_tail_repair_ladder = perf | 1 | baseline@probe | probe-ladder@impairment=jitter+metric=rung-spacing+layer=rtp+scale=4-arms
lib::socket::stream::tests::probe_fresh_tail_armor_latency = perf | 25 | composite(impairment,metric)@probe | probe-armor@impairment=clean+metric=armour-latency+layer=rtp
lib::socket::stream::tests::probe_fresh_tail_burst_loss_latency = perf | 145 | composite(handshake,impairment,metric)@probe | probe-armor@impairment=burst-loss+metric=armour-latency+layer=rtp+handshake=none
lib::socket::stream::tests::probe_single_symbol_interactive_fec_repair = perf | 45 | composite(fec,impairment,metric)@probe | probe-fec@impairment=loss+metric=repair-latency+layer=rtp+fec=on
lib::socket::stream::tests::probe_lone_tail_repair_deadline_latency = perf | 162 | composite(handshake,impairment,metric)@probe | probe-deadline@impairment=burst-loss+metric=repair-deadline+layer=rtp+handshake=seeded
lib::socket::stream::tests::probe_armor_copy_cell = perf | 10 | composite(impairment,metric,scale)@probe | probe-armor@impairment=clean+metric=armour-copy+layer=rtp+scale=cell
```

### The families

Six references cover the declared subset. The **residual** (default) family is
the always-run transport floors, stated against the clean-link byte-exact
delivery point; two of its rows are one dimension away from it (`scale`, and
`mss`) and the rest are labelled with the dimensions they actually move, which
is the honest reading of pre-existing arms that were never built as a
one-axis set. The five named families each carry their own reference:
`padding` (wire-shape at one 256 KiB transfer), `contested` (the
shared-bottleneck instrument's own sanity pair, one dimension apart),
`liveness` (the recency/connection-liveness family), `decoder-fuzz` (the
hostile-datagram FEC decoder fuzz pair, a deliberate repeat through the
unguarded path) and `probe` (the six report-only repair-latency instruments).
The bulk of the declared rows are `composite` because the arms genuinely vary
several dimensions at once — labelling them orthogonal would be the confound
the mandate exists to prevent.

The `padding` family's fitted-ACK arm is the one row whose cost fell without a
retune: its 24 trials x 2 arms x 256 KiB are now pooled with two transfers in
flight, which is the same units, the same bytes and the same datagrams, at a
declared 4.30 s (4.29 s, the max of three serialized harness-native reps;
4.39 s contended). The 5.60 s / 7.53 s / 7.14-9.90 s figures the pooling was
reported against came from the streamed instrument described above, so the A/B
ratio is recorded rather than re-derived: the unpooled arm no longer exists in
the tree to measure. The overlap stops at two
because the arm counts a scheduling-sensitive leak — an ACK sent before the
sampler has `MIN_SAMPLES` observations goes out unpadded — so contention is
visible in the measured statistic: the pooled fitted/baseline small ratio (the
unchanged `< 0.5` bound) measured 0.140-0.190 serial and 0.158-0.192 at two in
flight, the same envelope, while four in flight reached 0.328 at load average
15 and wider overlap inflated the work as well. No cell, threshold, round count
or tier changed. The always-run ceiling is a bound on the tier's declared
total, not on one test: that total is now 14.30 s of 60 s, and `default = 60`
is left as declared. The headroom over the 16.8-18.6 s the tier actually
measures is deliberate, and tightening a ceiling is a policy decision rather
than a consequence of one arm's cost.

```gate-budgets
default = 60
standard = 300
full = 6000
perf = 450
baseline = rtp_clean::rtp_over_netem_clean_link_delivers_data
baseline.padding = rtp_padding_bench::padded_wire_sizes_converge_to_one_peak
baseline.contested = shared_bottleneck::a_slow_reply_resynchronizes_instead_of_ending_the_phase
baseline.liveness = rtp_liveness::reverse_traffic_recency_advances_only_on_new_packets
baseline.decoder-fuzz = lib::traffic_shaping::redundancy::fec::tests::a_hostile_datagram_never_escapes_the_fec_decoder
baseline.probe = lib::traffic_shaping::recovery::pkt_send_space::tests::probe_lone_tail_repair_ladder
members.padding = padding-wire
members.contested = contested-instrument
members.liveness = transport-liveness
members.decoder-fuzz = decoder-fuzz
members.probe = probe-*
drift = 0.5
drift_floor_s = 2.0
```

The rest is **not** declared. The granularity is the family, not the row: a
family whose rows share one blocker is one gap naming the blocker and the
repair, because recording its rows individually would hide the blocker behind
noise. Two blockers account for every undeclared family — no wall clock in any
document, and a cell name whose property is foreign to the family it would
have to join. A **new arm closes neither**, so no family below is closed by
adding one.

```gate-coverage-gaps
attribution@baseline-family=burst-loss = the three full-tier rtp_burst_loss arms (rtp_sparse_message_tail_latency_under_burst_loss and the two bulk-goodput arms) are internally coherent — the tail arm is one impairment away from the shared rate+queue+loss topology the goodput pair already varies — but no document records their wall clock: GATE.md's only figure is "~180-316 s by report", a range and an unattributed report rather than a cost. One streamed release run of the target per row declares the family with no cell change.
attribution@baseline-family=shared-bottleneck-full = the seven full-tier shared_bottleneck arms share the contested instrument with the two default rows declared above, but their `#[ignore]` reasons state no wall clock; only fairness_longrun has one (~90 minutes). The repair is one streamed run per arm; the family then needs its own reference row because it separates from the declared `contested` family by impairment (bulk-contended rather than unit), which is a second cell name and not a retune of the first.
attribution@baseline-family=bufferbloat = rtp_bufferbloat::rtp_bulk_bounded_buffer_goodput_and_queue_bound is a single standard-tier row whose `#[ignore]` reason says only "slow". A one-row family has no member to state a relation against and no citable cost, so both halves are missing: one streamed run plus a `bufferbloat@...` cell name and a reference decides it.
attribution@baseline-family=gentle = rtp_gentle::gentle_mode_exits_via_gate_open_after_a_standing_queue_drains is a single standard-tier row, blocked the same way as bufferbloat, with the extra problem that its two-phase gate-open shape has no second arm in any tier to vary one dimension against — so it is a one-row family until a second arm exists, not merely an uncosted one.
attribution@baseline-family=fec-diversity = rtp_fec::rtp_max_diversity_fec_covers_single_packet_messages_under_loss measures a different property from the declared default-tier FEC row (max-diversity cover of single-packet messages versus whole-stream recovery) and is two declared dimensions away from it (fec-mode, metric). The repair is either a max-diversity arm one dimension from the declared FEC row or a composite label naming both, plus the row's cost.
attribution@baseline-family=padding-perf = the six perf-tier rtp_padding_bench rows are report-only A/B measurements (bulk throughput and small-echo latency across three presets) whose `#[ignore]` reasons state no wall clock, and whose cells are the A/B preset axis rather than the declared `padding-wire` axis. The repair is one streamed run per row plus a `padding-ab` cell name and a reference row of its own.
attribution@baseline-family=hol-verify4 = hol_verify4::v4_clean_rawbulk and v4_ge5_rawbulk are one dimension apart (impairment) and internally coherent, so only their costs are missing: neither `#[ignore]` reason states a wall clock and no document does either. One streamed release run per row declares the family with no cell change.
attribution@baseline-family=perf-lane = the four in-crate perf-lane tests are asserting gates, but this grammar resolves an `#[ignore]`d lib row's tier to `perf`, which this crate's own tier vocabulary reserves for report-only measurement; declaring an asserting perf lane as `perf` would file a gate under the tier that must contain no assertion. The repair is a tier the grammar can name for an in-crate asserting opt-in test, or reading the perf lanes' tier from the `ignored-manifest` classification the crate already keeps.
cost@metric=wall-clock = the remaining 717 default-tier lib tests have no per-test cost in any document. Their suite cost is measured (4.08-9.25 s parallel, 34.34-38.40 s serialized) but a row names one test, so the repair is to declare the expensive ones with cells of their own from the harness-native per-test map (libtest `--report-time`, max of three serialized reps, with the contended figure at the default 10-thread parallelism in brackets; every cost below is well under its target's total, and the two decoder fuzzes already declared are omitted): socket::stream::tests::test_fec_recovers_under_loss 9.44 s [9.03], traffic_shaping::control::handshake::opening::tests::lost_ready_is_recovered_by_a_duplicate_confirmation 1.65 s [1.65], socket::session::tests::a_stuck_underlay_still_resolves_the_session_handle 3.00 s [3.00], socket::session::tests::the_post_terminal_kill_tail_is_bounded 3.00 s [3.00], transmission::transmission_layer_test_facade::tests::proactive_watchdog_aborts_locally_before_best_effort_kill_completes 1.20 s [1.20], socket::stream::tests::test_fec_recovers_under_loss_with_mss_8192 0.96 s [1.54], traffic_shaping::control::handshake::opening::tests::post_open_timer_recovers_without_another_client_confirmation 1.51 s [1.52], traffic_shaping::control::handshake::opening::tests::nonce_bound_ready_retires_post_open_retransmissions 1.66 s [1.66], traffic_shaping::control::handshake::opening::tests::post_open_guard_recovers_after_three_lost_confirmations 0.89 s [0.86], socket::session::tests::read_drop_keeps_session_alive_until_recv_window_saturates 0.66 s [0.85] and socket::stream::tests::a_bulk_transfer_survives_loss_reorder_and_duplication 0.55 s [0.58]; nothing else in the target reaches 0.9 s in either mode. Two rows this gap listed at 5.06 s and 5.01 s are gone from it: each waited out a 5 s refusal deadline against a refused admission measured at 5-34 ms, so each now waits 500 ms and costs 0.50-0.55 s. A third row has left it the same way: `mpudp::tests::a_session_wider_than_the_cap_is_refused` waited out a 3 s refusal deadline against an admission measured at 0.67-1.46 ms (57 runs, 5 serialized through 12 and 40 concurrent, load average 3.3-3.8 on 10 cores), so it now waits 100 ms and costs 0.21 s [0.21], below this list's 0.9 s cutoff. A row left it under the same mandate without shortening a wait at all: `traffic_shaping::control::handshake::opening::tests::every_handshake_leg_recovers_from_one_lost_datagram` ran four independent single-loss legs — one per dropped handshake kind, each over its own channel pair with its own nonce, each recovering in one opening retry interval (250 ms + jitter) rather than in a fixed sleep — sequentially, so the tier paid four retry intervals for four cells that share nothing but the code under test. The legs are now polled concurrently (`tokio::join!` over the same four pairs, the same drop filter, the same kind-specific RTT assertions), which costs 0.35 s [0.35] against 1.22 s [1.22] before, below this list's 0.9 s cutoff. The cells are unchanged: a run with the drop recorded at each leg's write filter shows all four kinds dropped exactly once (Hello, HelloAck, Confirm, ConfirmAck), and breaking either the retransmission interval or the never-sample-after-a-retransmission rule still fails the test. A fourth was shortened rather than removed from the list: `transmission::transmission_layer_test_facade::tests::proactive_watchdog_aborts_locally_before_best_effort_kill_completes` slept the 2 s `max_timeout` of its own test-supplied `WatchdogTuning`, which is the watchdog deadline's upper bound rather than the deadline: the deadline is `clamp(rto * rto_multiplier, floor, max_timeout)`, and with the test's settled 1 ms RTT the estimator RTO sits on the 1 s `MIN_RTO` floor against a multiplier of 1, so a 50 us-resolution probe of `stall_reason` measures it at 1.00005 s (five runs) and the 2 s cap never binds. The wait is now 1.2x that measured deadline, and the row costs 1.20 s [1.20] against 2.00/2.00/2.00 before. No assertion changed and the tuning is untouched: the assertions need the watchdog to have fired when the next send pass evaluates it, and shortening only the sleep keeps the exercised configuration — including which term of the clamp binds — byte-identical. A fifth was shortened with its coverage relocated rather than dropped: `traffic_shaping::control::handshake::opening::tests::lost_ready_is_recovered_by_a_duplicate_confirmation` waited 3.2 s for the product's `POST_OPEN_RETRY_DELAYS` chain, of which only the +1 s slot is a cadence its assertions need — the server's duplicate confirmation and the client's retried Ready land 1.0368-1.3833 s after the opening (eight probed runs; the slot is `1s + 0-499 ms` per-nonce jitter) — and eight further probed runs found no event at all between 1.39 s and 4 s, because the retried Ready retires the recovery. The 3.2 s window did not cover the +3 s slot deterministically in any case, that slot's jitter placing it in [3.0, 3.5) s, so the retirement the window was said to prove was not in fact proven; it now is, exactly and at no wall clock, by `transmission::post_open_recovery::tests::retried_ready_retires_the_scheduled_retransmission_chain`, which drives the same `PostOpenRecovery` by argument — withholding the retirement in `PostOpenRecovery::observe` fails that test, and withholding the client's re-queue fails the integration test on `ready_attempts` at the shortened wait. The row costs 1.61-1.65 s serialized and 1.63-1.65 s contended against 3.23-3.25 s before, with no assertion or configuration changed. A sixth row was re-bounded upward rather than shortened, because its assertion forbids an event and the wait has to outlast the event it forbids: `traffic_shaping::control::handshake::opening::tests::nonce_bound_ready_retires_post_open_retransmissions` asserts that the server's post-open recovery sent no second `ConfirmAck`, and the slot such a send would come from is `1 s + retry_delay`, whose per-nonce jitter places it in [1.0, 1.5) s with the nonce drawn per connection from `SysRng`, so the former 1.1 s wait reached the slot only on the runs whose jitter was at most 100 ms: with the retirement withheld in `PostOpenRecovery::observe`, the test still passed 9 of 10 runs at 1.1 s and failed all 6 runs at the 1.6 s that bounds the 1.499 s worst case plus ~100 ms for the write driver's wake. The row costs 1.63-1.67 s against 1.13-1.16 s before, and no assertion or configuration changed. The property itself -- that a `Ready` retires the schedule so no later slot sends -- is pinned at no wall clock by `transmission::post_open_recovery::tests::retried_ready_retires_the_scheduled_retransmission_chain`; what the wall clock still buys in the integration row is the driver wiring, which only a wait past the slot can expose. The 2.9 s that the `mpudp` row gave back is not resolvable in the suite totals: the `--lib` target re-measured 4.20-9.19 s parallel and 35.07-35.88 s serialized (two reps) after the shortening, inside the 4.08-9.25 / 34.34-38.40 s range recorded here, so that range was left as measured then. The two shortenings above remove a further 2.4 s of serialized per-test sleep, and three fresh reps of the `--lib` binary after them measure 3.20-3.31 s at the default parallelism and 32.64-35.47 s serialized (load average 3.3-4.2 on 10 cores; the fit check still holds exactly — 35.46/35.48, 35.47/35.48 and 32.64/32.66 s of per-test sum against reported wall clock), with three end-to-end reps of `cargo test -p rtp` at 11.91-13.81 s (the sum of the harness's per-binary `finished in` figures, 734 passing and 33 ignored tests). The two row changes above move both halves of that fit check by their net -0.35 s and nothing else: the per-test sums land at 35.11, 35.12 and 32.29 s, inside the 33.99-38.05 s serialized range the same shift gives the target. Three fresh serialized reps of the `--lib` binary re-measure 32.43-33.95 s, and three end-to-end reps of `cargo test -p rtp` measure 12.35-13.45 s real against 12.03-12.34 s of `finished in` figures (load average 3.4-5.3 on 10 cores). Those reps are the current reading: the serialized floor moved down by the 2.4 s, and the parallel ones landed below the 4.08 s floor, which is this session's lighter load rather than the change — the tier's wall clock is a maximum over concurrently run sleeping tests, and both shortened rows are shorter than the `lib` target's longest, so the 2.4 s cannot have raised it. The 7.32 / 5.02 / 5.00 s set this gap originally named was read from the streamed instrument and is superseded above. This gap's top row, `socket::stream::tests::test_fec_recovers_under_loss`, was probed rather than shortened and is irreducible — the map paragraph above records the measurement (1.03-9.37 s over twelve serialized reps, a 0.17-0.24 s zero-loss floor for the same 512-message exchange, and 1-4 production repair stalls per run) — so its 9.44 s max-of-three cost is left unchanged.
transport-delivery@impairment=correlated-loss = no default-tier row drops four-state Gilbert-Elliot loss at the transport layer: the always-run impaired delivery rows use the harness's 5 % iid mild-loss preset and the 3 % iid FEC preset, and GE loss is exercised only by the opt-in burst-loss arms. An always-run GE arm is what closes this, and it is a new arm rather than a retune of a frozen one.
transport-delivery@metric=goodput-fraction = no default-tier row measures goodput as a fraction of the configured link rate: the capacity-relative floors are opt-in (bufferbloat `standard`, burst-loss `full`). The cell is knowingly empty in the always-run tier because the arm it needs is a multi-second rate-shaped run, which the 60 s budget's headroom over the measured 16.8-18.6 s does not currently buy; the deliberate answer is to measure the opt-in arm rather than to move a floor into a tier that cannot pay for it.
transport-latency@impairment=burst-loss = the always-run tier's only latency assertion is the 60 ms one-way delay observability check. The sparse-message tail under GE burst loss is opt-in (`full`), and an always-run tail arm would need a window short enough for the default budget and long enough to carry one recovery episode — a new arm, not a shortened probe.
```

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
src/socket/stream.rs::probe_lone_tail_repair_deadline_latency = probe
src/socket/stream.rs::probe_single_symbol_interactive_fec_repair = probe
src/traffic_shaping/recovery/pkt_send_space.rs::applying_many_sacks_remains_linear_in_the_send_window = perf-lane
src/traffic_shaping/recovery/pkt_send_space.rs::probe_lone_tail_repair_ladder = probe
src/traffic_shaping/recovery/pkt_send_space.rs::probe_lone_tail_finite_loss_ladder = probe
src/traffic_shaping/recovery/rtx_index.rs::deferred_loss_cancellation_does_not_rescan_the_pending_set = perf-lane
tests/rtp_bufferbloat.rs::rtp_bulk_bounded_buffer_goodput_and_queue_bound = standard
tests/rtp_burst_loss.rs::rtp_bulk_goodput_burst_loss_does_not_collapse_vs_random = full
tests/rtp_burst_loss.rs::rtp_bulk_goodput_under_iid_loss_keeps_a_high_fraction_of_the_loss_free_pipe = full
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

The probe inventory of what each probe validates: the count of assertion tokens
in the probe's own body. A probe's measurement-integrity checks are part of the
instrument, so they are recorded the same way the netem_test gate records the
asserting helpers a report-only `perf` scenario may reach — a change to them is
a change to this file.

```gate-probe-selfchecks
src/socket/stream.rs::probe_armor_copy_cell = 6
src/socket/stream.rs::probe_fresh_tail_armor_latency = 4
src/socket/stream.rs::probe_fresh_tail_burst_loss_latency = 7
src/socket/stream.rs::probe_lone_tail_repair_deadline_latency = 8
src/socket/stream.rs::probe_single_symbol_interactive_fec_repair = 4
src/traffic_shaping/recovery/pkt_send_space.rs::probe_lone_tail_repair_ladder = 12
src/traffic_shaping/recovery/pkt_send_space.rs::probe_lone_tail_finite_loss_ladder = 7
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
rtp_burst_loss::rtp_bulk_goodput_under_iid_loss_keeps_a_high_fraction_of_the_loss_free_pipe = full
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
rtp_burst_loss::rtp_bulk_goodput_under_iid_loss_keeps_a_high_fraction_of_the_loss_free_pipe
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
tests/hol_verify4.rs::assert_bulk_delivered = 1
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

### The M1 lone-tail residual is the ladder's height, and it is inherent

The step is settled: at the field's ~190 ms round trip with `±100 ms`
per-direction jitter the corroborated reorder margin is ~270 ms against the
300 ms `TAIL_PROBED_MIN_RTO`, so the floor is worth ~10 % there and a rung
below it was measured to *raise* the tail and the wire (the negative recorded
above).  The count is settled too: `n = floor(burst / m)` with `m` already at
the largest per-transmission datagram count the wire budget is written for —
six datagrams a message (`FRESH_INTERACTIVE_TAIL_ARMOR_COPIES_BURST_*` plus
the message-sized parity), which `lone_wire_x ≈ 6.1` confirms is what the lane
actually sends.  So the residual is `floor(burst / 6) × ~300 ms`: a 0.33–2.4 s
lone maximum is a burst of 6–15+ consecutive datagrams and the field's 3.2 s
climb is ~12 rungs of the same arithmetic.  The lever that would reduce it is
a larger `m`, and that is the interactive lane's own wire, which M2 exists to
bound.  Recorded as inherent, with the instrument that measures it
(`probe_lone_tail_finite_loss_ladder`) rather than as an open lever.