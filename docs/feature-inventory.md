# Feature inventory — `rtp` features that landed since v0.0.89

Scope: every **semantic feature** (capability, behaviour, invariant) that
landed in `rtp` between v0.0.89 (`900b0a3`) and `dev`/`default@`
(`342bc786`), i.e. the 41 commits in `jj log -r 'v0.0.89..dev'`.  This is a
research artifact only: it records what exists and where, not a target tree.

Method: read the diff `jj diff --from v0.0.89 --to default@` plus every
changed source file; commit messages were used only to name the intent.

## How to read this doc

- Each feature has an id (`C1`, `R3`, …), its area, and `file:line`
  citations in the current tree.
- `[HIDDEN]` marks a capability that is undocumented, implicit, only in a
  code comment, or only reachable through a test knob / env var.
- "shares file" names the other features whose code currently lives in the
  same file (the extraction passes must separate them).
- "keep together" names code that is mutually interdependent and must not be
  split.
- Paths are relative to the `rtp` crate root.

---

## Area: `traffic_shaping/core` (congestion response, lane, pacing)

### C1 — Explicit `CongestionLane` intent [HIDDEN in places]
The connection owner declares a congestion lane (`Shared` default,
`Dedicated` bulk) that is threaded end-to-end and consulted by the delay
controller; it is never inferred from delivery mode.
- Enum: `traffic_shaping/core/mod.rs:24`; re-export `lib.rs:24`.
- Plumbing: `transmission/transmission_layer.rs:113` (`UnreliableLayer`),
  `transmission/connection.rs:123,129,137`, `udp.rs:438,494`,
  `reliable/reliable_layer.rs:256,284,334`.
- Stored in the policy owner: `core/congestion_response/mod.rs:72`.
- `[HIDDEN]` The intent is a new **public** API surface (`pub use …
  CongestionLane` at `lib.rs:24`) but there is no README/doc mention; only
  `udp.rs` config doctext and inline comments describe it.
- shares file: `core/mod.rs` also exports fast-start/pacing; the enum itself
  is standalone.
- keep together: with C2/C3 (the lane is read by both).

### C2 — Per-lane delay tuning (drain fraction + gentle probe gain)
A `Dedicated` lane drains at the ordinary fraction (`0.9`) and creeps at a
much shallower gentle gain (`0.02`) than a `Shared` lane (`0.75` / `0.20`).
- Drain fraction: `core/congestion_response/mod.rs:293`
  (`drain_fraction`, `== Dedicated` → `DRAIN_RATE_FRACTION` at
  `core/gentle.rs:32`); `GENTLE_DRAIN_FRAC` `core/gentle.rs:21`.
- Gentle gain: `core/gentle.rs:19` (`DEDICATED_GENTLE_BW_PROBE_GAIN`),
  selection at `core/gentle.rs:229-234`.
- `[HIDDEN]` The `== Dedicated` branch appears in **three** files with no
  single authority: `core/gentle.rs`, `core/congestion_response/mod.rs`, and
  `reliable/reliable_layer.rs:1085` (dedicated fast-start exit).  The plan's
  "one authority instead of `== Dedicated` branches" is exactly this gap.
- shares file: `congestion_response/mod.rs` also holds C3, C4, drain
  response; `gentle.rs` also holds C7.
- keep together: C1 (lane selection is meaningless without the intent).

### C3 — Lane-uniform peak-scaled gentle probe base [HIDDEN]
The gentle probe scales from the recent **delivery peak** rather than the
instantaneous (drain-depressed) sample, on every lane (queue-depth-neutral).
- `core/congestion_response/mod.rs:285` (`gentle_probe_base`),
  peak tracking `core/rate_window.rs` (`WindowedDeliveryMax`), peak cleared
  on fast-start plateau `mod.rs:103`.
- `[HIDDEN]` The "queue-depth-neutral, so it applies to every lane" contract
  lives only in the function comment.
- shares file: `congestion_response/mod.rs`.

### C4 — App-limited shared-lane attribution
An application-limited sample on a `Shared` lane means the standing delay is
another flow's queue; the controller probes instead of draining, but a
`Dedicated` lane still drains.
- Path selection: `core/congestion_response/decision.rs:80` (`select_path`,
  `shared_app_limited`), computed at `core/congestion_response/mod.rs:141-147`.
- App-limited detection: `reliable/reliable_layer.rs:1381`
  (`detect_application_limited_phases`), fed at `reliable_layer.rs:1127`.
- Share-file note: decision.rs is otherwise just the typed decision.

### C5 — Bounded dedicated fast start (windowed ACK clock)
Windowed ACK-rate ramp with a multiplicative gain, a two-window plateau exit,
and a monotone floor at the paced rate; used only on `Dedicated`.
- State machine: `core/fast_start.rs:49` (`FastStart`), `:81` (`on_ack`),
  `FastStartStep` at `:35`; gains/thresholds `:25,:31`.
- Episode wiring: `reliable/reliable_layer.rs:942-988` (only when
  `congestion_response.dedicated()` and `min_rtt().is_some()`), reset at
  `reliable_layer.rs:924`.
- `[HIDDEN]` The **shared** lane keeps the old accumulator/exit
  (`should_exit_slow_start` at `reliable_layer.rs:228`), which is
  dead for the dedicated lane; the split is only in comments.
- shares file: `reliable_layer.rs` with C6, C10, C12, L*, R3 plumbing;
  `fast_start.rs` is standalone.
- keep together: with C6 (exit policy) and C2 (lane check).

### C6 — Dedicated fast-start exit policy
- A lone iid (non-congestion) loss does **not** end the ramp; only a loss
  block or a built queue does: `reliable_layer.rs:1085-1103` (inside
  `on_rate_sample`, defined `:1037`).
- A zero-delivery / non-finite window settles at the paced rate, never zero:
  `core/fast_start.rs:117-124` + the plateau arm in `reliable_layer.rs`.
- The tracked delivery peak is cleared on plateau so the gentle probe cannot
  creep back over capacity: `reliable_layer.rs:979`.
- `[HIDDEN]` The **dropped** dedicated cold-start app-limited hold is
  documented only in the `should_exit_slow_start` comment
  (`reliable_layer.rs:222-234`).

### C7 — Idle-gap continuity authority + timer bundle
One definition of "observation gap voids continuity" and one reset clock
(`IdleGap::observe`) that resets every registered timer atomically; a new
timer must be added to the bundle.
- `traffic_shaping/core/idle_gap.rs:28` (`idle_gap_threshold`), `:63`
  (`IdleGap`), `IdleContinuity` trait `:37`; tuple impl `:53`.
- Bundle: `core/queue_growth.rs:211-218` (`break_idle_continuity` resets
  `persistent_since` + `gentle`); `GentleMode` impl `core/gentle.rs:192`.
- Restart hooks: `core/gentle.rs:187` (`restart_drain_episode`).
- `[HIDDEN]` The **set of timers** in the bundle is the contract; it is not
  listed anywhere else, and adding a timer outside the bundle silently
  escapes the reset (the module doc says this is the failure mode).
- shares file: `queue_growth.rs` with C8, C9, C2; `gentle.rs` with C2.
- keep together: the bundle and every `IdleContinuity` impl.

### C8 — Reorder-tolerant RTT floor
A shorter, *baseline-scaled* floor bucket for the reorder lane (200 ms,
scaled by the established floor) so one reordered low-RTT echo cannot pin
the propagation floor; the default keeps the 5 s sample-scaled bucket.
- `core/queue_growth.rs:26` (`RTT_MIN_BUCKET_REORDER`),
  `WindowedRttMin` `:60`, `baseline_scaled` field `:72`,
  `with_min_bucket` `:86`, `fresh_floor` `:184`.

### C9 — Reorder-tolerant gate-jitter estimator [HIDDEN]
A windowed **steady-state** jitter floor (minimum over 16 samples) beside a
trending margin; a one-sided `gate_up_rtt_var` capped at `2*up` discounts
downward RTT excursions; a 3-sample step transient and a 128-sample maturity
guard fall back to the trending margin.
- `recovery/rtt_stats.rs:10,15,25,35,44,71` (constants/field),
  `gate_rtt_var:245`, `trending_gate_rtt_var:261`, `gate_jitter:268`,
  `recent_min_rtt:215`, `record_min_and_reseed_rto:191`.
- Consumption: `reliable/reliable_layer.rs:1067` picks `gate_jitter` only on
  the reorder lane; `queue_growth.rs:243` picks trending vs steady on a floor
  step.
- `[HIDDEN]` The estimator exists to feed a policy (`QueueGrowth`) that also
  uses `smooth_rtt_var` on the stock lane; the coupling is only in comments.
- shares file: `rtt_stats.rs` with V1; `queue_growth.rs` with C7/C8.
- keep together: the filter feeding both `gate_jitter` and the RTO must stay
  with `RttStats`.

### C10 — Reorder-lane probe cap derived from the probe gain
Per-probe rate increases are capped at `current * ORDINARY_PROBE_MAX_GAIN`
on the reorder lane only, and the cap constant is derived from the probe's
own gain so it can never clip a legitimate probe.
- `core/bandwidth_probe.rs:10` (`ORDINARY_PROBE_MAX_GAIN`),
  `reliable_layer.rs:1198` (`bounded_probe_target`), applied at `:1142,:1148`.

### C11 — Pacer post-idle burst floor [HIDDEN]
The token-bucket capacity floor was lowered 64 → 16 packets so an idle link
cannot dump ~90 ms of queue; the rate-scaled term and `MAX_BURST_PACKETS`
cap are unchanged.
- `core/pacing.rs:15` (`MIN_BURST_PACKETS`), `:16`
  (`MAX_BURST_PACKETS`), `burst_capacity` `:197`.
- `[HIDDEN]` Test override that pins the legacy 64-packet floor for
  non-pacer suites: `SendPacer::set_min_burst_for_test`
  (`core/pacing.rs:68`) exposed as `pin_legacy_pacer_burst_for_test`
  (`reliable_layer.rs:568`, `transmission_layer_test_facade.rs:30`).  This
  **changes production pacing behaviour** when enabled.

### C12 — `set_send_rate` validated bridge
Every computed `f64` send rate funnels through one bridge that substitutes
the live rate for a non-positive/non-finite value (instead of panicking the
transport worker) and reapplies cwnd even on an unchanged rate.
- `reliable/reliable_layer.rs:1419`; `pkt_send_space.set_send_rate` at
  `recovery/pkt_send_space.rs:1207` (also bounds cwnd by the peer receive
  window).
- `[HIDDEN]` All call sites (fast start, drain, probe, huge-loss) rely on
  this; the "no site may carry an unchecked `PosR::new().unwrap()`"
  invariant is comment-only.

---

## Area: `traffic_shaping/redundancy` (FEC, gate, armor)

### R1 — Interactive loss-gate preset
Per-connection loss-gate sensitivity: stock opens at 5% / 16 recovery
samples, interactive at 1% / 8, with hysteresis; the preset is selected by
`FecTuning` (not globally).
- `fec_gate.rs:43` (`FecLossGateThresholds`), `:58` (`STOCK`), `:69`
  (`INTERACTIVE`), `FecConditionGate:101`, `refresh_loss`,
  `effective_loss_ratio:176`, recovery ring `record_data_send`.
- Selection: `fec_tuning.rs:103` (`loss_gate_thresholds`, `instream_flush` →
  INTERACTIVE).
- shares file: `fec_gate.rs` also holds R2; `fec_tuning.rs` holds R1 + R3
  policy selection.

### R2 — No-spare-capacity deferral (hold the open group) [HIDDEN]
A closed capacity gate does **not** destroy the open group's parity; it
holds the group open so a pending tail probe cannot wipe it.  The deferral is
a separate FEC-state latch, not a decision.
- Decision: `fec_gate.rs:182` (`decide`, `FecGateDecision::NoSpareCapacity`).
- Enactment: `fec.rs:402` (`skip_open_group_no_spare_capacity`), latch
  `fec.rs:115`, resolved in `maybe_flush_parities` (`fec.rs:587`).
- Caller: `transmission/write_half.rs:1150,1165` (`close_fec_burst` match).
- `[HIDDEN]` `skip_open_group` (destroy) vs `skip_open_group_no_spare_capacity`
  (hold) vs `skip_open_group_loss_gate` are three near-identical arms whose
  difference (deferral) is the whole feature and is comment-only.
- shares file: `fec.rs` with R3, R4, R5, R6.
- keep together: `FecConditionGate::decide` and the three `close_fec_burst`
  arms must move together.

### R3 — In-stream multi-symbol group parity + budget-adaptive hold
When the interactive tuning is on, a group accumulates up to 8 data symbols
and emits up to 4 parities inline; depth is budget-adaptive (min of 4 and the
1/3 spare share) and a zero-budget pass **holds** the group instead of
destroying it.
- `fec.rs:40` (`INSTREAM_DATA_PER_GROUP`), `:44`
  (`INSTREAM_PARITY_PER_GROUP`), `:52` (`PARITY_BUDGET_DEN`),
  `encode_data:452`, `group_data_full:491`, `maybe_flush_parities:555`
  (instream branch ~`:643`).
- Inline flush trigger: `write_half.rs:1138` (`maybe_flush_full_fec_group`),
  gate `write_half.rs:554` (`instream_group_fec_enabled` = flag AND loss
  active).
- `[HIDDEN]` **Implicit contract**: `transmission/connection.rs:180-181`
  force-enables in-stream group FEC whenever `fec_tuning.instream_flush` is
  set (`unreliable_layer.instream_group_fec || fec_tuning.instream_flush`),
  i.e. the FEC tuning silently turns on a second flag.  Not stated in
  `fec_tuning.rs`.
- shares file: `fec.rs`, `write_half.rs`, `reliable_layer.rs`.
- keep together: with T1 (the interactive capacity gate) and R8 (the armor
  ladder reads the same gate).

### R4 — Cap-forced full-group parity stash
`MAX_DATA_PER_GROUP` (20) is now actually enforced: a group that reaches it
is force-flushed (stock-ratio parity) and queued for the next budgeted flush,
so `symbol_id` never exceeds the peer decoder max.  The stash **holds** on a
tight budget rather than dropping.
- `fec.rs:452` (`encode_data` cap check), `force_flush_capped_group:471`,
  `pending_cap_parity` `:120`, stash delivery in `maybe_flush_parities:566`.
- Note: the doc at `fec.rs:443` records that `MAX_DATA_PER_GROUP` was
  "previously unenforced" — a closed gap, not an open one.

### R5 — Parity trim ownership moved to the encoder; zero-extension removed
rtp no longer truncates single-symbol interactive parity to message size and
no longer pre-zero-extends parity datagrams; the `fec` encoder trims parity to
the group's information prefix and the decoder owns shard-length extension.
- Removed transmission-layer truncation/zero-extension (commit f06a099 /
  6297b79); the decoder path is `fec.rs:746` (`FecDecoderState::decode`);
  wire-size probe `udp/layer.rs:73-81`.
- Compatibility: the stock wire is **no longer byte-identical** to the
  untrimmed release for multi-symbol groups (commit f06a099 body).
- `[HIDDEN]` The feature now lives partly in the `fec` dependency; rtp only
  relies on it.  No rtp doc records the wire-size change.

### R6 — Rejected recovered-symbol accounting
The decoder counts reconstructed symbols rejected for claiming more bytes than
their shard holds; rtp mirrors and exposes that count.
- Decoder mirror: `fec.rs:789-793`, `rejected_recovered_symbols` accessor
  `:828`; stats field `:149,:171`.
- Exposure: `traffic_shaping/adjacent/metrics.rs:354`
  (`MetricsFecCounters.rejected_recovered_symbols`), built in
  `metrics_counters` (`fec.rs:843`, field `:874`).
- `[HIDDEN]` The count deliberately stays **out of the CSV `MetricsRow`**
  (see O4); the decision is recorded only in commit a8fb21f's body.

### R7 — Retransmission armor policy: recovery vs fresh interactive tail
A recovery duplicate is gated by the session `RTP_RTX_DUP` toggle; a fresh
interactive single-symbol tail is duplicated **independently** of that toggle
(driven by the force-flush FEC tuning), and both are suppressed while the
queue is building.
- Policy: `retransmission_armor/policy.rs:23` (`RetransmissionArmor`),
  `decide:39` (`ArmorDecision`); config `retransmission_armor/config.rs:15`.
- Call site: `write_half.rs:908-910`; the fresh-tail predicate at
  `write_half.rs:900` (`single_symbol_frame || open_group_data_count()==1`).
- `[HIDDEN]` The fresh-send branch bypasses the env toggle by design ("the
  interactive lane opts in through its FEC tuning") — comment-only contract.

### R8 — Fresh-tail loss-adaptive armor copy ladder
The fresh interactive single-symbol tail emits a copy count that is
monotone-non-increasing in measured loss, compensating for whether a parity
datagram will trail the burst; the per-message budget is six datagrams.
- `write_half.rs:161` (`fresh_tail_armor_copies`), thresholds `:141,:146`,
  tiers `:117,:125,:131,:137`.
- `[HIDDEN]` Test-only override `fresh_tail_armor_copies_override`
  (`WriteHalfSettings` `write_half.rs:67`, `UnreliableLayer`
  `transmission_layer.rs:137`), set through
  `set_fresh_tail_armor_copies_override_for_test` (`write_half.rs:608`) and
  by the ignored probe (`socket/stream.rs:1316`).  It replaces the whole
  ladder; `None` in production.
- keep together: with R1/R3 (the ladder reads `fec_gate.effective_loss_ratio`
  and `loss_active` at `write_half.rs:984-988`).

---

## Area: `traffic_shaping/recovery` (fast loss)

### V1 — Fast-loss arming from the queue-independent lifetime min_rtt
The evidence-gated fast-loss path arms on `rttvar < min_rtt` as well as the
srtt-relative gate, so bulk queueing (which inflates srtt and rttvar) does not
disarm it; the observed-reordering disable stays bounded.
- Gate: `rtt_stats.rs:312` (`fast_loss_armed_against_min_rtt`), `min_rtt`
  captured at `rtt_stats.rs:180`; `recent_min_rtt` reorder window
  (`rtt_stats.rs:10,215`).
- Composite decision: `pkt_send_space.rs:246` (`fast_loss_armed` OR), doc on
  `rto.rs`.
- shares file: `rtt_stats.rs` with C9.
- keep together: the filter feeding both V1 and the RTO/gate estimators.

### V2 — Pre-existing: `RTP_JITTER_CAP` fast reorder window (out of scope)
Not a since-v0.0.89 feature (only `pkt_send_space.gate_jitter()` and the V1 OR
were added).  Listed here because it is a production env toggle the reader
must know about:
- `pkt_send_space.rs:69-74` (`jitter_cap_enabled` / `jitter_cap_from_env`),
  `:139,:201`; window `rto.rs:99` (`fast_reorder_window`).
- **Default is ON**: `jitter_cap_enabled` returns true when the var is unset
  (`is_none_or(|v| v != "0" && !v.eq_ignore_ascii_case("false"))`); only an
  explicit `0`/`false` disables it.
- Test constructor `with_jitter_cap` `pkt_send_space.rs:284`.

---

## Area: `delivery/frame`

### D1 — `FrameMode::allow_reorder` + presets
A receiver-side, opt-in fast-forward flag added to `FrameMode`, with
`enabled_reordering()` / `with_reorder()` presets; default off.
- `delivery/frame/mod.rs:51` (`FrameMode`), `:55` (`allow_reorder`), `:68`
  (`enabled_reordering`), `:76` (`with_reorder`); env default `:87`
  (`RTP_FRAME_DELIVERY`).
- `[HIDDEN]` `RTP_FRAME_DELIVERY` default is the stock `enabled:false` with
  `allow_reorder:false`; the env function feeds `ConnectConfig::default()`.

### D2 — Receiver-side fast-forward delivery gate
A complete frame starting past an unrepaired in-order hole may be delivered
while `next` stays pinned at the hole; the frame's slots are tombstoned so the
cursor collapses them once the hole fills.  Delivery never changes ACKs.
- `delivery/frame/recv.rs:278` (`pop_complete_frame`, `allow_reorder` param),
  gate at `recv.rs:340` (`frame_start != front && !allow_reorder`).
- Window wrapper: `recv_queue/pkt_recv_space.rs:167`
  (`pop_complete_frame_with_reorder`), default `:154`
  (`pop_complete_frame`); tombstone collapse `:182,:190`.
- Reliable-layer dispatch: `reliable_layer.rs:1336-1350`
  (`recv_frame_buf`, `frame_delivery.allow_reorder`).
- shares file: `recv.rs` with D3; `pkt_recv_space.rs` with the (pre-existing)
  scan-resume/buffer-reuse logic.
- keep together: D1 flag, D2 gate, and `reliable_layer` dispatch.

### D3 — Tombstone-capture abandonment refinement
Only a tombstone landing **exactly** on the in-progress frame's next
continuation abandons it; a tombstone *beyond* the next continuation (from a
frame already fast-forwarded) resets the in-progress run instead, so the
earlier frame can still be repaired.
- `delivery/frame/recv.rs:213-243`; abandoned-frame tombstoning at
  `recv.rs:298-306`.
- `[HIDDEN]` The boundary (exactly-on-continuation vs beyond) is comment-only;
  the test `tombstone_on_an_in_progress_frames_continuation_still_abandons`
  pins it in `pkt_recv_space.rs`.

### D4 — `accept/connect_frame_delivery` honors `allow_reorder` [HIDDEN]
`accept_frame_delivery` / `connect_frame_delivery` force `enabled = true` but
now **preserve** the config's `allow_reorder` (previously they replaced the
whole `FrameMode::enabled()` and silently dropped it).
- `udp.rs:574` (`force_frame_delivery`), call site
  `udp.rs:369,384` (`accept_frame_delivery_configured`).
- `[HIDDEN]` This is a silent behaviour fix: no README/changelog mention; the
  only evidence is the helper doctext and the changed call site.

---

## Area: `transmission` (write half)

### T1 — Interactive FEC capacity gate
A second "genuinely spare capacity" predicate used by force-flush tunings:
it does **not** require the staging buffer to be empty (so a mid-burst
full-group flush can fire) and ignores a pending retransmit/tail probe, but
still requires send-window room, zero application write waiters, and no queue
growth.  Stock/bulk keeps the strict predicate.
- `reliable/reliable_layer.rs:479` (`fec_has_spare_capacity_interactive`),
  strict `:458` (`fec_has_spare_capacity`), tail gate `:443`.
- Selection: `write_half.rs:576` (`fec_gate_decision`, `fec_instream_flush`).
- `[HIDDEN]` The reason (batched interactive lane is always repairing) is
  comment-only.

### T2 — FEC parity burst WouldBlock residual queue + FIFO retry
A parity burst interrupted by `WouldBlock` retains its unsent tail (moved,
never copied) and retries FIFO before generating new parity; at most one
group's parity is ever held, bounding the queue.
- `write_half.rs:74` (`pending_fec_parity`), `flush_fec_parities:1184`,
  `drain_pending_fec_parity:1219`.
- `[HIDDEN]` Test accessor `pending_fec_parity_len_for_test`
  (`write_half.rs:1241`).
- keep together: the flush chain (`drain_pending` → budgeted
  `maybe_flush_parities` → re-drain) is IO-ordered and must stay one async
  orchestrator.

---

## Area: `reliable` (reliable layer orchestration)

### L1 — Fast-start / lane dispatch inside the layer
`recv_ack_pkt` owns the ACK-clocked fast-start episode and the
dedicated/shared branch; `on_rate_sample` owns the dedicated exit.
- `reliable_layer.rs:942-988` (episode), `:1085-1103` (exit),
  `:1119-1190` (decision application), `set_smooth_send_rate` `:1206`.
- shares file: `reliable_layer.rs` is the hot file hosting C5/C6/C10/C12,
  L2, L3, R3 plumbing, plus pre-existing CC/metrics.

### L2 — FEC interactive capacity predicate
See T1 (defined here, consumed by the write half).

### L3 — Frame reorder dispatch
See D2 (`recv_frame_buf` dispatch).

---

## Area: `udp`

### U1 — `congestion_lane` in connect/accept config
`ConnectConfig`/`AcceptConfig` gain a `congestion_lane` field threaded into
`UnreliableLayer` and the session.
- `udp.rs:438` (`AcceptConfig`), `:494` (`ConnectConfig`), defaults
  `:467,:524`, plumbing `:563,:664`.
- shares file: `udp.rs` with D4.

### U2 — Frame-delivery entry points preserve `allow_reorder`
See D4.

---

## Area: `metrics/observability`

### O1 — Observation schema version bump 28 → 29 [HIDDEN compatibility rule]
- `traffic_shaping/adjacent/metrics.rs:12` (`SCHEMA_VERSION = 29`).
- Stamped on every typed observation: `transmission/observability.rs:102`
  (`MetricsObservation`), logged `:122` (`MetricsRow`).
- `[HIDDEN]` There is no documented compatibility rule.  The bump is
  additive (new `MetricsFecCounters` field); consumers matching on the exact
  version must accept 29, and there is no migration note.  Also note the CSV
  `MetricsRow` header width is pinned **separately** (see O4) and was not
  bumped.

### O2 — `MetricsFecCounters.rejected_recovered_symbols`
- `adjacent/metrics.rs:354`; produced by `fec.rs:874`.
- See R6.

### O3 — `RetransmissionArmorDuplicate` widened to fresh interactive tail
The event doc now covers both the env-gated recovery duplicate and the
force-flush fresh interactive tail.
- `adjacent/metrics.rs:200-204`; emitted `write_half.rs:999`.

### O4 — CSV `MetricsRow` not extended [HIDDEN gap]
The rejected-recovered counter (and the widened armor event) were deliberately
kept out of the CSV trace schema because it changes the pinned header width
and every reader.  The decision is recorded only in commit a8fb21f's body; the
code has no TODO.  This is a genuine planned-but-unimplemented gap.

---

## Area: framing/wire

No in-scope change.  `delivery/frame/wire.rs`, `codec.rs`, `ack/*`,
`obfuscate/*` were untouched since v0.0.89 (the frame `FRAME_DATA_TS`/`DATA_TS`
framing predates it).  The only wire-adjacent in-scope change is R5 (parity
sizing), which is owned by the `fec` dependency.

---

## Area: testing support (added since v0.0.89)

### X1 — Deterministic impairment + FEC-tuned wrappers [test infra]
- `udp/testing.rs`: `BurstLoss` (`:120`), `BurstLossyWrite` (`:203`),
  `DelayedRead` (`:231`), and the `wrap_*_with_mss_and_fec_tuning` helpers.
- These are `pub` test helpers in a production crate; they do not affect the
  production path.

### X2 — Ignored in-process probe harness [test infra]
- `socket/stream.rs`: `probe_single_symbol_interactive_fec_repair` (`:863`),
  `probe_fresh_tail_armor_latency` (`:965`),
  `probe_fresh_tail_burst_loss_latency` (`:1082`),
  `probe_armor_copy_cell` (`:1256`, reads `ARMOR_*` env vars).
- All `#[ignore]`d; used to sweep the armor copy count / FEC repair.

### X3 — Test hooks / injectable knobs (full list)
These override production decisions when compiled/enabled:
- `SendPacer::set_min_burst_for_test` / `ReliableLayer::pin_legacy_pacer_burst_for_test`
  (C11) — changes the pacer burst floor.
- `fresh_tail_armor_copies_override` + `set_fresh_tail_armor_copies_override_for_test`
  (R8) — replaces the loss-adaptive ladder.
- `PktSendSpace::with_jitter_cap` (V2) — forces `RTP_JITTER_CAP`.
- `PktSendSpace::force_full_walk_ack_sync` / `full_walk_ack_sync`
  (pre-existing scoped-sync reference arm).
- `ReliableLayer::set_congestion_loss_ratio_for_test`,
  `set_queue_building_for_test`, `enqueue_send_data_for_test`,
  `set_cwnd_for_test` (`reliable_layer.rs:530,538,547,559`).
- `CongestionResponse::set_queue_building_for_test`
  (`congestion_response/mod.rs:314`); `congestion_response.queue_growth()`,
  `delivery_peak()`.
- `QueueGrowth::set_building`, `replace_floor`, `update_floor`
  (`queue_growth.rs`).
- `FecConditionGate::with_thresholds` (R1) — preset injection.
- `FecEncoderState`/`FecDecoderState` `#[cfg(test)]` accessors
  (`small_group_parity_count`, `parity_sent`, `rejected_recovered_symbols`,
  …).

---

## Environment variables read in production builds

| Var | Where | Effect | In scope |
|-----|-------|--------|----------|
| `RTP_JITTER_CAP` | `recovery/pkt_send_space.rs:74` | fast reorder window; **default ON** unless `0`/`false` | pre-existing (V2) |
| `RTP_RTX_DUP` | `retransmission_armor/config.rs:35` | recovery armor duplicate toggle; default off | in scope (R7) |
| `RTP_FRAME_DELIVERY` | `delivery/frame/mod.rs:88` | default `FrameMode`; default off | in scope (D1) |
| `RTP_INSTREAM_GROUP_FEC` | `redundancy/mod.rs:9` | in-stream group FEC; default off | in scope (R3) |
| `RTP_MAX_DIVERSITY` / legacy `RTP_MINDIV` | `fec_tuning.rs:121` | feeds `fec_tuning_from_env`; default stock | in scope (R1/R3) |
| `RTP_DEBUG_SEND` | `debug.rs:28` | per-packet `eprintln` tracing; read once, cached | pre-existing |

---

## Doc / code mismatches (flagged, not features)

- **M1 — `fec_tuning.rs` module doc vs `udp.rs` defaults.**  The module doc
  (`fec_tuning.rs:16`) says `RTP_MAX_DIVERSITY=1` "only feeds the default for
  A/B comparison — it is never read as the live setting, so it cannot
  silently apply to every connection in the process", and the function doc
  (`fec_tuning.rs:113`) says it is read "once at process startup".  In fact
  `ConnectConfig::default()` (`udp.rs:465`) and `AcceptConfig::default()`
  (`:522`) call `fec_tuning_from_env()` on **every config construction**, so
  the env var *is* the live tuning for any caller that does not override it,
  and it is read per-config, not once per process.  Same pattern for
  `RetransmissionArmorConfig::default()` (`config.rs:35`) whose doctext says
  the config `Default` reads `RTP_RTX_DUP` "exactly once".
- **M2 — `pkt_send_space.rs` fast-loss doc vs the `RTP_JITTER_CAP` path.**
  The `fast_loss_armed` doc (`:241`) says "Always-on when armed; there is no
  env toggle — the gates are the safety."  That is true of the fast-loss
  *declaration* gate, but a separate `RTP_JITTER_CAP` toggle changes the
  reorder window that schedules the retransmit; the two are described in
  different files without a cross-reference.
- **M3 — `fec.rs` `MAX_DATA_PER_GROUP` doc.**  The doc at `fec.rs:443`
  explicitly says the cap was "documented as a forced-flush point but
  previously unenforced"; R4 now enforces it.  Not a live mismatch, but the
  doc is written as a correction of an earlier claim.

## Planned-but-unimplemented gaps observed in the code

- **G1 — CSV trace schema.**  a8fb21f/37f0fa8 deliberately deferred adding
  `rejected_recovered_symbols` (and the widened armor event) to `MetricsRow`;
  only the typed observation schema gained it (O4).  No TODO in code.
- **G2 — Dedicated cold-start app-limited hold.**  Explicitly "evaluated and
  dropped" (`reliable_layer.rs:225-231`); the shared-lane path is the only
  one that exits on app-limited samples.
- **G3 — Defensive/unreachable arms.**  `delivery/frame/recv.rs:383` (generic
  fallback "unreachable after a real scan"); the single-packet fast-path
  fall-through at `recv.rs`; `write_half.rs:326` (`measure_failed` encode
  path "unreachable with an MSS-sized buffer"); `fec.rs:689` defensive
  `data_count > INSTREAM_DATA_PER_GROUP` skip.  These are dead branches kept
  as guard rails.
- **G4 — `FecState` `small_group_parity_count` depth cap.**  `fec.rs:32`
  (`MAX_INTERACTIVE_PARITY_DEPTH`) clamps the depth to the decoder group size;
  the deeper-diversity `max_diversity()` (depth 3) is only safe because of
  this clamp — the coupling is not documented.

---

## Shared-file matrix (features that must be separated by the extraction)

| File | In-scope features currently sharing it |
|------|----------------------------------------|
| `reliable/reliable_layer.rs` | C5, C6, C10, C12, C4 (detection), L1, L3, T1, R7/R8 (queue-building + spare-capacity reads) |
| `transmission/write_half.rs` | R3, R7, R8, T1, T2, R2 (call site) |
| `traffic_shaping/core/congestion_response/mod.rs` | C2, C3, C4 |
| `traffic_shaping/core/gentle.rs` | C2, C7 |
| `traffic_shaping/core/queue_growth.rs` | C2, C7, C8, C9 (floor-step margin) |
| `traffic_shaping/recovery/rtt_stats.rs` | C9, V1 |
| `traffic_shaping/redundancy/fec.rs` | R2, R3, R4, R5, R6 |
| `traffic_shaping/redundancy/fec_gate.rs` | R1, R2 |
| `traffic_shaping/redundancy/fec_tuning.rs` | R1 (gate selection), R3 (in-stream policy) |
| `delivery/frame/recv.rs` | D2, D3 |
| `delivery/frame/mod.rs` | D1 (flag + env) |
| `traffic_shaping/adjacent/metrics.rs` | O1, O2, O3 |

## Mutually interdependent features that must stay together

- **C1 + C2 (+ C3)** — the lane intent is read by the drain/probe/exit
  branches; splitting the enum from its consumers invents a new authority.
- **C5 + C6** — the fast-start ramp and its exit policy share the episode
  state and the delivery-peak clearing.
- **C7 + its bundle** — `IdleGap` + every `IdleContinuity` impl (persistent
  queue timer, drain episode) must move as one; a lone timer escapes the
  reset.
- **C8 + C9 + V1** — `RttStats`/`WindowedRttMin` estimators feed the reorder
  floor, the gate jitter, and the fast-loss arming; extract policy, not the
  filters.
- **R1 + R2 + R3** — the gate decision, the deferral enactment, and the
  in-stream budget hold are one state machine.
- **R7 + R8** — the armor decision and the copy ladder both read
  `FecConditionGate` loss state; keep the policy + ladder together.
- **T2's flush chain** — `drain_pending_fec_parity` → `maybe_flush_parities`
  → re-hold is IO-ordered; only the data structures/policy may be extracted,
  the async orchestration stays one place.
- **`FecState` encoder/decoder + `Arc<Stats>`** — the two actor halves share
  the counters (R6); keep them together.
- **D1 + D2 + D3** — the flag, the delivery gate, and the abandonment rule
  are one fast-forward contract; D4 is the entry-point fix that must move
  with D1.
