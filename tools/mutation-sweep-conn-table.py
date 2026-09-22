#!/usr/bin/env python3
"""Mutation sweep over rtp's session-admission ledger.

The per-connection tables themselves (the `HashMap` keyed by peer address or
dispatch key, the four-state probe limiter, mpudp's path-set backlog) live in
the read-only `udp_listener` / `mpudp` git dependencies. What rtp owns is the
*admission ledger* it feeds them: the `session_count` counter, the
`>= max_connections` predicate that turns it into a `DispatchPolicy`
(`Create` = admit an unknown key, `ExistingOnly` = drop it), the counter's
acquire on accept and release on session exit, and the `max_session_conns`
forwarded to mpudp's path-set backlog. A boundary error in any of those does
not crash: it leaks a slot, unbounds the cap, or rides a probe into the table.

Run:

    python3 tools/mutation-sweep-conn-table.py run --mode all
    python3 tools/mutation-sweep-conn-table.py run --mode existing
    python3 tools/mutation-sweep-conn-table.py run --mode new
    python3 tools/mutation-sweep-conn-table.py run --mutants udp-pred-eq

`--mode all` runs the whole libtest binary, `--mode existing` runs it with the
tests added by this change skipped (`--skip`), and `--mode new` runs each of
them alone (`--exact`), which is how a mutant's catch is attributed to a new
test rather than to the pre-existing suite.

Process hygiene: the libtest binary is run directly (never through `cargo`) in
its own session (`start_new_session=True`), under an in-script deadline. A
`timeout cargo ...` kills cargo and reparents the test binary to init, where it
keeps holding an ephemeral UDP port and poisons later mutants; here the binary
itself is the killed process, and after every mutant the script sweeps the
machine for any process whose command line still names the binary path and
reports the stray count. A hung mutant is reported as `HUNG`, never as a pass.

Restore discipline: the pristine bytes are written back with
`os.utime(path, None)` so the mtime is bumped past the mutant's build. An
mtime-preserving restore (or any restore that leaves the source older than the
build) makes cargo reuse the *mutant* binary, which shows up as a pristine tree
that looks broken. After every restore the script asserts the rebuilt binary's
mtime is at least as new as the restored source, and asserts the file's SHA-256
is back to the recorded pristine digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Optional shared (warm) target directory; cargo's own default is used when unset.
TARGET_DIR = os.environ.get("RTP_MUTATION_TARGET_DIR")

# Full libtest paths of the tests this change adds, used by `--mode existing`
# (skip them) and `--mode new` (run each alone). Keep in sync with the source.
NEW_TESTS: list[str] = [
    "udp::tests::the_session_cap_re_binds_after_a_soft_overshoot",
    "udp::tests::an_abandoned_accept_holds_no_cap_slot",
    "keyed_udp::tests::an_exited_keyed_session_frees_its_cap_slot",
    "keyed_udp::tests::an_overshot_keyed_ledger_still_refuses_unknown_keys",
    "mpudp::tests::a_session_wider_than_the_cap_is_refused",
]

UDP_PRED = "let policy = if session_count.load(Ordering::Relaxed) >= max_connections {"
UDP_ARMS = """                        let policy = if session_count.load(Ordering::Relaxed) >= max_connections {
                            DispatchPolicy::ExistingOnly
                        } else {
                            DispatchPolicy::Create
                        };
"""
UDP_ARMS_SWAPPED = """                        let policy = if session_count.load(Ordering::Relaxed) >= max_connections {
                            DispatchPolicy::Create
                        } else {
                            DispatchPolicy::ExistingOnly
                        };
"""
UDP_SLOT_BLOCK = """    if handshake {
        server_opening_handshake(&mut unreliable_layer).await?;
    }
    // Occupy a cap slot only when the accept succeeds (past the handshake, so
    // failed handshakes never consume a slot); the session's on-exit hook
    // releases it when the supervisor task ends.
    session_count.fetch_add(1, Ordering::SeqCst);
"""
UDP_SLOT_BLOCK_EARLY = """    session_count.fetch_add(1, Ordering::SeqCst);
    if handshake {
        server_opening_handshake(&mut unreliable_layer).await?;
    }
"""

KEYED_PRED = "let policy = if session_count.load(Ordering::Relaxed) >= max_connections {"
KEYED_ARMS = """                let policy = if session_count.load(Ordering::Relaxed) >= max_connections {
                    DispatchPolicy::ExistingOnly
                } else {
                    DispatchPolicy::Create
                };
"""
KEYED_ARMS_SWAPPED = """                let policy = if session_count.load(Ordering::Relaxed) >= max_connections {
                    DispatchPolicy::Create
                } else {
                    DispatchPolicy::ExistingOnly
                };
"""

# (id, file, anchor, replacement, one-line intent)
MUTANTS: list[tuple[str, str, str, str, str]] = [
    (
        "udp-pred-gt",
        "src/udp.rs",
        UDP_PRED,
        UDP_PRED.replace(">= max_connections", "> max_connections"),
        "admit one unknown source past the cap (`>` for `>=`)",
    ),
    (
        "udp-pred-eq",
        "src/udp.rs",
        UDP_PRED,
        UDP_PRED.replace(">= max_connections", "== max_connections"),
        "cap stops re-binding after the documented soft overshoot (`==` for `>=`)",
    ),
    (
        "udp-arms-swap",
        "src/udp.rs",
        UDP_ARMS,
        UDP_ARMS_SWAPPED,
        "miss disposition inverted: unknown sources dropped below the cap",
    ),
    (
        "udp-slot-acquire-drop",
        "src/udp.rs",
        "    session_count.fetch_add(1, Ordering::SeqCst);\n",
        "",
        "accepted sessions never occupy a slot: the cap never binds",
    ),
    (
        "udp-slot-acquire-early",
        "src/udp.rs",
        UDP_SLOT_BLOCK,
        UDP_SLOT_BLOCK_EARLY,
        "slot acquired before the handshake: an aborted/failed accept leaks a slot",
    ),
    (
        "udp-slot-release-drop",
        "src/udp.rs",
        "                session_count.fetch_sub(1, Ordering::SeqCst);\n",
        "",
        "exited sessions never release their slot: permanent capacity leak",
    ),
    (
        "keyed-pred-gt",
        "src/keyed_udp.rs",
        KEYED_PRED,
        KEYED_PRED.replace(">= max_connections", "> max_connections"),
        "admit one unknown key past the cap (`>` for `>=`)",
    ),
    (
        "keyed-pred-eq",
        "src/keyed_udp.rs",
        KEYED_PRED,
        KEYED_PRED.replace(">= max_connections", "== max_connections"),
        "cap stops re-binding after the documented soft overshoot (`==` for `>=`)",
    ),
    (
        "keyed-arms-swap",
        "src/keyed_udp.rs",
        KEYED_ARMS,
        KEYED_ARMS_SWAPPED,
        "miss disposition inverted: unknown keys dropped below the cap",
    ),
    (
        "keyed-slot-acquire-drop",
        "src/keyed_udp.rs",
        "        session_count.fetch_add(1, Ordering::SeqCst);\n",
        "",
        "accepted keys never occupy a slot: the cap never binds",
    ),
    (
        "keyed-slot-release-drop",
        "src/keyed_udp.rs",
        "                session_count.fetch_sub(1, Ordering::SeqCst);\n",
        "",
        "exited keys never release their slot: permanent capacity leak",
    ),
    (
        "mpudp-cap-ignored",
        "src/mpudp.rs",
        "MpUdpListener::bind(addrs, max_session_conns, DISPATCHER_BUF_SIZE)",
        "MpUdpListener::bind(addrs, NonZeroUsize::new(usize::MAX).unwrap(), DISPATCHER_BUF_SIZE)",
        "the caller's path-set cap is discarded: mpudp never refuses a wide session",
    ),
]


def sha256(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def cargo_build(log) -> str:
    """Build the rtp lib test target, return the libtest executable path."""
    proc = subprocess.run(
        [
            "cargo",
            "test",
            "-p",
            "rtp",
            "--lib",
            "--no-run",
            "--message-format=json",
        ],
        cwd=ROOT,
        env={**os.environ, **({"CARGO_TARGET_DIR": TARGET_DIR} if TARGET_DIR else {})},
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if proc.returncode != 0:
        log.write(proc.stderr[-4000:])
        raise RuntimeError(f"mutant failed to compile (exit {proc.returncode})")
    exe = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("reason") == "compiler-artifact" and msg.get("executable"):
            exe = msg["executable"]
    if exe is None:
        raise RuntimeError("cargo produced no test executable")
    return exe


def stray_processes(exe: str) -> list[str]:
    out = subprocess.run(
        ["ps", "-axo", "pid=,command="], capture_output=True, text=True, timeout=60
    ).stdout
    return [line for line in out.splitlines() if exe in line]


def run_binary(exe: str, args: list[str], deadline_s: int) -> tuple[str, str, list[str]]:
    """Run the libtest binary in its own session. Returns (verdict, output, failed)."""
    proc = subprocess.Popen(
        [exe, f"--test-threads={max(2, os.cpu_count() or 2)}", *args],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    hung = False
    try:
        out = proc.communicate(timeout=deadline_s)[0]
    except subprocess.TimeoutExpired:
        hung = True
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        out = proc.communicate()[0]
    failed = re.findall(r"^test (\S+) \.\.\. FAILED", out or "", re.M)
    if hung:
        verdict = "HUNG"
    elif proc.returncode != 0:
        verdict = "KILLED"
    else:
        verdict = "SURVIVED"
    return verdict, out or "", failed


def mode_args(mode: str) -> list[list[str]]:
    if mode == "all":
        return [[]]
    if mode == "existing":
        return [[f"--skip={name}" for name in NEW_TESTS]] if NEW_TESTS else [[]]
    if mode == "new":
        if not NEW_TESTS:
            raise SystemExit("--mode new needs NEW_TESTS populated")
        return [[f"--exact", name] for name in NEW_TESTS]
    raise SystemExit(f"unknown mode {mode}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["run", "list"])
    ap.add_argument("--mode", default="all", choices=["all", "existing", "new"])
    ap.add_argument("--mutants", default="", help="comma-separated subset of ids")
    ap.add_argument("--deadline", type=int, default=180, help="seconds per mutant run")
    ap.add_argument(
        "--log-dir",
        default="",
        help="write each mutant run's libtest output to <dir>/<id>.<mode>.log",
    )
    args = ap.parse_args()

    if args.command == "list":
        for mid, path, _, _, note in MUTANTS:
            print(f"{mid}\t{path}\t{note}")
        return 0

    wanted = [m for m in args.mutants.split(",") if m]
    selected = [m for m in MUTANTS if not wanted or m[0] in wanted]
    if not selected:
        raise SystemExit("no mutants selected")

    # Pre-flight: the anchor must occur exactly once, and the pristine tree must
    # pass, before any mutation is written.
    pristine: dict[str, str] = {}
    for mid, path, anchor, _, _ in MUTANTS:
        full = os.path.join(ROOT, path)
        if full not in pristine:
            pristine[full] = sha256(full)
        body = open(full, encoding="utf-8").read()
        count = body.count(anchor)
        if count != 1:
            raise SystemExit(f"{mid}: anchor occurs {count} times in {path} (need 1)")

    print(f"# mode={args.mode} mutants={len(selected)} deadline={args.deadline}s")
    pre_exe = cargo_build(sys.stderr)
    pre_verdict, _, pre_failed = run_binary(pre_exe, mode_args(args.mode)[0], args.deadline)
    print(f"# pristine pre-flight: {pre_verdict} failed={pre_failed}")
    if pre_verdict != "SURVIVED":
        print("!! pristine tree does not pass; aborting before any mutation")
        return 2

    rows = []
    for mid, path, anchor, replacement, note in selected:
        full = os.path.join(ROOT, path)
        original = open(full, encoding="utf-8").read()
        assert original.count(anchor) == 1, mid
        write_started = time.time()
        with open(full, "w", encoding="utf-8") as f:
            f.write(original.replace(anchor, replacement))
        try:
            exe = cargo_build(sys.stderr)
            if os.path.getmtime(exe) < write_started - 1:
                raise RuntimeError(
                    "cargo reused a stale test binary (source mtime older than build)"
                )
            verdicts = []
            failed_all: set[str] = set()
            outputs = []
            for run_args in mode_args(args.mode):
                verdict, out, failed = run_binary(exe, run_args, args.deadline)
                verdicts.append(verdict)
                failed_all.update(failed)
                outputs.append(f"$ {' '.join(run_args)}\n{out}")
            if args.log_dir:
                os.makedirs(args.log_dir, exist_ok=True)
                with open(
                    os.path.join(args.log_dir, f"{mid}.{args.mode}.log"),
                    "w",
                    encoding="utf-8",
                ) as log:
                    log.write("\n".join(outputs))
            if "HUNG" in verdicts:
                verdict = "HUNG"
            elif "KILLED" in verdicts:
                verdict = "KILLED"
            else:
                verdict = "SURVIVED"
        except RuntimeError as exc:
            verdict, failed_all = "COMPILE-ERROR", set()
            print(f"  {mid}: {exc}")
        finally:
            with open(full, "w", encoding="utf-8") as f:
                f.write(original)
            os.utime(full, None)
            if sha256(full) != pristine[full]:
                raise SystemExit(f"{mid}: restore did not reproduce the pristine bytes")
            # Force the restore to be rebuilt before the next mutant so a stale
            # mutant binary can never be measured as a pristine tree.
            cargo_build(sys.stderr)

        strays = stray_processes(exe)
        for line in strays:
            pid = line.split()[0]
            try:
                os.kill(int(pid), signal.SIGKILL)
            except (ProcessLookupError, ValueError):
                pass
        leftover = stray_processes(exe)
        row = {
            "id": mid,
            "file": path,
            "note": note,
            "verdict": verdict,
            "failed": sorted(failed_all),
            "strays": len(strays),
            "strays_left": len(leftover),
        }
        rows.append(row)
        print(json.dumps(row))

    print("\n# SUMMARY")
    for row in rows:
        print(
            f"{row['verdict']:<13} {row['id']:<26} strays={row['strays']} "
            f"left={row['strays_left']} failed={row['failed']}"
        )
    bad = [r for r in rows if r["strays_left"]]
    if bad:
        print(f"!! stray processes remain for {[r['id'] for r in bad]}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
