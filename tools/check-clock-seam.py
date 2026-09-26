#!/usr/bin/env python3
"""Fail when opening-handshake decision code reads the clock directly.

Rule: the opening handshake arms every leg deadline and re-anchors every retry
from the current instant. That read goes through the `clock::ClockRef` seam
(`src/clock.rs`), never `std::time::Instant::now()` at the decision site. The
seam keeps the boundary reachable from a test: a connection carrying the system
clock behaves exactly as before, while a test can install a clock that follows
a paused runtime, so a multi-second deadline is driven instead of slept
through. A direct read re-hides the deadline behind wall time, which is exactly
what the seam exists to prevent -- and it re-introduces the multi-second cost
this crate's always-run tier was shortened to remove.

The scan is the opening module only: that is the path the seam was introduced
for, an allowlist entry rather than a wider scan keeps every other module's
direct reads out of this gate until they are migrated too, and `src/clock.rs`
is the seam implementation and is not in the scanned path. `#[cfg(test)]` items
are skipped there, because a test's own clock reads are not decision code.

A second section guards the rows whose wall-clock wait was removed by driving
their clock or their decision instant. Those rows are test bodies, so they are
not covered by the decision-code scan; the table below records, per row, the
snippet its driver must keep and the wall-clock wait it must not regain. A row
that goes back to sleeping to its deadline is a cost regression the tier would
otherwise pay silently, and the guard reports it instead.

Run from the rtp repo root:

    python3 tools/check-clock-seam.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO / "src/traffic_shaping/control/handshake/opening"

# A direct read of the current time, or a duration measured from a stored
# instant.
TIME_READ = re.compile(
    r"Instant::now|SystemTime::now|Utc::now|Local::now|\.elapsed\(\)"
)

# (relative path, line snippet, expected occurrence count, reason).
#
# Categories:
#   (a) the seam implementation (never in SOURCE_DIR)
#   (c) a legitimate non-decision use (timestamp, metric, pure measurement,
#       tokio timer primitive)
#   (b) a decision site that belongs behind the seam but has not been migrated;
#       the reason must say what migration would require
ALLOWLIST: list[tuple[str, str, int, str]] = []

# (relative path, test fn, snippet the driver must keep, wall-clock wait the
# row must not regain, reason).
DRIVEN_ROWS: list[tuple[str, str, str, str, str]] = [
    (
        "src/traffic_shaping/control/handshake/opening/tests.rs",
        "the_opening_completes_at_the_fields_worst_round_trip",
        "start_paused = true",
        'flavor = "multi_thread"',
        "the opening's leg deadlines are driven by the ClockRef seam, so the row "
        "must run on a runtime it can drive; the multi-thread flavour rejects "
        "start_paused and puts the two 3205 ms legs back on the wall clock",
    ),
    (
        "src/transmission/transmission_layer_test_facade.rs",
        "proactive_watchdog_aborts_locally_before_best_effort_kill_completes",
        "send_pkts_at",
        "tokio::time::sleep",
        "the watchdog deadline is crossed by passing the decision instant into "
        "the send pass; sleeping to it costs the tier 1.2 s of wall clock for a "
        "wait no assertion measures",
    ),
]


def item_body(path: Path, fn_name: str) -> str | None:
    """The brace-balanced body of `fn_name` in `path`, or `None`."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not re.search(r"\bfn " + re.escape(fn_name) + r"\s*[(<]", line):
            continue
        # Include the attribute lines above the fn (`#[tokio::test(...)]`),
        # so a row's flavour or pause setting is part of its checked body.
        start = index
        while start > 0 and (
            lines[start - 1].lstrip().startswith("#[")
            or lines[start - 1].lstrip().startswith("//")
        ):
            start -= 1
        depth = 0
        started = False
        body: list[str] = []
        for cursor in range(start, len(lines)):
            for char in lines[cursor]:
                if char == "{":
                    depth += 1
                    started = True
                elif char == "}":
                    depth -= 1
            body.append(lines[cursor])
            if started and depth <= 0:
                return "\n".join(body)
        return "\n".join(body)
    return None


def test_spans(text: str) -> list[tuple[int, int]]:
    """Line ranges [start, end] covered by `#[cfg(test)]`-attributed items."""
    lines = text.splitlines()
    spans: list[tuple[int, int]] = []
    for index, raw in enumerate(lines):
        marker = raw.find("#[cfg(test)]")
        if marker == -1:
            continue
        # Find the item that follows the attribute; it may share the line.
        if raw[marker + len("#[cfg(test)]") :].strip():
            item = index
        else:
            item = index + 1
            while item < len(lines) and (
                not lines[item].strip()
                or lines[item].lstrip().startswith("#[")
                or lines[item].lstrip().startswith("//")
            ):
                item += 1
        if item >= len(lines):
            continue
        end = item
        if "{" in lines[item]:
            depth = 0
            cursor = item
            started = False
            while cursor < len(lines):
                for char in lines[cursor]:
                    if char == "{":
                        depth += 1
                        started = True
                    elif char == "}":
                        depth -= 1
                end = cursor
                cursor += 1
                if started and depth <= 0:
                    break
        else:
            while end < len(lines) and ";" not in lines[end]:
                end += 1
        spans.append((index, end))
    return spans


def production_lines(path: Path) -> list[tuple[int, str]]:
    """Lines of `path` outside any `#[cfg(test)]` item, 1-indexed."""
    text = path.read_text(encoding="utf-8")
    spans = test_spans(text)
    out: list[tuple[int, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if any(start < number <= end for start, end in spans):
            continue
        out.append((number, line))
    return out


def source_files() -> list[Path]:
    return sorted(
        path
        for path in SOURCE_DIR.rglob("*.rs")
        if path.name != "tests.rs" and path.parent.name != "tests"
    )


def strip_comment(line: str) -> str:
    """The code part of `line`, with a trailing `//` comment removed."""
    return line.split("//", 1)[0]


def scan() -> list[tuple[str, int, str, str]]:
    """All direct time reads in production code: (path, line, text, snippet)."""
    hits: list[tuple[str, int, str, str]] = []
    for path in source_files():
        relative = path.relative_to(REPO).as_posix()
        for number, line in production_lines(path):
            match = TIME_READ.search(strip_comment(line))
            if match is not None:
                hits.append((relative, number, line, match.group(0)))
    return hits


def main() -> int:
    hits = scan()
    bad = False
    budget: list[int] = [count for _path, _snippet, count, _reason in ALLOWLIST]
    for path, number, line, _token in hits:
        allowed = False
        for index, (allow_path, snippet, _count, _reason) in enumerate(ALLOWLIST):
            if path == allow_path and snippet in line and budget[index] > 0:
                budget[index] -= 1
                allowed = True
                break
        if not allowed:
            print(f"DIRECT TIME READ: {path}:{number}: {line.strip()}")
            print(
                "  rule: opening decision code must use the ClockRef seam "
                "(src/clock.rs); a non-decision use belongs in the allowlist "
                "with a reason"
            )
            bad = True
    for index, (path, snippet, count, reason) in enumerate(ALLOWLIST):
        if budget[index] != 0:
            print(
                f"STALE allowlist entry {path!r} ({snippet!r}): expected "
                f"{count} matching line(s), found {count - budget[index]}"
            )
            print(f"  reason: {reason}")
            bad = True
    if bad:
        print(
            f"\nclock-seam guard failed: {len(hits)} direct time read(s) in "
            f"the opening handshake",
            file=sys.stderr,
        )
        return 1
    for row_path, row_fn, required, forbidden, reason in DRIVEN_ROWS:
        body = item_body(REPO / row_path, row_fn)
        if body is None:
            print(f"DRIVEN ROW MISSING: {row_path}::{row_fn}")
            bad = True
            continue
        if required not in body:
            print(f"DRIVEN ROW STOPPED DRIVING: {row_path}::{row_fn}")
            print(f"  expected {required!r} in the row body; reason: {reason}")
            bad = True
        if forbidden in body:
            print(f"DRIVEN ROW REGAINED A WALL-CLOCK WAIT: {row_path}::{row_fn}")
            print(f"  found {forbidden!r}; reason: {reason}")
            bad = True
    if bad:
        print(
            "\nclock-seam guard failed: a driven row regressed or a direct "
            "time read appeared",
            file=sys.stderr,
        )
        return 1
    print(
        f"clock-seam guard OK: {len(hits)} direct time read(s) in the opening "
        f"handshake, all allowlisted, and {len(DRIVEN_ROWS)} driven row(s) still "
        f"driven"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
