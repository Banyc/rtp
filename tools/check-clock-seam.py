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
for, and an allowlist entry rather than a wider scan keeps every other module's
direct reads out of this gate until they are migrated too. `#[cfg(test)]` items
and `tests.rs` files are skipped; `src/clock.rs` is the seam implementation and
is not in the scanned path.

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
    print(
        f"clock-seam guard OK: {len(hits)} direct time read(s) in the opening "
        f"handshake, all allowlisted"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
