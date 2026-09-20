#!/usr/bin/env python3
"""Verify the rtp opt-in ignored-test inventory in GATE.md.

`cargo test` silently skips every `#[ignore]`d test, so the opt-in set and its
classification is recorded in GATE.md. This script re-derives that set from the
source files under `src/` and the scenario targets under `tests/`, and exits
non-zero when the manifest and reality disagree, so an ignored test can never
be added, removed, renamed, or reclassified without the gate documentation
being updated.

The in-crate (`src/`) inventory has one of two honest classifications:

- `perf-lane` — an *asserting* test kept opt-in because its assertion is a
  wall-clock sub-linear-scaling ratio (e.g. `many < few * 8.0`), which is
  inherently environment-dependent and only meaningful under `--release`.
  These tests carry an `#[ignore = "perf lane: ..."]` reason documenting the
  run command, and the checker requires (a) the reason to still say `perf lane`
  and (b) the test body to still contain an assertion token. A perf lane that
  loses its assertion has silently stopped being a gate and is an error.
- `probe` — a *report-only* measurement probe: it prints counters or latency
  summaries and asserts nothing. The checker requires its body to contain no
  assertion token, so a probe cannot silently grow a check under the ignore
  flag (the same class of hole the netem_test gate closes for its `perf` tier).

The relocated scenario targets (`tests/`) keep the harness tier vocabulary:

- `standard` / `full` — *asserting* opt-in scenarios (correctness or
  performance floors measured in an opt-in tier). The checker requires the
  test body to still contain an assertion token, so an opt-in scenario that
  loses its assertion has silently stopped being a gate.
- `perf` — a *report-only* scenario (A/B bench, measurement). The checker
  requires its body to contain no assertion token, so a check cannot hide
  under the report-only tier.

Files under `fuzz/`, `examples/`, and `local/` are not scanned (besides
`tests/`, the opt-in inventory is the in-crate `src/` set). The
assertion-token set matches netem_test's `tools/check-gate.py`, including the
debug-only forms; the brace counting is the same regex-level body extraction
that harness uses, so both gates agree on what a function body is.

Usage:
    python3 tools/check-ignored.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "GATE.md"
SRC = REPO / "src"
TESTS = REPO / "tests"

# `perf-lane`/`probe` classify the in-crate (`src/`) ignored tests;
# `standard`/`full`/`perf` are the scenario tiers for the relocated
# `tests/` targets and use the same names as the netem_test scenario gate.
CLASSIFICATIONS = {"perf-lane", "probe", "standard", "full", "perf"}
ASSERTING_CLASSIFICATIONS = {"perf-lane", "standard", "full"}
ASSERTION_TOKENS = re.compile(
    r"(debug_assert_ne!|debug_assert_eq!|debug_assert!|assert_ne!|assert_eq!|assert!|panic!|unreachable!)"
)
# An `#[ignore]` attribute: bare or with a reason string.
IGNORE_RE = re.compile(r"#\[ignore\s*(?:=\s*\"([^\"]*)\")?\s*\]")
FN_RE = re.compile(
    r"\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+([A-Za-z0-9_]+)\s*(?:<[^>]*>)?\s*\("
)


def manifest_block(name: str) -> str | None:
    """Return the body of the ```<name> fenced block, or None."""
    text = MANIFEST.read_text(encoding="utf-8")
    block = re.search(rf"```{re.escape(name)}\n(.*?)```", text, re.S)
    return block.group(1) if block else None


def manifest_entries() -> dict[str, str]:
    block = manifest_block("ignored-manifest")
    if block is None:
        sys.exit(f"{MANIFEST}: no ```ignored-manifest block found")
    entries: dict[str, str] = {}
    for raw in block.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, _, classification = line.partition(" = ")
        name, classification = name.strip(), classification.strip()
        if classification not in CLASSIFICATIONS:
            sys.exit(f"{MANIFEST}: {name} has unknown classification {classification!r}")
        if name in entries:
            sys.exit(f"{MANIFEST}: duplicate entry {name}")
        entries[name] = classification
    return entries


def fn_body(text: str, start: int) -> str:
    """Brace-balanced body starting at ``start`` (the index of `{`)."""
    depth = 0
    idx = start
    while idx < len(text):
        char = text[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                break
        idx += 1
    return text[start : idx + 1]


def strip_comments(text: str) -> str:
    """Remove `//` line comments and `/* ... */` block comments.

    Doc comments routinely mention `#[ignore]` ("kept `#[ignore]`d by
    default"), which the attribute regex would otherwise mistake for a real
    skip; stripping comments first also stops fn signatures inside doc code
    blocks from being picked up as function definitions. String literals are
    kept (an `#[ignore = \"...\"]` reason is an attribute, not a comment).
    """
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        if text.startswith("//", i):
            while i < n and text[i] != "\n":
                i += 1
            out.append("\n")
            continue
        if text.startswith("/*", i):
            depth = 1
            i += 2
            start = i
            while depth and i < n:
                if text.startswith("/*", i):
                    depth += 1
                    i += 2
                elif text.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            out.append("\n" * text[start:i].count(chr(10)))
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def ignored_tests() -> dict[str, tuple[str, str]]:
    """Map `relpath::fn` -> (ignore reason, body) for every `#[ignore]`d test.

    The crate's own `src/` tree and the relocated `tests/` scenario targets
    are scanned; the attribute is matched before the function it decorates,
    exactly as the compiler would see it.
    """
    found: dict[str, tuple[str, str]] = {}
    for root in (SRC, TESTS):
        for path in sorted(root.rglob("*.rs")):
            text = strip_comments(path.read_text(encoding="utf-8"))
            for match in IGNORE_RE.finditer(text):
                fn_match = FN_RE.search(text, match.end())
                if fn_match is None:
                    sys.exit(f"{path}: #[ignore] attribute not followed by fn")
                start = text.find("{", fn_match.end())
                if start == -1:
                    sys.exit(f"{path}: fn {fn_match.group(1)} has no body")
                rel = path.relative_to(REPO).as_posix()
                found[f"{rel}::{fn_match.group(1)}"] = (
                    match.group(1) or "",
                    fn_body(text, start),
                )
    return found


def main() -> int:
    manifest = manifest_entries()
    actual = ignored_tests()

    bad = False
    missing = sorted(actual.keys() - manifest.keys())
    stale = sorted(manifest.keys() - actual.keys())
    if missing or stale:
        for name in missing:
            print(f"UNCLASSIFIED ignored test (add to GATE.md ignored-manifest): {name}")
        for name in stale:
            print(f"STALE GATE.md entry (no longer #[ignore]d): {name}")
        bad = True

    for name, classification in sorted(manifest.items()):
        if name not in actual:
            continue
        reason, body = actual[name]
        tokens = ASSERTION_TOKENS.findall(body)
        if classification == "perf-lane":
            if "perf lane" not in reason:
                print(
                    f"perf-lane {name} no longer documents its run command "
                    f"(reason {reason!r} lacks 'perf lane'): update the #[ignore] reason"
                )
                bad = True
            if not tokens:
                print(
                    f"perf-lane {name} contains no assertion token: it has silently "
                    f"stopped being a gate (reclassify as probe or restore the assertion)"
                )
                bad = True
        elif classification == "probe":
            if tokens:
                print(
                    f"probe {name} contains assertion token(s) "
                    f"({', '.join(sorted(set(tokens)))}): a report-only probe must "
                    f"assert nothing (reclassify as perf-lane or remove the assertion)"
                )
                bad = True
        elif classification in ("standard", "full"):
            if not tokens:
                print(
                    f"{classification} scenario {name} contains no assertion token: "
                    f"it has silently stopped being a gate (reclassify as perf "
                    f"or restore the assertion)"
                )
                bad = True
        elif classification == "perf":
            if tokens:
                print(
                    f"perf scenario {name} contains assertion token(s) "
                    f"({', '.join(sorted(set(tokens)))}): a report-only scenario must "
                    f"assert nothing (reclassify as standard/full or remove the assertion)"
                )
                bad = True

    if bad:
        print(
            f"\nmanifest has {len(manifest)} entries, source reports "
            f"{len(actual)} #[ignore]d tests; update GATE.md",
            file=sys.stderr,
        )
        return 1

    print(f"ignored-test manifest OK: {len(actual)} #[ignore]d tests classified")
    for classification in sorted(CLASSIFICATIONS):
        count = sum(1 for c in manifest.values() if c == classification)
        print(f"  {classification}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())