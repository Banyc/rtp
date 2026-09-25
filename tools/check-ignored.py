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
  summaries and asserts nothing. The checker requires no assertion token to be
  reachable from its body — in the body itself or through a helper defined in
  the same file — so a probe cannot silently grow a check under the ignore flag
  (the same class of hole the netem_test gate closes for its `perf` tier).

The relocated scenario targets (`tests/`) keep the harness tier vocabulary:

- `standard` / `full` — *asserting* opt-in scenarios (correctness or
  performance floors measured in an opt-in tier). The checker requires an
  assertion token to be reachable from the test body — in the body itself or
  through a helper defined in the same target file — so an opt-in scenario
  that loses its assertion has silently stopped being a gate. The closure is
  crate-local: a call whose callee lives outside the scanned file is never
  followed, so it cannot silently supply the assertion.
- `perf` — a *report-only* scenario (A/B bench, measurement). The checker
  requires its body to contain no assertion token, so a check cannot hide
  under the report-only tier. The helper reach of this tier is *not* left to
  this file's own-body scan: the shared scenario gate
  (`netem_test/tools/check-gate.py`) follows the call graph from every `perf`
  scenario and requires each asserting helper it reaches to be declared in
  the crate's `gate-perf-guard-helpers` block.

The classification also fixes the tier a test belongs to: `perf-lane` and
`probe` are the in-crate (`src/`) ignored set, `standard`/`full`/`perf` are the
relocated scenario targets under `tests/`. A manifest entry that crosses that
split is an error, so a scenario cannot be filed under an in-crate tier (and
vice versa) and escape the tier's rules.

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
# The split is enforced: a manifest entry whose classification does not match
# the tree its path lives under is an error.
CLASSIFICATIONS = {"perf-lane", "probe", "standard", "full", "perf"}
IN_CRATE_CLASSIFICATIONS = {"perf-lane", "probe"}
SCENARIO_CLASSIFICATIONS = {"standard", "full", "perf"}
ASSERTION_TOKENS = re.compile(
    r"(debug_assert_ne!|debug_assert_eq!|debug_assert!|assert_ne!|assert_eq!|assert!|panic!|unreachable!)"
)
# An `#[ignore]` attribute: bare or with a reason string.
IGNORE_RE = re.compile(r"#\[ignore\s*(?:=\s*\"([^\"]*)\")?\s*\]")
FN_RE = re.compile(
    r"\b(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:const\s+)?fn\s+([A-Za-z0-9_]+)\s*(?:<[^>]*>)?\s*\("
)
CALL_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)\s*\(")


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


def local_functions(text: str) -> dict[str, str]:
    """Map every function defined in ``text`` to its brace-balanced body.

    ``text`` is already comment-stripped, so a signature inside a doc example
    is not mistaken for a definition. The first definition of a name wins, the
    same way the scenario gate extracts bodies.
    """
    found: dict[str, str] = {}
    for match in FN_RE.finditer(text):
        start = text.find("{", match.end())
        if start == -1:
            continue
        found.setdefault(match.group(1), fn_body(text, start))
    return found


def reached_assertions(body: str, functions: dict[str, str]) -> set[str]:
    """Every assertion token ``body`` or a transitively called local function
    contains.

    Only functions defined in the same scanned file are followed, so a call
    whose callee lives outside it cannot silently supply the token.
    """
    seen: set[str] = set()
    found: set[str] = set()
    stack = [body]
    while stack:
        current = stack.pop()
        found.update(ASSERTION_TOKENS.findall(current))
        for call in CALL_RE.finditer(current):
            name = call.group(1).rsplit("::", 1)[-1]
            if name in functions and name not in seen:
                seen.add(name)
                stack.append(functions[name])
    return found


def reaches_assertion(body: str, functions: dict[str, str]) -> bool:
    """True when ``body`` or a transitively called local function asserts.

    A `standard`/`full` scenario may keep its assertion one call away, in a
    helper defined in the same target file; the own-body scan would misread
    such a gate as report-only and demand it be reclassified. Only functions
    defined in the same scanned file are followed, so a call whose callee
    lives outside it cannot silently supply the token.
    """
    return bool(reached_assertions(body, functions))


def ignored_tests() -> tuple[dict[str, tuple[str, str]], dict[str, dict[str, str]]]:
    """`(relpath::fn -> (ignore reason, body), relpath -> local functions)`.

    The crate's own `src/` tree and the relocated `tests/` scenario targets
    are scanned; the attribute is matched before the function it decorates,
    exactly as the compiler would see it. The per-file function map lets the
    `standard`/`full` check follow an assertion into a helper defined in the
    same file.
    """
    found: dict[str, tuple[str, str]] = {}
    functions: dict[str, dict[str, str]] = {}
    for root in (SRC, TESTS):
        for path in sorted(root.rglob("*.rs")):
            text = strip_comments(path.read_text(encoding="utf-8"))
            rel = path.relative_to(REPO).as_posix()
            functions[rel] = local_functions(text)
            for match in IGNORE_RE.finditer(text):
                fn_match = FN_RE.search(text, match.end())
                if fn_match is None:
                    sys.exit(f"{path}: #[ignore] attribute not followed by fn")
                start = text.find("{", fn_match.end())
                if start == -1:
                    sys.exit(f"{path}: fn {fn_match.group(1)} has no body")
                found[f"{rel}::{fn_match.group(1)}"] = (
                    match.group(1) or "",
                    fn_body(text, start),
                )
    return found, functions


def main() -> int:
    manifest = manifest_entries()
    actual, functions = ignored_tests()

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
        in_crate = name.startswith("src/")
        if classification in IN_CRATE_CLASSIFICATIONS and not in_crate:
            print(
                f"{classification} {name} is not an in-crate test: `perf-lane` and "
                f"`probe` classify the ignored tests under `src/` "
                f"(reclassify as standard/full/perf)"
            )
            bad = True
        if classification in SCENARIO_CLASSIFICATIONS and in_crate:
            print(
                f"{classification} {name} is not a `tests/` scenario target: "
                f"`standard`, `full` and `perf` classify the relocated scenario tiers "
                f"(reclassify as perf-lane/probe)"
            )
            bad = True
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
            rel = name.rsplit("::", 1)[0]
            reached = reached_assertions(body, functions.get(rel, {}))
            if reached:
                print(
                    f"probe {name} reaches assertion token(s) "
                    f"({', '.join(sorted(reached))}): a report-only probe must "
                    f"assert nothing, in its body or through a same-file helper "
                    f"(reclassify as perf-lane or remove the assertion)"
                )
                bad = True
        elif classification in ("standard", "full"):
            rel = name.rsplit("::", 1)[0]
            if not reaches_assertion(body, functions.get(rel, {})):
                print(
                    f"{classification} scenario {name} reaches no assertion token: "
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