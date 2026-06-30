#!/usr/bin/env python3
"""Fuse the result lists of dual-cluster e2e raw logs into ONE unified report.

Each raw log is a complete, independent TestFlows run, so the logs cannot be
concatenated and replayed: their test-id namespaces both root at the same path, the
ids collide, and the later run's scenarios are silently dropped. Instead we render
each log's ``tfs transform short`` separately, then fuse the per-scenario result lines
into a single Passing/Failing listing and re-tally the scenario/step totals.

Usage: merge_dual_results.py <raw1> <label1> [<raw2> <label2> ...]
Prints the unified report; exits 1 if any scenario failed/errored, else 0.
"""
import re
import subprocess
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
# Leaf scenario result line, e.g. "[ OK ] /regression/.../test_010072. name (8m 58s)".
# Rollup lines (/regression, /regression/e2e.test_operator) lack "/test_" -> excluded.
RESULT = re.compile(r"\[\s*(OK|Fail|Error|Skip)\s*\]\s+(/regression/\S*/test_\S.*)$")
# Scenario counts are derived from deduped RESULT lines (retry-safe), not this summary,
# so there is no SCEN regex. STEP is still read from the summary for the informational tally.
STEP = re.compile(r"(\d+)\s+steps?\s+\(([^)]*)\)")
TOTAL = re.compile(r"Total time\s+(.+)")
BREAKDOWN = re.compile(r"(\d+)\s+(ok|failed|skipped|errored)")
PASS_SYMBOL, FAIL_SYMBOL = "✔", "✘"  # ✔ ✘


def transform(raw):
    with open(raw, "rb") as fh:
        proc = subprocess.run(
            ["tfs", "--no-colors", "transform", "short"],
            stdin=fh, capture_output=True, text=True,
        )
    return ANSI.sub("", proc.stdout)


def breakdown(counts):
    parts = [f"{v} {k}" for k, v in counts.items() if v]
    return ", ".join(parts) if parts else "0"


_STATUS_KEY = {"OK": "ok", "Fail": "failed", "Skip": "skipped", "Error": "errored"}
_TIME_SUFFIX = re.compile(r"\s*\([0-9hms .]+\)\s*$")  # trailing " (10m 13s)" / " (16s 903ms)"


def parse(raw, label):
    # Dedup by scenario (path+name, minus the trailing "(time)") so a test that was
    # retried fail->...->ok collapses to ONE entry, with OK/Skip winning over Fail/Error
    # — i.e. a scenario that eventually passed counts as passing. Scenario counts are
    # derived from these deduped results, so --retry never double-counts. Step counts
    # come from the transform summary (informational; may include retry attempts).
    by_scenario = {}  # scenario-key -> (status, formatted entry line)
    steps = {"ok": 0, "failed": 0, "skipped": 0, "errored": 0}
    n_steps = 0
    total = "?"
    for line in transform(raw).splitlines():
        m = RESULT.search(line)
        if m:
            status, rest = m.group(1), m.group(2).strip()
            key = _TIME_SUFFIX.sub("", rest)
            passed = status in ("OK", "Skip")
            prev = by_scenario.get(key)
            # First sighting, or a later pass that supersedes an earlier fail (retry won).
            if prev is None or (passed and prev[0] in ("Fail", "Error")):
                symbol = PASS_SYMBOL if passed else FAIL_SYMBOL
                by_scenario[key] = (status, f"{symbol} [ {status} ] [{label}] {rest}")
            continue
        st = STEP.search(line)
        if st:
            n_steps = int(st.group(1))
            steps = {"ok": 0, "failed": 0, "skipped": 0, "errored": 0}
            steps.update({w: int(n) for n, w in BREAKDOWN.findall(st.group(2)) if w in steps})
        tm = TOTAL.search(line)
        if tm:
            total = tm.group(1).strip()
    passing = [e for s, e in by_scenario.values() if s in ("OK", "Skip")]
    failing = [e for s, e in by_scenario.values() if s in ("Fail", "Error")]
    scen = {"ok": 0, "failed": 0, "skipped": 0, "errored": 0}
    for s, _ in by_scenario.values():
        scen[_STATUS_KEY[s]] += 1
    return {"label": label, "passing": passing, "failing": failing,
            "n_scen": len(by_scenario), "scen": scen, "n_steps": n_steps, "steps": steps, "total": total}


def main(argv):
    args = argv[1:]
    if len(args) < 2 or len(args) % 2:
        sys.exit("usage: merge_dual_results.py <raw1> <label1> [<raw2> <label2> ...]")
    runs = [parse(args[i], args[i + 1]) for i in range(0, len(args), 2)]

    passing = [e for r in runs for e in r["passing"]]
    failing = [e for r in runs for e in r["failing"]]
    scen = {"ok": 0, "failed": 0, "skipped": 0, "errored": 0}
    steps = {"ok": 0, "failed": 0, "skipped": 0, "errored": 0}
    n_scen = n_steps = 0
    for r in runs:
        n_scen += r["n_scen"]
        n_steps += r["n_steps"]
        for k in scen:
            scen[k] += r["scen"][k]
        for k in steps:
            steps[k] += r["steps"][k]

    print()
    print("==================== COMBINED dual-cluster results ====================")
    if passing:
        print("\nPassing\n")
        print("\n".join(passing))
    if failing:
        print("\nFailing\n")
        print("\n".join(failing))
    print()
    print(f"{n_scen} scenarios ({breakdown(scen)})")
    print(f"{n_steps} steps ({breakdown(steps)})")
    for r in runs:
        print(f"  {r['label']}: {r['n_scen']} scenarios ({breakdown(r['scen'])}), total time {r['total']}")
    print("=======================================================================")

    # Prominent, scannable headline — the one line to read for the run's outcome.
    failed_total = scen["failed"] + scen["errored"]
    bar = "#" * 71
    print()
    print(bar)
    if failed_total == 0:
        print(f"###  RESULT: ALL OK  —  {scen['ok']}/{n_scen} scenarios passed, 0 failed")
    else:
        print(f"###  RESULT: FAILED TESTS: {failed_total}  "
              f"({scen['failed']} failed, {scen['errored']} errored of {n_scen}) — see 'Failing' above")
    print(bar)
    return 1 if failed_total else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
