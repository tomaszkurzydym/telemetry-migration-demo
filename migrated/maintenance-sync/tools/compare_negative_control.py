#!/usr/bin/env python3
"""Negative controls for compare.py (SYNTHETIC DEMO): each tampering of a passing run's dumps must make it FAIL.
Usage: compare_negative_control.py <run_dir> <report.json>"""
import json
import pathlib
import re
import subprocess
import sys
import tempfile

run, out = pathlib.Path(sys.argv[1]), sys.argv[2]
tool = pathlib.Path(__file__).with_name("compare.py")
files = {k: (run / f"{k}.jsonl").read_text().splitlines() for k in ["raw", "legacy-db", "rust-db", "legacy-q", "rust-q"]}
mile_wos = {m.group(1) for r in map(json.loads, files["raw"]) if '"mi"' in r["value"]
            for m in [re.search(r'"work_order_id"\s*:\s*"(WO-[0-9]{8})"', r["value"])] if m}
km_rows = [i for i, l in enumerate(files["rust-db"]) if json.loads(l)["work_order_id"] not in mile_wos]
i = km_rows[0]


def edit_row(lines, f):
    r = json.loads(lines[i]); f(r); lines = list(lines); lines[i] = json.dumps(r); return lines


qsrc = lambda l: tuple(json.loads(json.loads(l)["value"])["source"].values())[1:]
legacy_src = {qsrc(l) for l in files["legacy-q"]}
both = next(j for j, l in enumerate(files["rust-q"]) if qsrc(l) in legacy_src)
rust_only = next(j for j, l in enumerate(files["rust-q"]) if qsrc(l) not in legacy_src)


def edit_q(lines, f, j=None):
    j = both if j is None else j
    r = json.loads(lines[j]); q = json.loads(r["value"]); f(q); r["value"] = json.dumps(q)
    lines = list(lines); lines[j] = json.dumps(r); return lines


ldb = {json.loads(l)["work_order_id"]: json.loads(l) for l in files["legacy-db"]}
moved = next(j for j, l in enumerate(files["rust-db"]) if (lambda r: r["work_order_id"] in ldb
             and ldb[r["work_order_id"]]["event_time_ms"] != r["event_time_ms"])(json.loads(l)))


def both_q(f):
    out = {}
    for k in ["legacy-q", "rust-q"]:
        j = next(n for n, l in enumerate(files[k]) if qsrc(l) == qsrc(files["rust-q"][both]))
        out[k] = edit_q(files[k], f, j)
    return out


cases = {
    "defect_m1_winner_row_changed": {"rust-db": (lambda ls: ls[:moved] + [json.dumps({**json.loads(ls[moved]),
                                     "parts_count": json.loads(ls[moved])["parts_count"] + 1})] + ls[moved + 1:])(files["rust-db"])},
    "both_payloads_corrupt": both_q(lambda q: q.update(payload='{"work_order_id":"WO-00000000"}')),
    "row_field_changed": {"rust-db": edit_row(files["rust-db"], lambda r: r.update(parts_count=r["parts_count"] + 1))},
    "row_missing": {"rust-db": files["rust-db"][:i] + files["rust-db"][i + 1:]},
    "row_extra": {"rust-db": files["rust-db"] + [json.dumps({**json.loads(files["rust-db"][i]), "work_order_id": "WO-99999999"})]},
    "dtc_order_changed": {"rust-db": edit_row(files["rust-db"], lambda r: r.update(dtc_codes=list(reversed(r["dtc_codes"])) + ["P0000"]))},
    "quarantine_code_changed": {"rust-q": edit_q(files["rust-q"], lambda q: q.update(error_code="INVALID_STATUS" if q["error_code"] != "INVALID_STATUS" else "INVALID_TS"))},
    "quarantine_missing": {"rust-q": files["rust-q"][:both] + files["rust-q"][both + 1:]},
    "defect_m1_extra_payload_leak": {"rust-q": edit_q(files["rust-q"], lambda q: q.update(payload='{"technician_notes":"SYN-NOTE-000001"}'), rust_only)},
    "quarantine_payload_leak": {"rust-q": edit_q(files["rust-q"], lambda q: q.update(payload='{"technician_notes":"SYN-NOTE-000001"}'))},
    "shadow_written_by_legacy": {"rust-db": edit_row(files["rust-db"], lambda r: r.update(written_by="legacy-maintenance-sync"))},
    "empty_rust_db": {"rust-db": []},
}
results = {}
with tempfile.TemporaryDirectory() as d:
    for name, change in cases.items():
        args = []
        for k, lines in files.items():
            p = pathlib.Path(d) / f"{name}-{k}.jsonl"
            p.write_text("\n".join(change.get(k, lines)) + "\n")
            args += [f"--{k}", str(p)]
        rc = subprocess.run([sys.executable, tool, *args, "--out", f"{d}/{name}.json"], capture_output=True).returncode
        results[name] = "FAIL (as required)" if rc == 1 else f"PASSED - control broken (rc={rc})"
ok = all(v.startswith("FAIL") for v in results.values())
report = {"verdict": "PASS" if ok else "FAIL", "controls": results}
json.dump(report, open(out, "w"), indent=1)
print(json.dumps(report, indent=1))
sys.exit(0 if ok else 1)
