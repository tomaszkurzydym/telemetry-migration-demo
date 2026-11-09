#!/usr/bin/env python3
"""Recovery checker (SYNTHETIC DEMO): compares a faulted run's committed shadow output against a clean
baseline run over the same seeded input. Checks no lost and no duplicate committed effects.

Usage: check_recovery.py <baseline_dir> <faulted_dir> --scenario NAME --report out.json
Each dir holds rust.jsonl, rust_q.jsonl (read_committed) and optionally rust_uncommitted.jsonl.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from tmdlib import decrypt_location, key_from_env, load_env_file, read_jsonl


def rows(d: Path, n: str) -> list[dict]:
    p = d / f"{n}.jsonl"
    return read_jsonl(str(p)) if p.exists() else []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("baseline")
    ap.add_argument("faulted")
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--report", required=True)
    a = ap.parse_args()
    load_env_file()
    key, _ = key_from_env()
    B, F = Path(a.baseline), Path(a.faulted)

    def idx(rs):
        out, dup = {}, Counter()
        for r in rs:
            v = json.loads(r["value"], parse_float=Decimal)
            k = (v["vin"], v["event_id"])
            if k in out:
                dup[k] += 1
            out[k] = v
        return out, dup

    base, bdup = idx(rows(B, "rust"))
    flt, fdup = idx(rows(F, "rust"))
    lost = sorted(set(base) - set(flt))
    extra = sorted(set(flt) - set(base))
    field_diff = Counter()
    for k in set(base) & set(flt):
        b, f = base[k], flt[k]
        for fld in (set(b) | set(f)) - {"location_enc"}:
            if b.get(fld) != f.get(fld):
                field_diff[fld] += 1
        if ("location_enc" in b) != ("location_enc" in f):
            field_diff["location_enc:presence"] += 1
        elif "location_enc" in b and decrypt_location(key, *k, b["location_enc"]) != decrypt_location(key, *k, f["location_enc"]):
            field_diff["location_enc:plaintext"] += 1

    def qidx(rs):
        c = Counter()
        for r in rs:
            q = json.loads(r["value"])
            c[(q["source"]["partition"], q["source"]["offset"], q["error_code"])] += 1
        return c
    bq, fq = qidx(rows(B, "rust_q")), qidx(rows(F, "rust_q"))
    q_lost = sum(1 for k in bq if k not in fq)
    q_extra = sum(1 for k in fq if k not in bq)
    q_dup = sum(c - 1 for c in fq.values() if c > 1)
    uncommitted = rows(F, "rust_uncommitted")
    report = {
        "scenario": a.scenario,
        "baseline_records": len(base), "faulted_committed_records": sum(1 for _ in rows(F, "rust")),
        "faulted_read_uncommitted_records": len(uncommitted) if uncommitted else None,
        "aborted_or_uncommitted_records_visible_to_read_uncommitted": (len(uncommitted) - len(rows(F, "rust"))) if uncommitted else None,
        "lost_outputs": len(lost), "lost_examples": [list(k) for k in lost[:10]],
        "extra_outputs": len(extra), "duplicate_committed_outputs": sum(fdup.values()),
        "baseline_duplicates": sum(bdup.values()),
        "field_differences_vs_baseline": dict(field_diff),
        "quarantine": {"baseline": sum(bq.values()), "faulted": sum(fq.values()), "lost": q_lost, "extra": q_extra, "duplicates": q_dup},
    }
    ok = not (lost or extra or fdup or field_diff or q_lost or q_extra or q_dup) and len(base) > 0
    report["verdict"] = "PASS" if ok else "FAIL"
    Path(a.report).parent.mkdir(parents=True, exist_ok=True)
    Path(a.report).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
