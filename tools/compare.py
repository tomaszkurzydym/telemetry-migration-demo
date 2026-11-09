#!/usr/bin/env python3
"""Shadow comparator: legacy (Spark, legacy-cg) vs Rust shadow (rust-shadow-cg). SYNTHETIC DEMO.

Reads the committed contents of the legacy and shadow output/quarantine topics (with its own throwaway
consumer group, so it never disturbs either processor), joins normalized records on (vin, event_id) and
writes reports/diff-<run>.json. Location ciphertexts use random nonces, so both sides are decrypted and
the recovered values are compared numerically; every ciphertext must authenticate under the expected key.
Coordinates are never printed or written.

Usage: compare.py --run <id> [--from-dumps <dir>]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from tmdlib import consume_all, decrypt_location, dump_jsonl, key_from_env, load_env_file, read_jsonl

TOPICS = {
    "raw": "telemetry.battery.raw",
    "legacy": "telemetry.battery.normalized",
    "rust": "telemetry.battery.normalized.shadow",
    "legacy_q": "telemetry.battery.quarantine",
    "rust_q": "telemetry.battery.quarantine.shadow",
}
GROUPS = {"legacy": "legacy-cg", "rust": "rust-shadow-cg"}
MAX_EXAMPLES = 25


def git_rev() -> str:
    try:
        rev = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        dirty = subprocess.call(["git", "diff", "--quiet", "HEAD"], stderr=subprocess.DEVNULL) != 0
        return rev + ("-dirty" if dirty else "")
    except Exception:
        return "unknown"


def load(run_dir: Path, from_dumps: bool, bootstrap: str) -> dict[str, list[dict]]:
    data = {}
    for name, topic in TOPICS.items():
        path = run_dir / f"{name}.jsonl"
        if not from_dumps:
            dump_jsonl(consume_all(bootstrap, topic, f"compare-{uuid.uuid4().hex[:8]}"), str(path))
        data[name] = read_jsonl(str(path))
    return data


def dec(v):
    return json.loads(v, parse_float=Decimal, parse_int=Decimal)


def index(rows: list[dict], side: str, report: dict) -> dict[tuple, dict]:
    out, dup = {}, Counter()
    for r in rows:
        rec = dec(r["value"])
        k = (rec.get("vin"), rec.get("event_id"))
        if k in out:
            dup[k] += 1
            continue
        out[k] = {"rec": rec, "partition": r["partition"], "offset": r["offset"], "key": r["key"]}
    report["duplicates"][side] = {"count": sum(dup.values()), "examples": [list(k) for k in list(dup)[:MAX_EXAMPLES]]}
    return out


def is_defect1(raw_cell, legacy_v, rust_v) -> bool:
    if raw_cell is None:
        return False
    raw_cell = Decimal(raw_cell)
    return (legacy_v == raw_cell.quantize(Decimal(1), rounding=ROUND_DOWN)
            and rust_v == raw_cell.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--from-dumps", action="store_true", help="reuse run/<id>/*.jsonl instead of reading Kafka")
    ap.add_argument("--out-dir", default="reports")
    a = ap.parse_args()
    load_env_file()
    key, key_id = key_from_env()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    run_dir = Path("run") / a.run
    run_dir.mkdir(parents=True, exist_ok=True)
    data = load(run_dir, a.from_dumps, bootstrap)

    report: dict = {"run": a.run, "code_version": git_rev(), "topics": TOPICS, "processor_groups": GROUPS,
                    "counts": {k: len(v) for k, v in data.items()}, "duplicates": {}}

    # Input order per (vin, event_id): first occurrence, for ordering checks and DEFECT-1 classification.
    raw_first: dict[tuple, dict] = {}
    for r in data["raw"]:
        try:
            v = dec(r["value"])
        except Exception:
            continue
        if isinstance(v, dict):
            raw_first.setdefault((r["key"], v.get("event_id")), {"offset": r["offset"], "partition": r["partition"], "v": v})

    L = index(data["legacy"], "legacy", report)
    R = index(data["rust"], "rust", report)
    missing = sorted(set(L) - set(R))
    extra = sorted(set(R) - set(L))
    mismatches: list[dict] = []
    by_field = Counter()
    defect1 = 0
    crypto = Counter()
    for k in sorted(set(L) & set(R)):
        lr, rr = L[k]["rec"], R[k]["rec"]
        for f in sorted((set(lr) | set(rr)) - {"location_enc"}):
            if lr.get(f) == rr.get(f):
                continue
            raw_cell = raw_first.get(k, {}).get("v", {}).get("cell_delta_mv")
            if f == "cell_delta_mv" and is_defect1(raw_cell, lr.get(f), rr.get(f)):
                defect1 += 1
                continue
            by_field[f] += 1
            if len(mismatches) < MAX_EXAMPLES:
                mismatches.append({"vin": k[0], "event_id": k[1], "field": f,
                                   "legacy": str(lr.get(f)), "rust": str(rr.get(f))})
        le, re_ = lr.get("location_enc"), rr.get("location_enc")
        if (le is None) != (re_ is None):
            by_field["location_enc:presence"] += 1
            continue
        if le is None:
            continue
        crypto["pairs"] += 1
        try:
            lp = decrypt_location(key, k[0], k[1], le)
            crypto["legacy_roundtrip_ok"] += 1
        except Exception:
            lp = None
            crypto["legacy_auth_failed"] += 1
        try:
            rp = decrypt_location(key, k[0], k[1], re_)
            crypto["rust_roundtrip_ok"] += 1
        except Exception:
            rp = None
            crypto["rust_auth_failed"] += 1
        if le.get("key_id") != key_id or re_.get("key_id") != key_id:
            crypto["key_id_mismatch"] += 1
        if le.get("nonce") == re_.get("nonce"):
            crypto["nonce_reused_across_sides"] += 1
        if lp is not None and rp is not None:
            if lp == rp:
                crypto["plaintext_equal"] += 1
            else:
                by_field["location_enc:plaintext"] += 1
                if len(mismatches) < MAX_EXAMPLES:
                    mismatches.append({"vin": k[0], "event_id": k[1], "field": "location_enc", "legacy": "<redacted>", "rust": "<redacted>"})
        raw_loc = raw_first.get(k, {}).get("v", {}).get("location")
        if isinstance(raw_loc, dict) and rp is not None and (Decimal(raw_loc["lat"]), Decimal(raw_loc["lon"])) != (rp["lat"], rp["lon"]):
            by_field["location_enc:not_input_value"] += 1

    # Nonce uniqueness across all ciphertexts on both sides.
    nonces = Counter(dec(r["value"]).get("location_enc", {}).get("nonce") for side in ("legacy", "rust") for r in data[side])
    nonces.pop(None, None)
    crypto["distinct_nonces"] = len(nonces)
    crypto["repeated_nonces"] = sum(c - 1 for c in nonces.values() if c > 1)

    # Ordering: per side, per (vin) output order must follow input offset order.
    ordering: dict[str, dict] = {}
    for side, idx in (("legacy", L), ("rust", R)):
        by_vin = defaultdict(list)
        for k, e in idx.items():
            by_vin[k[0]].append((e["partition"], e["offset"], raw_first.get(k, {}).get("offset", -1), k[1]))
        viol, ex = 0, []
        for vin, lst in by_vin.items():
            lst.sort()
            parts = {p for p, *_ in lst}
            for prev, cur in zip(lst, lst[1:]):
                if cur[2] < prev[2]:
                    viol += 1
                    if len(ex) < MAX_EXAMPLES:
                        ex.append({"vin": vin, "event_id": cur[3]})
            if len(parts) > 1:
                viol += 1
        ordering[side] = {"violations": viol, "examples": ex}

    # Quarantine parity by source offset.
    def qidx(rows):
        return {(q["source"]["partition"], q["source"]["offset"]): q["error_code"] for q in map(dec, (r["value"] for r in rows))}
    lq, rq = qidx(data["legacy_q"]), qidx(data["rust_q"])
    q_report = {"legacy": len(lq), "rust": len(rq), "missing_in_rust": len(set(lq) - set(rq)),
                "extra_in_rust": len(set(rq) - set(lq)),
                "error_code_mismatch": sum(1 for k in set(lq) & set(rq) if lq[k] != rq[k]),
                "by_error_code_rust": dict(Counter(rq.values()))}

    unexplained = (len(missing) + len(extra) + sum(by_field.values()) + ordering["legacy"]["violations"]
                   + ordering["rust"]["violations"] + report["duplicates"]["legacy"]["count"]
                   + report["duplicates"]["rust"]["count"] + q_report["missing_in_rust"] + q_report["extra_in_rust"]
                   + q_report["error_code_mismatch"] + crypto["legacy_auth_failed"] + crypto["rust_auth_failed"]
                   + crypto["key_id_mismatch"] + crypto["repeated_nonces"] + crypto["nonce_reused_across_sides"])
    report.update({
        "joined": len(set(L) & set(R)),
        "missing_in_rust": {"count": len(missing), "examples": [list(k) for k in missing[:MAX_EXAMPLES]]},
        "extra_in_rust": {"count": len(extra), "examples": [list(k) for k in extra[:MAX_EXAMPLES]]},
        "field_mismatches": {"unexplained_by_field": dict(by_field), "examples": mismatches},
        "intentional_documented_diffs": {"DEFECT-1": {"field": "cell_delta_mv", "count": defect1,
                                                      "explanation": "legacy truncates, contract/Rust round half-up (docs/contract.md §4)"}},
        "location_crypto": dict(crypto),
        "ordering_violations": ordering,
        "quarantine": q_report,
        "unexplained_total": unexplained,
        "verdict": "PASS" if unexplained == 0 else "FAIL",
    })
    Path(a.out_dir).mkdir(exist_ok=True)
    out = Path(a.out_dir) / f"diff-{a.run}.json"
    out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({k: report[k] for k in ("run", "counts", "joined", "unexplained_total", "verdict")}, default=str))
    print(f"DEFECT-1 diffs: {defect1}; location pairs plaintext-equal: {crypto['plaintext_equal']}/{crypto['pairs']}; report: {out}")
    return 0 if unexplained == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
