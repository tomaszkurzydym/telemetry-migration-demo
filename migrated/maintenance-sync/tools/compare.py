#!/usr/bin/env python3
"""Shadow comparison for maintenance-sync (SYNTHETIC DEMO).

Joins the legacy and Rust service DB tables on work_order_id and the two quarantine topics on source
(partition, offset). A difference passes only if it is explained by a documented deviation (contract §9),
checked against the raw input record(s) that caused it. Exit 1 on FAIL, including empty inputs.

Usage: compare.py --raw raw.jsonl --legacy-db a.jsonl --rust-db b.jsonl --legacy-q q1.jsonl --rust-q q2.jsonl --out report.json
"""
import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

FIELDS = ["vin", "status", "service_center_id", "event_time_ms", "odometer_km", "dtc_codes", "parts_count", "labor_min"]
MAX_KM = Decimal("2000000")
LATER_THAN_ODOMETER = {"INVALID_DTC", "INVALID_PARTS", "INVALID_LABOR"}  # contract §3 order


def jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def km(x: Decimal, factor: str) -> Decimal:
    return (x * Decimal(factor)).quantize(Decimal("0.1"), ROUND_HALF_UP)


def mile_reading(raw_value: str):
    """(Decimal miles) if the raw record is an object with odometer_unit 'mi' and a plain numeric odometer."""
    try:
        o = json.loads(raw_value, parse_float=Decimal, parse_int=Decimal)
    except (ValueError, InvalidOperation):
        return None
    if isinstance(o, dict) and o.get("odometer_unit") == "mi" and isinstance(o.get("odometer"), Decimal):
        return o
    return None


def m1_range_shift(o) -> bool:
    """Legacy accepts (×1.6 within range), contract rejects (×1.609344 out of range)."""
    x = o["odometer"]
    return 0 <= x <= MAX_KM and km(x, "1.6") <= MAX_KM < km(x, "1.609344")


def redact(v):
    if isinstance(v, dict):
        return {k: "[REDACTED]" if k == "technician_notes" else redact(x) for k, x in v.items()}
    return [redact(x) for x in v] if isinstance(v, list) else v


def payload_matches_raw(q, rawrec) -> bool:
    """An explained-but-unmatched quarantine record must still carry the correctly redacted source record."""
    try:
        return rawrec is not None and json.loads(q["payload"]) == redact(json.loads(rawrec["value"]))
    except ValueError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    for a in ["raw", "legacy-db", "rust-db", "legacy-q", "rust-q", "out"]:
        ap.add_argument(f"--{a}", required=True)
    a = ap.parse_args()
    raw = jsonl(a.raw)
    ldb = {r["work_order_id"]: r for r in jsonl(a.legacy_db)}
    rdb = {r["work_order_id"]: r for r in jsonl(a.rust_db)}
    lq_all, rq_all = [json.loads(r["value"]) for r in jsonl(a.legacy_q)], [json.loads(r["value"]) for r in jsonl(a.rust_q)]
    key = lambda q: (q["source"]["partition"], q["source"]["offset"])
    lq, rq = {key(q): q for q in lq_all}, {key(q): q for q in rq_all}
    raw_by_src = {(r["partition"], r["offset"]): r for r in raw}
    by_wo = defaultdict(list)
    for r in raw:
        try:
            o = json.loads(r["value"])
            if isinstance(o, dict) and isinstance(o.get("work_order_id"), str):
                by_wo[o["work_order_id"]].append(r)
        except ValueError:
            pass

    problems, explained = [], Counter()
    if not raw or not ldb or not rdb or not lq or not rq:
        problems.append({"kind": "EMPTY_INPUT", "raw": len(raw), "legacy_db": len(ldb), "rust_db": len(rdb),
                         "legacy_q": len(lq), "rust_q": len(rq)})

    def row_diff_explained(wo, l, r) -> bool:
        miles = [m for m in (mile_reading(x["value"]) for x in by_wo.get(wo, [])) if m is not None]
        if l and r and all(l[f] == r[f] for f in FIELDS if f != "odometer_km"):
            # Same winning event; only the conversion differs.
            return any(m.get("ts_ms") == l["event_time_ms"] and str(km(m["odometer"], "1.6")) == l["odometer_km"]
                       and str(km(m["odometer"], "1.609344")) == r["odometer_km"] for m in miles)
        # Different winner or missing row: some mile event of this work order is in the shifted range.
        return any(m1_range_shift(m) for m in miles)

    for wo in sorted(set(ldb) | set(rdb)):
        l, r = ldb.get(wo), rdb.get(wo)
        if l and r and all(l[f] == r[f] for f in FIELDS):
            continue
        kind = "MISSING_IN_RUST" if not r else "EXTRA_IN_RUST" if not l else "FIELD_DIFF"
        if row_diff_explained(wo, l, r):
            explained[f"DEFECT-M1:{kind}"] += 1
        else:
            problems.append({"kind": f"DB_{kind}", "work_order_id": wo,
                             "fields": [f for f in FIELDS if l and r and l[f] != r[f]]})

    for src in sorted(set(lq) | set(rq)):
        l, r = lq.get(src), rq.get(src)
        if l and r and all(l[k] == r[k] for k in ["error_code", "work_order_id", "vin"]) and \
                (l["payload"] == r["payload"] or json.loads(l["payload"]) == json.loads(r["payload"])
                 if not l["payload"].startswith("[MALFORMED") else l["payload"] == r["payload"]):
            continue
        rawrec = raw_by_src.get(src)
        m = mile_reading(rawrec["value"]) if rawrec else None
        if r and not payload_matches_raw(r, rawrec) and not r["payload"].startswith("[MALFORMED"):
            problems.append({"kind": "QUARANTINE_PAYLOAD_NOT_REDACTED_SOURCE", "source": list(src)})
        elif l is None and r and r["error_code"] == "INVALID_ODOMETER" and m and m1_range_shift(m):
            explained["DEFECT-M1:QUARANTINE_EXTRA_IN_RUST"] += 1
        elif l and r and r["error_code"] == "INVALID_ODOMETER" and l["error_code"] in LATER_THAN_ODOMETER \
                and (l["work_order_id"], l["vin"]) == (r["work_order_id"], r["vin"]) and m and m1_range_shift(m):
            # Validation order: the corrected conversion now fails at the odometer check, before the later check.
            explained["DEFECT-M1:QUARANTINE_EARLIER_CODE"] += 1
        elif l and r and l["error_code"] == "MALFORMED_JSON" and r["error_code"] == "INVALID_ODOMETER" \
                and rawrec and "e-" in rawrec["value"].lower():
            explained["DEFECT-M2:QUARANTINE_CODE"] += 1
        else:
            problems.append({"kind": "QUARANTINE_" + ("MISSING_IN_RUST" if not r else "EXTRA_IN_RUST" if not l else "DIFF"),
                             "source": list(src), "legacy": l and l["error_code"], "rust": r and r["error_code"]})

    rust_written_by = Counter(r["written_by"] for r in rdb.values())
    if set(rust_written_by) - {"rust-maintenance-sync"}:
        problems.append({"kind": "SHADOW_TABLE_WRITTEN_BY_OTHER", "written_by": dict(rust_written_by)})
    report = {
        "verdict": "PASS" if not problems else "FAIL",
        "raw_records": len(raw),
        "db_rows": {"legacy": len(ldb), "rust": len(rdb), "identical": sum(1 for w in ldb if w in rdb and all(ldb[w][f] == rdb[w][f] for f in FIELDS))},
        "quarantine": {"legacy_records": len(lq_all), "legacy_unique": len(lq), "rust_records": len(rq_all), "rust_unique": len(rq),
                       "rust_by_code": dict(Counter(q["error_code"] for q in rq.values()))},
        "explained_by_defect": dict(explained),
        "unexplained": len(problems),
        "problems": problems[:50],
    }
    with open(a.out, "w") as f:
        json.dump(report, f, indent=1)
    print(json.dumps({k: report[k] for k in ["verdict", "db_rows", "quarantine", "explained_by_defect", "unexplained"]}, indent=1))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
