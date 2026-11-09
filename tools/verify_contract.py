#!/usr/bin/env python3
"""Schema verifier for schema_v 2 output (contract §4–§5). SYNTHETIC DEMO.

Usage: verify_contract.py <normalized.jsonl> [--input raw.jsonl] [--report out.json]
Exits non-zero on any violation. Never prints coordinates.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from decimal import Decimal

sys.path.insert(0, __import__("os").path.dirname(__file__))
from tmdlib import EVENT_TIME_RE, FORBIDDEN_KEYS, OUTPUT_KEYS, decrypt_location, key_from_env, load_env_file, read_jsonl


def find_forbidden(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in FORBIDDEN_KEYS:
                yield f"{path}.{k}"
            yield from find_forbidden(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from find_forbidden(v, f"{path}[{i}]")


def check(rec: dict, key, aes_key: bytes | None, key_id: str | None) -> list[str]:
    errs = []
    if set(rec) - {"location_enc"} != OUTPUT_KEYS:
        errs.append(f"keyset:{sorted(set(rec) ^ (OUTPUT_KEYS | ({'location_enc'} & set(rec))))}")
    errs += [f"forbidden_key:{p}" for p in find_forbidden(rec)]
    if rec.get("schema_v") != 2:
        errs.append("schema_v")
    if key != rec.get("vin"):
        errs.append("key_not_preserved")
    if not isinstance(rec.get("event_id"), str) or not rec.get("event_id"):
        errs.append("event_id")
    if not isinstance(rec.get("event_time"), str) or not EVENT_TIME_RE.match(rec["event_time"]):
        errs.append("event_time")
    if not isinstance(rec.get("out_of_order"), bool):
        errs.append("out_of_order")
    for f, dp in (("soc_pct", 1), ("pack_temp_k", 2), ("cell_delta_mv", 0)):
        v = rec.get(f)
        if v is not None:
            if not isinstance(v, (int, Decimal)) or isinstance(v, bool):
                errs.append(f"{f}:type")
            elif -Decimal(v).as_tuple().exponent > dp:
                errs.append(f"{f}:scale")
    s = rec.get("soc_pct")
    if isinstance(s, (int, Decimal)) and not (0 <= s <= 100):
        errs.append("soc_pct:range")
    t = rec.get("pack_temp_k")
    if isinstance(t, (int, Decimal)) and not (Decimal("233.15") <= t <= Decimal("353.15")):
        errs.append("pack_temp_k:range")
    if "location_enc" in rec:
        e = rec["location_enc"]
        if not isinstance(e, dict) or set(e) != {"ciphertext", "nonce", "key_id"}:
            errs.append("location_enc:shape")
        else:
            try:
                if len(base64.b64decode(e["nonce"], validate=True)) != 12:
                    errs.append("location_enc:nonce_len")
                if len(base64.b64decode(e["ciphertext"], validate=True)) <= 16:
                    errs.append("location_enc:ct_len")
            except Exception:
                errs.append("location_enc:base64")
            if key_id and e.get("key_id") != key_id:
                errs.append("location_enc:key_id")
            if aes_key:
                try:
                    decrypt_location(aes_key, rec["vin"], rec["event_id"], e)
                except Exception:
                    errs.append("location_enc:auth_failed")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("normalized")
    ap.add_argument("--input", help="raw input dump: check location_enc presence matches input location")
    ap.add_argument("--report")
    ap.add_argument("--no-decrypt", action="store_true")
    a = ap.parse_args()
    load_env_file()
    aes_key, key_id = (None, None) if a.no_decrypt else key_from_env()
    rows = read_jsonl(a.normalized)
    violations: dict[str, int] = {}
    examples = []
    with_loc = {}
    if a.input:
        for r in read_jsonl(a.input):
            try:
                v = json.loads(r["value"])
                if isinstance(v, dict) and isinstance(v.get("event_id"), str):
                    with_loc.setdefault((r["key"], v["event_id"]), v.get("location") is not None)
            except Exception:
                pass
    for r in rows:
        rec = json.loads(r["value"], parse_float=Decimal)
        errs = check(rec, r["key"], aes_key, key_id)
        if a.input:
            had = with_loc.get((r["key"], rec.get("event_id")))
            if had is None:
                errs.append("not_in_input")
            elif had != ("location_enc" in rec):
                errs.append("location_enc:presence")
        for e in errs:
            violations[e] = violations.get(e, 0) + 1
            if len(examples) < 20:
                examples.append({"partition": r["partition"], "offset": r["offset"], "event_id": rec.get("event_id"), "error": e})
    report = {"file": a.normalized, "records": len(rows),
              "with_location_enc": sum(1 for r in rows if '"location_enc"' in r["value"]),
              "decrypt_checked": aes_key is not None, "violations": violations, "examples": examples,
              "ok": not violations}
    out = json.dumps(report, indent=2)
    if a.report:
        open(a.report, "w").write(out + "\n")
    print(out)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
