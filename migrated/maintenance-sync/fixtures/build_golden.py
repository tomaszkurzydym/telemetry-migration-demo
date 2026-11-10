#!/usr/bin/env python3
"""Writes golden.json. SYNTHETIC DEMO. Every expected value below is typed by hand from docs/contract.md;
nothing is computed by, or captured from, either implementation. This script only saves repetition in inputs."""
import json, pathlib

K = "SYN-VIN-0001"
BASE = [("vin", '"SYN-VIN-0001"'), ("work_order_id", '"WO-00000001"'), ("ts_ms", "1700000000000"),
        ("status", '"OPEN"'), ("service_center_id", '"SC-001"'), ("odometer", "100"), ("odometer_unit", '"km"')]

def ev(drop=(), **over):
    """Raw JSON text: BASE with fields replaced/added (values are raw JSON literals) or dropped."""
    fields = [(k, over.pop(k, v)) for k, v in BASE if k not in drop]
    fields += list(over.items())
    return "{" + ",".join(f'"{k}":{v}' for k, v in fields) + "}"

def row(**over):
    r = {"work_order_id": "WO-00000001", "vin": K, "status": "OPEN", "service_center_id": "SC-001",
         "event_time_ms": 1700000000000, "odometer_km": "100.0", "dtc_codes": [], "parts_count": 0, "labor_min": None}
    r.update(over)
    return r

def rej(code, wo="WO-00000001", vin=K):
    return {"error_code": code, "work_order_id": wo, "vin": vin}

R, Q = "row", "reject"
cases = [
    # §4/§5 normalization
    ("km basic, full optional fields", K, ev(odometer="12345.67", dtc_codes='[" p0a80 ","P0A80","b1234"]',
        parts='[{"part_no":"PN-ABC123","qty":2},{"part_no":"PN-000001","qty":3}]', labor_min="90",
        technician_notes='"SYN-NOTE-000001 checked HV connector torque"'),
     {R: row(odometer_km="12345.7", dtc_codes=["B1234", "P0A80"], parts_count=5, labor_min=90)}, None),
    ("km half-up 12.25", K, ev(odometer="12.25"), {R: row(odometer_km="12.3")}, None),
    ("km 0.04 rounds down", K, ev(odometer="0.04"), {R: row(odometer_km="0.0")}, None),
    ("km negative zero", K, ev(odometer="-0"), {R: row(odometer_km="0.0")}, None),
    ("km trailing zeros ignored for scale", K, ev(odometer="1.2340"), {R: row(odometer_km="1.2")}, None),
    ("km exponent notation", K, ev(odometer="1.5e2"), {R: row(odometer_km="150.0")}, None),
    ("km max 2000000", K, ev(odometer="2000000"), {R: row(odometer_km="2000000.0")}, None),
    ("km 1999999.95 rounds to max", K, ev(odometer="1999999.95"), {R: row(odometer_km="2000000.0")}, None),
    ("km above max", K, ev(odometer="2000000.001"), {Q: rej("INVALID_ODOMETER")}, None),
    ("km 4 decimals", K, ev(odometer="1.2345"), {Q: rej("INVALID_ODOMETER")}, None),
    ("km negative", K, ev(odometer="-1"), {Q: rej("INVALID_ODOMETER")}, None),
    ("odometer as string", K, ev(odometer='"100"'), {Q: rej("INVALID_ODOMETER")}, None),
    ("huge positive exponent", K, ev(odometer="1e9999999999"), {Q: rej("INVALID_ODOMETER")}, None),
    ("huge negative exponent", K, ev(odometer="1e-9999999999"), {Q: rej("INVALID_ODOMETER")},
     {"defect": "DEFECT-M2", Q: rej("MALFORMED_JSON", None, None)}),
    ("mi 100", K, ev(odometer="100", odometer_unit='"mi"'), {R: row(odometer_km="160.9")},
     {"defect": "DEFECT-M1", R: row(odometer_km="160.0")}),
    ("mi 0.05", K, ev(odometer="0.05", odometer_unit='"mi"'), {R: row(odometer_km="0.1")}, None),
    ("mi 12.345", K, ev(odometer="12.345", odometer_unit='"mi"'), {R: row(odometer_km="19.9")},
     {"defect": "DEFECT-M1", R: row(odometer_km="19.8")}),
    ("mi largest accepted 1242742.415", K, ev(odometer="1242742.415", odometer_unit='"mi"'),
     {R: row(odometer_km="2000000.0")}, {"defect": "DEFECT-M1", R: row(odometer_km="1988387.9")}),
    ("mi 1242742.416 rejected", K, ev(odometer="1242742.416", odometer_unit='"mi"'), {Q: rej("INVALID_ODOMETER")},
     {"defect": "DEFECT-M1", R: row(odometer_km="1988387.9")}),
    ("mi 1250000 rejected", K, ev(odometer="1250000", odometer_unit='"mi"'), {Q: rej("INVALID_ODOMETER")},
     {"defect": "DEFECT-M1", R: row(odometer_km="2000000.0")}),
    ("mi raw above 2000000", K, ev(odometer="2000001", odometer_unit='"mi"'), {Q: rej("INVALID_ODOMETER")}, None),
    ("dtc null means empty", K, ev(dtc_codes="null"), {R: row()}, None),
    ("parts empty array", K, ev(parts="[]"), {R: row()}, None),
    ("labor max 1440", K, ev(labor_min="1440"), {R: row(labor_min=1440)}, None),
    ("labor null", K, ev(labor_min="null"), {R: row()}, None),
    ("ts max", K, ev(ts_ms="253402300799999"), {R: row(event_time_ms=253402300799999)}, None),
    ("ts zero", K, ev(ts_ms="0"), {R: row(event_time_ms=0)}, None),
    ("unknown field ignored", K, ev(extra='{"a":1}'), {R: row()}, None),
    ("duplicate key, last wins", K, ev(status='"BAD"').replace('}', ',"status":"OPEN"}'), {R: row()}, None),
    # §3 validation and order
    ("missing field beats key mismatch", "SYN-VIN-9999", ev(drop=("odometer",)), {Q: rej("MISSING_REQUIRED_FIELD")}, None),
    ("required field null", K, ev(status="null"), {Q: rej("MISSING_REQUIRED_FIELD")}, None),
    ("vin not a string", K, ev(vin="7"), {Q: rej("MISSING_REQUIRED_FIELD", vin=None)}, None),
    ("vin empty", "", ev(vin='""'), {Q: rej("MISSING_REQUIRED_FIELD", vin=None)}, None),
    ("key mismatch beats bad work order", "SYN-VIN-9999", ev(work_order_id='"WO-1"'), {Q: rej("KEY_MISMATCH", wo=None)}, None),
    ("null key", None, ev(), {Q: rej("KEY_MISMATCH")}, None),
    ("work order too short", K, ev(work_order_id='"WO-1"'), {Q: rej("INVALID_WORK_ORDER", wo=None)}, None),
    ("work order not a string", K, ev(work_order_id="1"), {Q: rej("INVALID_WORK_ORDER", wo=None)}, None),
    ("ts decimal literal", K, ev(ts_ms="1.0"), {Q: rej("INVALID_TS")}, None),
    ("ts exponent literal", K, ev(ts_ms="1e3"), {Q: rej("INVALID_TS")}, None),
    ("ts negative", K, ev(ts_ms="-1"), {Q: rej("INVALID_TS")}, None),
    ("ts above max", K, ev(ts_ms="253402300800000"), {Q: rej("INVALID_TS")}, None),
    ("status lower case", K, ev(status='"open"'), {Q: rej("INVALID_STATUS")}, None),
    ("service center empty string", K, ev(service_center_id='""'), {Q: rej("INVALID_SERVICE_CENTER")}, None),
    ("service center 4 digits", K, ev(service_center_id='"SC-0001"'), {Q: rej("INVALID_SERVICE_CENTER")}, None),
    ("unit upper case", K, ev(odometer_unit='"KM"'), {Q: rej("INVALID_UNIT")}, None),
    ("unit miles", K, ev(odometer_unit='"miles"'), {Q: rej("INVALID_UNIT")}, None),
    ("dtc 33 entries", K, ev(dtc_codes=json.dumps(["P0A80"] * 33)), {Q: rej("INVALID_DTC")}, None),
    ("dtc 32 duplicates collapse", K, ev(dtc_codes=json.dumps(["P0A80"] * 32)), {R: row(dtc_codes=["P0A80"])}, None),
    ("dtc bad code", K, ev(dtc_codes='["P0A8"]'), {Q: rej("INVALID_DTC")}, None),
    ("dtc non-string element", K, ev(dtc_codes="[1]"), {Q: rej("INVALID_DTC")}, None),
    ("dtc not an array", K, ev(dtc_codes='"P0A80"'), {Q: rej("INVALID_DTC")}, None),
    ("parts qty 0", K, ev(parts='[{"part_no":"PN-ABC123","qty":0}]'), {Q: rej("INVALID_PARTS")}, None),
    ("parts qty 1000", K, ev(parts='[{"part_no":"PN-ABC123","qty":1000}]'), {Q: rej("INVALID_PARTS")}, None),
    ("parts lower-case part_no", K, ev(parts='[{"part_no":"PN-abc123","qty":1}]'), {Q: rej("INVALID_PARTS")}, None),
    ("parts 101 entries", K, ev(parts=json.dumps([{"part_no": "PN-ABC123", "qty": 1}] * 101)), {Q: rej("INVALID_PARTS")}, None),
    ("parts 100 x 999", K, ev(parts=json.dumps([{"part_no": "PN-ABC123", "qty": 999}] * 100)), {R: row(parts_count=99900)}, None),
    ("labor 1441", K, ev(labor_min="1441"), {Q: rej("INVALID_LABOR")}, None),
    ("labor decimal literal", K, ev(labor_min="1.5"), {Q: rej("INVALID_LABOR")}, None),
    ("dtc checked before parts and labor", K, ev(dtc_codes='["x"]', parts="1", labor_min="-1"), {Q: rej("INVALID_DTC")}, None),
    # §3 check 1 + §7 quarantine
    ("malformed text", K, "{oops", {Q: rej("MALFORMED_JSON", None, None), "payload": "[MALFORMED: 5 bytes, not retained]"}, None),
    ("top-level array", K, "[]", {Q: rej("MALFORMED_JSON", None, None), "payload": "[MALFORMED: 2 bytes, not retained]"}, None),
    ("trailing tokens", K, "{} {}", {Q: rej("MALFORMED_JSON", None, None), "payload": "[MALFORMED: 5 bytes, not retained]"}, None),
    ("null value", K, None, {Q: rej("MALFORMED_JSON", None, None), "payload": "[MALFORMED: 0 bytes, not retained]"}, None),
]
notes_case = ev(status='"open"', technician_notes='"SYN-NOTE-000002 replaced coolant pump"',
                parts='[{"part_no":"PN-ABC123","qty":1,"technician_notes":"SYN-NOTE-000003"}]')
payload_expected = {"vin": K, "work_order_id": "WO-00000001", "ts_ms": 1700000000000, "status": "open",
                    "service_center_id": "SC-001", "odometer": 100, "odometer_unit": "km",
                    "technician_notes": "[REDACTED]",
                    "parts": [{"part_no": "PN-ABC123", "qty": 1, "technician_notes": "[REDACTED]"}]}
cases.append(("notes redacted at any depth in quarantine payload", K, notes_case,
              {Q: rej("INVALID_STATUS"), "payload_json": payload_expected}, None))

out_cases = []
for name, key, value, expect, legacy in cases:
    c = {"name": name, "key": key, "value": value, "expect": expect}
    if legacy:
        c["legacy"] = legacy
    out_cases.append(c)
out_cases.append({"name": "invalid UTF-8", "key": K, "value_hex": "7bff7d",
                  "expect": {Q: rej("MALFORMED_JSON", None, None), "payload": "[MALFORMED: 3 bytes, not retained]"}})

W = lambda n: f'"WO-9999010{n}"'
seq = [
    {"name": "lifecycle: latest event wins",
     "events": [ev(work_order_id=W(1), ts_ms="1000"), ev(work_order_id=W(1), ts_ms="2000", status='"IN_PROGRESS"'),
                ev(work_order_id=W(1), ts_ms="3000", status='"COMPLETED"', labor_min="45")],
     "expect": {"WO-99990101": row(work_order_id="WO-99990101", event_time_ms=3000, status="COMPLETED", labor_min=45)}},
    {"name": "late event never overwrites",
     "events": [ev(work_order_id=W(2), ts_ms="3000", status='"COMPLETED"'), ev(work_order_id=W(2), ts_ms="2000", status='"IN_PROGRESS"')],
     "expect": {"WO-99990102": row(work_order_id="WO-99990102", event_time_ms=3000, status="COMPLETED")}},
    {"name": "tie on event_time: later offset wins",
     "events": [ev(work_order_id=W(3), ts_ms="1000", status='"IN_PROGRESS"'), ev(work_order_id=W(3), ts_ms="1000", status='"CANCELLED"')],
     "expect": {"WO-99990103": row(work_order_id="WO-99990103", event_time_ms=1000, status="CANCELLED")}},
    {"name": "rejected latest event leaves previous row",
     "events": [ev(work_order_id=W(4), ts_ms="1000"), ev(work_order_id=W(4), ts_ms="2000", status='"COMPLETED"', labor_min="2000")],
     "expect": {"WO-99990104": row(work_order_id="WO-99990104", event_time_ms=1000)}},
    {"name": "identical replay is one row",
     "events": [ev(work_order_id=W(5), ts_ms="1000")] * 3,
     "expect": {"WO-99990105": row(work_order_id="WO-99990105", event_time_ms=1000)}},
    {"name": "DEFECT-M1 changes which event wins",
     "events": [ev(work_order_id=W(6), ts_ms="1000"),
                ev(work_order_id=W(6), ts_ms="2000", status='"COMPLETED"', odometer="1250000", odometer_unit='"mi"')],
     "expect": {"WO-99990106": row(work_order_id="WO-99990106", event_time_ms=1000)},
     "legacy": {"defect": "DEFECT-M1",
                "expect": {"WO-99990106": row(work_order_id="WO-99990106", event_time_ms=2000, status="COMPLETED", odometer_km="2000000.0")}}},
]
doc = {"contract": "migrated/maintenance-sync/docs/contract.md v1",
       "note": "SYNTHETIC DEMO. Hand-specified from the contract; key/value are the Kafka record. 'legacy' = documented legacy deviation.",
       "cases": out_cases, "sequences": seq}
pathlib.Path(__file__).with_name("golden.json").write_text(json.dumps(doc, indent=1) + "\n")
print(len(out_cases), "cases", len(seq), "sequences")
