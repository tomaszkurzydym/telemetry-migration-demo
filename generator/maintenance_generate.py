#!/usr/bin/env python3
"""Seeded synthetic vehicle-maintenance event generator (SYNTHETIC DEMO).

Emits JSON work-order events to `maintenance.events.raw`, keyed by synthetic VIN (`SYN-VIN-NNNN`).
Same (seed, events, vins) => byte-identical payloads in identical order.

Each work order moves OPEN -> IN_PROGRESS -> COMPLETED (or CANCELLED). The anomaly mix exercises the legacy
job on purpose: replayed duplicates, late (out-of-order) events, odometers in miles, lower-case and repeated
DTC codes, rounding and range boundaries, invalid fields, malformed JSON and key mismatches. Technician notes
are synthetic free text tagged `SYN-NOTE-NNNNNN` so privacy scans can find them.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

RAW_TOPIC = "maintenance.events.raw"
BASE_TS_MS = 1767225600000  # 2026-01-01T00:00:00.000Z
DTCS = ["P0A80", "P0AFA", "P1E00", "B1234", "C0035", "U0100", "P0A7F", "U3000"]
PARTS = ["PN-HV0012", "PN-CL0440", "PN-BR1020", "PN-TP0007", "PN-FL3001", "PN-WP0099"]
NOTES = ["checked HV connector torque", "coolant loop bled", "replaced cabin filter", "software update applied"]
BOUNDARY_ODOMETERS = [("2000000", "km"), ("1250000", "mi"), ("0", "km"), ("1999999.95", "km"), ("0.05", "mi")]


@dataclass
class Event:
    key: str
    value: bytes
    kind: str  # valid | duplicate | late | invalid:<code> | malformed


@dataclass
class WorkOrder:
    wo_id: str
    vin: str
    service_center: str
    odometer: str
    unit: str
    dtcs: list[str]
    status: str = "OPEN"
    parts: list[tuple[str, int]] = field(default_factory=list)
    events: list[list[tuple[str, str]]] = field(default_factory=list)


def _obj(fields: list[tuple[str, str]]) -> bytes:
    """Serialize with raw JSON fragments as values (numbers stay exact decimal literals)."""
    return ("{" + ",".join(f"{json.dumps(k)}:{v}" for k, v in fields) + "}").encode()


def _odometer(rng: random.Random) -> tuple[str, str]:
    r = rng.random()
    if r < 0.05:
        return rng.choice(BOUNDARY_ODOMETERS)
    unit = "mi" if rng.random() < 0.3 else "km"
    if r < 0.15:  # two decimals ending in 5: exercises half-up rounding
        return f"{rng.randint(0, 250_000)}.{rng.randint(0, 9)}5", unit
    return f"{rng.randint(0, 250_000)}.{rng.randint(0, 9)}", unit


def _dtcs(rng: random.Random) -> list[str]:
    codes = rng.sample(DTCS, rng.randint(0, 3))
    if codes and rng.random() < 0.25:
        codes[0] = codes[0].lower()
    if codes and rng.random() < 0.15:
        codes.append(codes[-1])
    return codes


def generate_events(seed: int, events: int, vins: int = 100) -> list[Event]:
    rng = random.Random(seed)
    vin_ids = [f"SYN-VIN-{i:04d}" for i in range(1, vins + 1)]
    clock = {v: BASE_TS_MS + rng.randint(0, 3_600_000) for v in vin_ids}
    open_wo: dict[str, WorkOrder] = {}
    done: dict[str, list[WorkOrder]] = {v: [] for v in vin_ids}
    sent: dict[str, list[Event]] = {v: [] for v in vin_ids}
    out: list[Event] = []
    wo_seq = note_seq = 0

    while len(out) < events:
        vin = rng.choice(vin_ids)
        clock[vin] += rng.randint(60_000, 3_600_000)
        roll = rng.random()
        # --- exact replay of an earlier event of this VIN (~3%)
        if roll < 0.03 and sent[vin]:
            prev = rng.choice(sent[vin][-20:])
            out.append(Event(prev.key, prev.value, "duplicate"))
            continue
        # --- late event: an earlier state of a work order re-sent with its old ts and a changed odometer (~3%)
        if roll < 0.06:
            candidates = [w for w in done[vin][-5:] + ([open_wo[vin]] if vin in open_wo else []) if len(w.events) >= 2]
            if candidates:
                w = rng.choice(candidates)
                fields = dict(rng.choice(w.events[:-1]))
                fields["odometer"] = f"{rng.randint(0, 250_000)}.5"
                out.append(Event(vin, _obj(list(fields.items())), "late"))
                continue

        w = open_wo.get(vin)
        if w is None:
            wo_seq += 1
            odo, unit = _odometer(rng)
            w = WorkOrder(f"WO-{wo_seq:08d}", vin, f"SC-{rng.randint(1, 40):03d}", odo, unit, _dtcs(rng))
            status = "OPEN"
        elif w.status == "OPEN":
            status = "IN_PROGRESS"
            w.parts = [(p, rng.randint(1, 4)) for p in rng.sample(PARTS, rng.randint(0, 3))]
        else:
            status = "COMPLETED" if rng.random() < 0.9 else "CANCELLED"

        fields = [
            ("vin", json.dumps(vin)), ("work_order_id", json.dumps(w.wo_id)), ("ts_ms", str(clock[vin])),
            ("status", json.dumps(status)), ("service_center_id", json.dumps(w.service_center)),
            ("odometer", w.odometer), ("odometer_unit", json.dumps(w.unit)), ("dtc_codes", json.dumps(w.dtcs)),
        ]
        if status != "OPEN":
            fields.append(("parts", json.dumps([{"part_no": p, "qty": q} for p, q in w.parts])))
        if status == "COMPLETED":
            fields.append(("labor_min", str(rng.choice([0, 1440] if rng.random() < 0.05 else [rng.randint(15, 600)]))))
        if rng.random() < 0.4:
            note_seq += 1
            fields.append(("technician_notes", json.dumps(f"SYN-NOTE-{note_seq:06d} {rng.choice(NOTES)}")))

        key, r2 = vin, rng.random()
        if r2 < 0.05:  # invalid record; work-order state does not advance
            idx = {k: i for i, (k, _) in enumerate(fields)}
            code = rng.choice(["MISSING_REQUIRED_FIELD", "KEY_MISMATCH", "INVALID_WORK_ORDER", "INVALID_TS",
                               "INVALID_STATUS", "INVALID_SERVICE_CENTER", "INVALID_UNIT", "INVALID_ODOMETER",
                               "INVALID_DTC", "INVALID_PARTS", "INVALID_LABOR"])
            bad = {
                "INVALID_WORK_ORDER": ("work_order_id", json.dumps("WO-12")),
                "INVALID_TS": ("ts_ms", rng.choice(["-1", "1.5e12", json.dumps("2026-01-01")])),
                "INVALID_STATUS": ("status", json.dumps(status.lower())),
                "INVALID_SERVICE_CENTER": ("service_center_id", json.dumps("SC-1")),
                "INVALID_UNIT": ("odometer_unit", json.dumps("miles")),
                "INVALID_ODOMETER": ("odometer", rng.choice(["-1.0", "2000000.1", json.dumps("12000"), "1.0005"])),
                "INVALID_DTC": ("dtc_codes", json.dumps(["P0A8"])),
                "INVALID_PARTS": ("parts", json.dumps([{"part_no": "PN-HV0012", "qty": 0}])),
                "INVALID_LABOR": ("labor_min", "1441"),
            }
            if code == "MISSING_REQUIRED_FIELD":
                drop = rng.choice(["vin", "work_order_id", "ts_ms", "status", "odometer"])
                fields = [f for f in fields if f[0] != drop]
            elif code == "KEY_MISMATCH":
                key = rng.choice([v for v in vin_ids if v != vin])
            else:
                k, v = bad[code]
                if k in idx:
                    fields[idx[k]] = (k, v)
                else:
                    fields.append((k, v))
            out.append(Event(key, _obj(fields), f"invalid:{code}"))
            continue
        if r2 < 0.06:  # malformed JSON (~1%)
            full = _obj(fields).decode()
            value = rng.choice([full[: rng.randint(5, len(full) - 1)], full.replace('"odometer":', '"odometer":NaN,"x":'),
                                "not json at all"])
            out.append(Event(key, value.encode(), "malformed"))
            continue

        ev = Event(key, _obj(fields), "valid")
        w.status = status
        w.events.append(fields)
        sent[vin].append(ev)
        out.append(ev)
        if status in ("COMPLETED", "CANCELLED"):
            open_wo.pop(vin, None)
            done[vin].append(w)
        else:
            open_wo[vin] = w
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--events", type=int, default=10_000)
    ap.add_argument("--vins", type=int, default=100)
    ap.add_argument("--rate", type=float, default=0, help="events/sec; 0 = as fast as possible")
    ap.add_argument("--topic", default=RAW_TOPIC)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    args = ap.parse_args()

    from confluent_kafka import Producer

    evs = generate_events(args.seed, args.events, args.vins)
    p = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 5, "partitioner": "murmur2_random"})
    failures = 0

    def on_delivery(err, _msg):
        nonlocal failures
        if err is not None:
            failures += 1

    t0 = time.time()
    for i, ev in enumerate(evs):
        while True:
            try:
                p.produce(args.topic, key=ev.key.encode(), value=ev.value, on_delivery=on_delivery)
                break
            except BufferError:
                p.poll(0.1)
        if args.rate > 0:
            time.sleep(max(0.0, t0 + (i + 1) / args.rate - time.time()))
        p.poll(0)
    p.flush(60)
    kinds = Counter(e.kind.split(":")[0] for e in evs)
    print(json.dumps({"seed": args.seed, "events": len(evs), "vins": args.vins, "kinds": dict(kinds),
                      "delivery_failures": failures, "elapsed_s": round(time.time() - t0, 3)}))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
