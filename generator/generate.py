#!/usr/bin/env python3
"""Seeded synthetic battery-telemetry generator (SYNTHETIC DEMO).

Emits JSON events to `telemetry.battery.raw`, keyed by synthetic VIN (`SYN-VIN-NNNN`).
Same (seed, events, vins) => byte-identical payloads in identical order.

The anomaly mix exercises the contract on purpose: duplicates, out-of-order timestamps,
boundary values, half-up rounding cases, invalid records, malformed JSON, key mismatches.
Locations are drawn from open ocean (lat 10-11N, lon 29-30W) and are synthetic.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass

RAW_TOPIC = "telemetry.battery.raw"
BASE_TS_MS = 1767225600000  # 2026-01-01T00:00:00.000Z
FIRMWARES = ["bms-4.2.1", "bms-4.2.2", "bms-4.3.0", "bms-5.0.0-rc1"]


@dataclass
class Event:
    key: str
    value: bytes
    kind: str  # valid | duplicate | out_of_order | invalid:<code> | malformed
    location: tuple[str, str] | None  # ground-truth plaintext (never written anywhere by the generator)


def _dec(rng: random.Random, lo: float, hi: float, dp: int) -> str:
    """Random decimal literal with exactly `dp` digits, as text (no binary-float artefacts)."""
    scale = 10**dp
    n = rng.randint(int(lo * scale), int(hi * scale))
    sign = "-" if n < 0 else ""
    n = abs(n)
    return f"{sign}{n // scale}.{n % scale:0{dp}d}" if dp else f"{sign}{n}"


def _json_obj(fields: list[tuple[str, str]]) -> bytes:
    """Serialize with raw numeric literals (values are pre-rendered JSON fragments)."""
    return ("{" + ",".join(f"{json.dumps(k)}:{v}" for k, v in fields) + "}").encode()


def generate_events(seed: int, events: int, vins: int = 200) -> list[Event]:
    rng = random.Random(seed)
    vin_ids = [f"SYN-VIN-{i:04d}" for i in range(1, vins + 1)]
    clock = {v: BASE_TS_MS + rng.randint(0, 60_000) for v in vin_ids}
    history: dict[str, list[Event]] = {v: [] for v in vin_ids}
    out: list[Event] = []
    seq = 0
    while len(out) < events:
        vin = rng.choice(vin_ids)
        roll = rng.random()
        # --- exact duplicate of an earlier event of the same VIN (~2%)
        if roll < 0.02 and history[vin]:
            prev = rng.choice(history[vin][-50:])
            out.append(Event(prev.key, prev.value, "duplicate", prev.location))
            continue
        seq += 1
        event_id = f"evt-{seed}-{seq:08d}"
        clock[vin] += rng.randint(500, 1500)
        ts = clock[vin]
        kind = "valid"
        # --- out-of-order timestamp (~3%)
        if 0.02 <= roll < 0.05:
            ts = clock[vin] - rng.randint(5_000, 120_000)
            kind = "out_of_order"
        soc = _dec(rng, 0, 100, 2)
        temp = _dec(rng, -40, 80, 2)
        volt = _dec(rng, 300, 420, 1)
        delta = _dec(rng, 0, 60, 1)
        # boundary + half-up probes (~5%)
        r2 = rng.random()
        if r2 < 0.01:
            soc = rng.choice(["0", "100", "0.0", "100.00", "99.95", "0.05"])
        elif r2 < 0.02:
            temp = rng.choice(["-40", "80", "-40.00", "80.0", "-39.995", "79.995"])
        elif r2 < 0.03:
            soc = f"{rng.randint(0, 99)}.{rng.randint(0, 9)}5"
        elif r2 < 0.04:
            delta = f"{rng.randint(0, 59)}.5"
        elif r2 < 0.05:
            delta = rng.choice(["0", "0.0", "0.4", "0.5", "59.9"])
        fields: list[tuple[str, str]] = [
            ("vin", json.dumps(vin)),
            ("event_id", json.dumps(event_id)),
            ("ts_ms", str(ts)),
            ("soc_pct", soc),
            ("pack_voltage_v", volt),
            ("pack_temp_c", temp),
            ("cell_delta_mv", delta),
            ("firmware", json.dumps(rng.choice(FIRMWARES))),
        ]
        location = None
        if rng.random() < 0.7:
            location = (_dec(rng, 10, 11, 5), _dec(rng, -30, -29, 5))
            fields.append(("location", f'{{"lat":{location[0]},"lon":{location[1]}}}'))
        elif rng.random() < 0.1:
            fields.append(("location", "null"))
        key = vin
        # --- invalid records (~3%) and malformed JSON (~1%)
        r3 = rng.random()
        if r3 < 0.03:
            code = rng.choice(["soc", "temp", "missing", "type", "ts", "key", "loc"])
            idx = {k: i for i, (k, _) in enumerate(fields)}
            if code == "soc":
                fields[idx["soc_pct"]] = ("soc_pct", rng.choice(["100.01", "-0.01", "150", "-5"]))
                kind = "invalid:OUT_OF_RANGE_SOC"
            elif code == "temp":
                fields[idx["pack_temp_c"]] = ("pack_temp_c", rng.choice(["80.01", "-40.01", "120", "-273.15"]))
                kind = "invalid:OUT_OF_RANGE_TEMP"
            elif code == "missing":
                drop = rng.choice(["vin", "event_id", "ts_ms"])
                fields = [f for f in fields if f[0] != drop]
                kind = "invalid:MISSING_REQUIRED_FIELD"
            elif code == "type":
                fields[idx["soc_pct"]] = ("soc_pct", json.dumps(soc))
                kind = "invalid:INVALID_TYPE"
            elif code == "ts":
                fields[idx["ts_ms"]] = ("ts_ms", "-1")
                kind = "invalid:INVALID_TS"
            elif code == "key":
                key = rng.choice([v for v in vin_ids if v != vin])
                kind = "invalid:KEY_MISMATCH"
            elif code == "loc":
                if location is not None:
                    fields[idx["location"]] = ("location", f'{{"lat":{location[0]},"lon":"east"}}')
                else:
                    fields = [f for f in fields if f[0] != "location"] + [("location", '"somewhere"')]
                kind = "invalid:INVALID_LOCATION"
            value = _json_obj(fields)
        elif r3 < 0.04:
            full = _json_obj(fields).decode()
            value = rng.choice([
                full[: rng.randint(5, len(full) - 1)],  # truncated
                full.replace(f'"soc_pct":{soc}', '"soc_pct":NaN'),
                "[" + full + "]",
                "not json at all",
            ]).encode()
            kind = "malformed"
        else:
            value = _json_obj(fields)
        ev = Event(key, value, kind, location)
        out.append(ev)
        if kind in ("valid", "out_of_order"):
            history[vin].append(ev)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--events", type=int, default=10_000)
    ap.add_argument("--vins", type=int, default=200)
    ap.add_argument("--rate", type=float, default=0, help="events/sec; 0 = as fast as possible")
    ap.add_argument("--topic", default=RAW_TOPIC)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    args = ap.parse_args()

    from confluent_kafka import Producer

    evs = generate_events(args.seed, args.events, args.vins)
    p = Producer({
        "bootstrap.servers": args.bootstrap,
        "partitioner": "murmur2_random",  # Java-compatible: same VIN -> same partition as JVM clients
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
    })
    failures = 0

    def on_delivery(err, _msg):
        nonlocal failures
        if err is not None:
            failures += 1

    t0 = time.monotonic()
    for i, ev in enumerate(evs):
        while True:
            try:
                p.produce(args.topic, key=ev.key.encode(), value=ev.value, on_delivery=on_delivery)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
        if args.rate > 0:
            target = t0 + (i + 1) / args.rate
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
    p.flush(60)
    kinds: dict[str, int] = {}
    for ev in evs:
        k = ev.kind.split(":")[0]
        kinds[k] = kinds.get(k, 0) + 1
    print(json.dumps({"seed": args.seed, "events": len(evs), "vins": args.vins, "kinds": kinds,
                      "delivery_failures": failures, "elapsed_s": round(time.monotonic() - t0, 3)}))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
