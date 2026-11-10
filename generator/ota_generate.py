#!/usr/bin/env python3
"""Seeded synthetic vehicle over-the-air (OTA) update event generator (SYNTHETIC DEMO).

Emits JSON progress events to `ota.events.raw`, keyed by synthetic VIN (`SYN-VIN-NNNN`).
Same (seed, events, vins) => byte-identical payloads in identical order.

Each vehicle works through a fixed set of campaigns, one ECU each. A campaign moves
OFFERED -> DOWNLOADING (progress) -> DOWNLOADED -> INSTALLING (progress) -> SUCCEEDED, or FAILED (and is retried)
or ROLLED_BACK. Version pairs cross digit-width boundaries on purpose (1.9.4 -> 1.10.2 upgrade, 2.10.1 -> 2.9.0
downgrade). The anomaly mix exercises the legacy job: replayed duplicates, late (out-of-order) events, events after
a final state, upper-case hashes, invalid fields, malformed JSON and key mismatches. Every event carries a synthetic
`session_token` tagged `SYN-TOK-NNNNNNNN` (sometimes nested under `diagnostics`) so privacy scans can find it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass, field

RAW_TOPIC = "ota.events.raw"
BASE_TS_MS = 1767225600000  # 2026-01-01T00:00:00.000Z
# campaign_id, ecu, to_version, from_versions (one picked per vehicle)
CAMPAIGNS = [
    ("OTA-2026-0001", "BMS", "4.2.0", ["4.1.3", "4.1.9"]),
    ("OTA-2026-0002", "VCU", "1.10.2", ["1.9.4", "1.10.0"]),
    ("OTA-2026-0003", "INFOTAINMENT", "12.0.1", ["11.4.2", "11.9.0"]),
    ("OTA-2026-0004", "ADAS", "3.0.0", ["2.10.5", "2.9.7"]),
    ("OTA-2026-0005", "TCU", "2.9.0", ["2.10.1", "2.8.7"]),
]
FAILURE_CODES = ["E-101", "E-204", "E-317", "E-450"]
INVALID_CODES = ["MISSING_REQUIRED_FIELD", "KEY_MISMATCH", "INVALID_CAMPAIGN", "INVALID_TS", "INVALID_ECU",
                 "INVALID_STATE", "INVALID_VERSION", "INVALID_PACKAGE_HASH", "INVALID_PROGRESS",
                 "INVALID_FAILURE_CODE"]
FINAL = {"SUCCEEDED", "ROLLED_BACK"}


@dataclass
class Event:
    key: str
    value: bytes
    kind: str  # valid | duplicate | late | post_final | invalid:<code> | malformed


@dataclass
class Rollout:
    vin: str
    campaign_id: str
    ecu: str
    from_version: str
    to_version: str
    sha: str
    state: str = ""
    progress: int = 0
    history: list[list[tuple[str, str]]] = field(default_factory=list)


def _obj(fields: list[tuple[str, str]]) -> bytes:
    """Serialize with raw JSON fragments as values (numbers stay exact literals)."""
    return ("{" + ",".join(f"{json.dumps(k)}:{v}" for k, v in fields) + "}").encode()


def _next_state(rng: random.Random, r: Rollout) -> tuple[str, int | None, str | None]:
    s = r.state
    if s in ("", "FAILED"):
        return ("OFFERED", None, None) if s == "" else ("DOWNLOADING", 0, None)
    if s == "OFFERED":
        return "DOWNLOADING", 0, None
    if s == "DOWNLOADING":
        if r.progress >= 100:
            return "DOWNLOADED", 100, None
        if rng.random() < 0.05:
            return "FAILED", r.progress, rng.choice(FAILURE_CODES)
        return "DOWNLOADING", min(100, r.progress + rng.randint(10, 45)), None
    if s == "DOWNLOADED":
        return "INSTALLING", 0, None
    # INSTALLING
    if r.progress >= 100:
        x = rng.random()
        if x < 0.06:
            return "ROLLED_BACK", None, rng.choice(FAILURE_CODES)
        if x < 0.10:
            return "FAILED", None, rng.choice(FAILURE_CODES)
        return "SUCCEEDED", 100, None
    return "INSTALLING", min(100, r.progress + rng.randint(20, 60)), None


def _fields(r: Rollout, ts: int, state: str, progress: int | None, failure: str | None, token: str,
            rng: random.Random) -> list[tuple[str, str]]:
    f = [("vin", json.dumps(r.vin)), ("campaign_id", json.dumps(r.campaign_id)), ("ts_ms", str(ts)),
         ("ecu", json.dumps(r.ecu)), ("state", json.dumps(state)), ("from_version", json.dumps(r.from_version)),
         ("to_version", json.dumps(r.to_version)),
         ("package_sha256", json.dumps(r.sha.upper() if rng.random() < 0.02 else r.sha))]
    if progress is not None:
        f.append(("progress_pct", str(progress)))
    if failure is not None:
        f.append(("failure_code", json.dumps(failure)))
    if rng.random() < 0.3:
        f.append(("diagnostics", _obj([("rssi_dbm", str(-rng.randint(50, 110))), ("session_token", json.dumps(token))]).decode()))
    else:
        f.append(("session_token", json.dumps(token)))
    if state == "DOWNLOADING":
        f.append(("bytes_downloaded", str(rng.randint(0, 900_000_000))))
    return f


def _invalidate(rng: random.Random, f: list[tuple[str, str]], code: str) -> tuple[list[tuple[str, str]], str | None]:
    """Return (fields, key override) that the legacy job rejects with `code`."""
    d = dict(f)
    def put(k: str, v: str) -> list[tuple[str, str]]:
        return [(kk, v if kk == k else vv) for kk, vv in f] + ([] if k in d else [(k, v)])
    if code == "MISSING_REQUIRED_FIELD":
        k = rng.choice(["campaign_id", "ts_ms", "ecu", "state", "from_version", "to_version", "package_sha256"])
        return ([x for x in f if x[0] != k] if rng.random() < 0.7 else put(k, "null")), None
    if code == "KEY_MISMATCH":
        return f, "SYN-VIN-0000"
    if code == "INVALID_CAMPAIGN":
        return put("campaign_id", json.dumps(rng.choice(["ota-2026-0001", "OTA-26-0001", "CAMPAIGN-7", ""]))), None
    if code == "INVALID_TS":
        return put("ts_ms", rng.choice(["-1", "\"1767225600000\"", "1767225600000.5", "253402300800000"])), None
    if code == "INVALID_ECU":
        return put("ecu", json.dumps(rng.choice(["bms", "GATEWAY", "", "BMS "]))), None
    if code == "INVALID_STATE":
        return put("state", json.dumps(rng.choice(["DONE", "succeeded", "PAUSED"]))), None
    if code == "INVALID_VERSION":
        return put("to_version", json.dumps(rng.choice(["4.2", "v4.2.0", "4.2.0-rc1", "4.2.0.1"]))), None
    if code == "INVALID_PACKAGE_HASH":
        return put("package_sha256", json.dumps(rng.choice(["deadbeef", "z" * 64, d["package_sha256"].strip('"')[:63]]))), None
    if code == "INVALID_PROGRESS":
        return put("progress_pct", rng.choice(["101", "-5", "50.5", "\"40\""])), None
    # INVALID_FAILURE_CODE: failure state without a valid code, or a code on a non-failure state
    if d["state"] in ('"FAILED"', '"ROLLED_BACK"'):
        return put("failure_code", json.dumps(rng.choice(["E-1", "e-101", "TIMEOUT"]))), None
    return put("failure_code", json.dumps("E-101")), None


def generate_events(seed: int, events: int, vins: int = 1000) -> list[Event]:
    rng = random.Random(seed)
    vin_ids = [f"SYN-VIN-{i:04d}" for i in range(1, vins + 1)]
    clock = {v: BASE_TS_MS + rng.randint(0, 3_600_000) for v in vin_ids}
    pending = {v: [c for c in CAMPAIGNS if rng.random() < 0.8] for v in vin_ids}
    active: dict[str, Rollout] = {}
    finished: list[Rollout] = []
    out: list[Event] = []
    token = 0

    def tok() -> str:
        nonlocal token
        token += 1
        return f"SYN-TOK-{token:08d}"

    def start(v: str) -> Rollout | None:
        if not pending[v]:
            return None
        cid, ecu, to, froms = pending[v].pop(rng.randrange(len(pending[v])))
        sha = hashlib.sha256(f"{cid}:{ecu}:{to}".encode()).hexdigest()
        return Rollout(v, cid, ecu, rng.choice(froms), to, sha)

    while len(out) < events:
        x = rng.random()
        if out and x < 0.03:
            prev = rng.choice(out)
            out.append(Event(prev.key, prev.value, "duplicate"))
            continue
        if out and x < 0.045:
            out.append(Event(rng.choice(vin_ids), rng.choice([b'{"vin":"SYN-VIN-0001","state":', b"\xff\xfe{}",
                                                              b"[]", b"not json", b""]), "malformed"))
            continue
        hist = [r for r in finished[-200:] if r.history] if x < 0.07 else []
        if hist:
            # late: a stale state with an older timestamp; post_final: a newer event after a final state
            r = rng.choice(hist)
            if rng.random() < 0.5:
                f = [(k, str(int(v) - rng.randint(1, 30_000)) if k == "ts_ms" else v) for k, v in rng.choice(r.history[:-1] or r.history)]
                out.append(Event(r.vin, _obj(f), "late"))
            else:
                clock[r.vin] += rng.randint(1_000, 60_000)
                f = _fields(r, clock[r.vin], "INSTALLING", rng.randint(0, 99), None, tok(), rng)
                out.append(Event(r.vin, _obj(f), "post_final"))
            continue
        v = rng.choice(vin_ids)
        r = active.get(v) or start(v)
        if r is None:
            continue
        active[v] = r
        state, progress, failure = _next_state(rng, r)
        clock[v] += rng.randint(1_000, 120_000)
        f = _fields(r, clock[v], state, progress, failure, tok(), rng)
        if rng.random() < 0.05:
            code = rng.choice(INVALID_CODES)
            bad, key = _invalidate(rng, f, code)
            out.append(Event(key or v, _obj(bad), f"invalid:{code}"))
            continue
        r.state, r.progress = state, progress or 0
        r.history.append(f)
        out.append(Event(v, _obj(f), "valid"))
        if state in FINAL:
            finished.append(r)
            del active[v]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--events", type=int, default=10_000)
    ap.add_argument("--vins", type=int, default=1000)
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
