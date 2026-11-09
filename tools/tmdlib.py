"""Shared helpers for the SYNTHETIC DEMO verification tools (no plaintext coordinates are ever written)."""
from __future__ import annotations

import base64
import json
import os
import re
import time
from decimal import Decimal
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

AAD_PREFIX = "telemetry.location.v1"
OUTPUT_KEYS = {"schema_v", "vin", "event_id", "event_time", "soc_pct", "pack_voltage_v", "pack_temp_k",
               "cell_delta_mv", "firmware", "out_of_order"}
FORBIDDEN_KEYS = {"location", "lat", "lon", "latitude", "longitude", "ts_ms", "pack_temp_c"}
EVENT_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def load_env_file(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def key_from_env() -> tuple[bytes, str]:
    raw = os.environ.get("LOCATION_ENC_KEY", "")
    if not raw:
        raise SystemExit("FATAL: LOCATION_ENC_KEY is not set")
    key = base64.b64decode(raw, validate=True)
    if len(key) != 32:
        raise SystemExit("FATAL: LOCATION_ENC_KEY must decode to 32 bytes")
    return key, os.environ.get("LOCATION_ENC_KEY_ID") or "local-dev-1"


def decrypt_location(key: bytes, vin: str, event_id: str, enc: dict) -> dict:
    """Returns {"lat": Decimal, "lon": Decimal}. Raises on tamper / wrong key / wrong record."""
    nonce = base64.b64decode(enc["nonce"], validate=True)
    if len(nonce) != 12:
        raise ValueError("nonce must be 12 bytes")
    ct = base64.b64decode(enc["ciphertext"], validate=True)
    pt = AESGCM(key).decrypt(nonce, ct, f"{AAD_PREFIX}|{vin}|{event_id}".encode())
    obj = json.loads(pt, parse_float=Decimal, parse_int=Decimal)
    if set(obj) != {"lat", "lon"}:
        raise ValueError("unexpected plaintext shape")
    return {"lat": Decimal(obj["lat"]), "lon": Decimal(obj["lon"])}


def consume_all(bootstrap: str, topic: str, group: str, idle_s: float = 5.0, max_s: float = 600.0,
                from_beginning: bool = True, isolation: str = "read_committed") -> list[dict]:
    """Reads a topic to its current end (read_committed by default). Returns records with metadata."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False,
                  "isolation.level": isolation, "auto.offset.reset": "earliest",
                  "enable.partition.eof": True})
    md = c.list_topics(topic, timeout=10).topics[topic]
    parts = sorted(md.partitions)
    c.assign([TopicPartition(topic, p, 0 if from_beginning else -1) for p in parts])
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    out: list[dict] = []
    done = {p for p, e in ends.items() if e == 0}
    t0 = last = time.time()
    while len(done) < len(parts) and time.time() - t0 < max_s and time.time() - last < idle_s:
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            if m.error().code() == -191:  # _PARTITION_EOF
                done.add(m.partition())
            continue
        last = time.time()
        out.append({"partition": m.partition(), "offset": m.offset(),
                    "key": m.key().decode() if m.key() else None, "value": m.value()})
        if m.offset() + 1 >= ends[m.partition()]:
            done.add(m.partition())
    c.close()
    return out


def dump_jsonl(records: list[dict], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps({"partition": r["partition"], "offset": r["offset"], "key": r["key"],
                                "value": r["value"].decode("utf-8", "replace")}) + "\n")


def read_jsonl(path: str) -> list[dict]:
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]
