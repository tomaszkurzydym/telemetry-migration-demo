#!/usr/bin/env python3
"""Privileged decrypting consumer (SYNTHETIC DEMO).

Authorized mode (LOCATION_ENC_KEY set): decrypts location_enc and prints recovered coordinates.
Unauthorized mode (--no-key, or key absent): prints only the opaque location_enc fields — there is no way
to recover coordinates without the key. A wrong key is rejected by GCM authentication.

Usage: decrypt_consumer.py [--topic T] [--limit N] [--no-key] [--wrong-key]
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from tmdlib import decrypt_location, load_env_file


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="telemetry.battery.normalized.shadow")
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--no-key", action="store_true", help="simulate a consumer without key access")
    ap.add_argument("--wrong-key", action="store_true", help="use a freshly generated (wrong) key")
    a = ap.parse_args()
    if not a.no_key:
        load_env_file()
    raw = os.environ.get("LOCATION_ENC_KEY", "") if not a.no_key else ""
    key = os.urandom(32) if a.wrong_key else (base64.b64decode(raw) if raw else None)
    mode = "WRONG-KEY" if a.wrong_key else ("AUTHORIZED" if key else "UNAUTHORIZED (no key)")
    print(f"# decrypt_consumer mode={mode} topic={a.topic}")

    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                  "group.id": f"decrypt-consumer-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    c.subscribe([a.topic])
    shown = idle = 0
    while shown < a.limit and idle < 20:
        m = c.poll(0.5)
        if m is None or m.error():
            idle += 1
            continue
        rec = json.loads(m.value())
        enc = rec.get("location_enc")
        if not enc:
            continue
        shown += 1
        if key is None:
            print(f"{rec['vin']} {rec['event_id']} location_enc.ciphertext={enc['ciphertext']} nonce={enc['nonce']} key_id={enc['key_id']}")
            continue
        try:
            loc = decrypt_location(key, rec["vin"], rec["event_id"], enc)
            print(f"{rec['vin']} {rec['event_id']} lat={loc['lat']} lon={loc['lon']} (key_id={enc['key_id']}, GCM tag verified)")
        except Exception as e:
            print(f"{rec['vin']} {rec['event_id']} DECRYPT REJECTED ({type(e).__name__}: authentication failed)")
    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
