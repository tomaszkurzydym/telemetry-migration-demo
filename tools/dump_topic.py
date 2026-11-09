#!/usr/bin/env python3
"""Dump a topic to JSONL. SYNTHETIC DEMO. Usage: dump_topic.py <topic> <out.jsonl> [--uncommitted]"""
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from tmdlib import consume_all, dump_jsonl, load_env_file

load_env_file()
iso = "read_uncommitted" if "--uncommitted" in sys.argv[3:] else "read_committed"
recs = consume_all(os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"), sys.argv[1], f"dump-{uuid.uuid4().hex[:8]}", isolation=iso)
dump_jsonl(recs, sys.argv[2])
print(f"{sys.argv[1]} ({iso}): {len(recs)} records -> {sys.argv[2]}")
