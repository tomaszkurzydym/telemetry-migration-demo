#!/usr/bin/env python3
"""Recovery check (SYNTHETIC DEMO): after faults, the shadow table must equal a clean run's (no lost, extra or
different rows), every quarantined source record must be present exactly as in the clean run (repeats are allowed
and counted, contract §8), and the stored positions must equal the input high watermarks."""
import argparse
import json
import subprocess
import sys
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

FIELDS = ["vin", "status", "service_center_id", "event_time_ms", "odometer_km", "dtc_codes", "parts_count", "labor_min",
          "schema_v", "written_by"]


def jsonl(p):
    return [json.loads(x) for x in open(p) if x.strip()]


ap = argparse.ArgumentParser()
for x in ["clean-db", "clean-q", "test-db", "test-q", "topic", "group", "name", "out"]:
    ap.add_argument(f"--{x}", required=True)
a = ap.parse_args()
cdb = {r["work_order_id"]: [r[f] for f in FIELDS] for r in jsonl(a.clean_db)}
tdb = {r["work_order_id"]: [r[f] for f in FIELDS] for r in jsonl(a.test_db)}
src = lambda r: (json.loads(r["value"])["source"]["partition"], json.loads(r["value"])["source"]["offset"])
cq = {src(r): json.loads(r["value"]) for r in jsonl(a.clean_q)}
tq_list = jsonl(a.test_q)
tq_counts = Counter(src(r) for r in tq_list)
tq = {src(r): json.loads(r["value"]) for r in tq_list}

c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "recovery-check"})
parts = sorted(c.list_topics(a.topic, timeout=10).topics[a.topic].partitions)
high = {p: c.get_watermark_offsets(TopicPartition(a.topic, p), timeout=10)[1] for p in parts}
c.close()
sql = f"SELECT partition || ',' || next_offset FROM maintenance_shadow_offsets WHERE consumer_group = '{a.group}' AND topic = '{a.topic}'"
outp = subprocess.run(["docker", "exec", "tmd-service-db", "sh", "-c", 'psql -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$0"', sql],
                      check=True, capture_output=True, text=True).stdout
stored = {int(p): int(o) for p, o in (l.split(",") for l in outp.split())}

report = {
    "name": a.name,
    "clean_rows": len(cdb), "test_rows": len(tdb),
    "rows_missing": len(set(cdb) - set(tdb)), "rows_extra": len(set(tdb) - set(cdb)),
    "rows_different": sum(1 for w in cdb if w in tdb and cdb[w] != tdb[w]),
    "clean_quarantine": len(cq), "test_quarantine_unique": len(tq), "test_quarantine_records": len(tq_list),
    "quarantine_missing": len(set(cq) - set(tq)), "quarantine_extra": len(set(tq) - set(cq)),
    "quarantine_different": sum(1 for s in cq if s in tq and cq[s] != tq[s]),
    "quarantine_repeats_allowed": sum(n - 1 for n in tq_counts.values()),
    "stored_positions": stored, "high_watermarks": high,
}
ok = (report["clean_rows"] > 0 and report["clean_quarantine"] > 0 and stored == high and
      all(report[k] == 0 for k in ["rows_missing", "rows_extra", "rows_different", "quarantine_missing",
                                   "quarantine_extra", "quarantine_different"]))
report["verdict"] = "PASS" if ok else "FAIL"
json.dump(report, open(a.out, "w"), indent=1)
print(json.dumps(report))
sys.exit(0 if ok else 1)
