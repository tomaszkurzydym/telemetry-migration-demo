#!/usr/bin/env python3
"""Dump a service DB table to JSONL, one row per line (SYNTHETIC DEMO). Usage: db_dump.py <table> <out.jsonl>"""
import re
import subprocess
import sys

table, out = sys.argv[1], sys.argv[2]
assert re.fullmatch(r"[a-z_][a-z0-9_]*", table), table
sql = (f"COPY (SELECT row_to_json(t) FROM (SELECT work_order_id, vin, status, service_center_id, "
       f"(extract(epoch FROM event_time) * 1000)::bigint AS event_time_ms, odometer_km::text AS odometer_km, dtc_codes, "
       f"parts_count, labor_min, schema_v, written_by, updated_at FROM {table} ORDER BY work_order_id) t) TO STDOUT")
data = subprocess.run(["docker", "exec", "tmd-service-db", "sh", "-c", 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$0"', sql],
                      check=True, capture_output=True).stdout
open(out, "wb").write(data)
print(f"{table}: {data.count(b'\n')} rows -> {out}")
