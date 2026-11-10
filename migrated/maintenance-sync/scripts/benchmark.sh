#!/usr/bin/env bash
# Benchmark on this VM (SYNTHETIC DEMO; not production capacity). Do not run with the recovery tests (one broker).
# 3 backlog drains of 100k events, then 3 paced runs at 2,000 ev/s (60k events) reading p99 latency (Kafka record
# timestamp -> committed DB transaction) from the Prometheus histogram. Usage: benchmark.sh <report.json>
set -euo pipefail
cd "$(dirname "$0")/../../.."
out=$1; M=migrated/maintenance-sync; PY=.venv/bin/python; TMP=$(mktemp -d)
reset() { scripts/maintenance_topics.sh --reset >/dev/null; $M/scripts/shadow_reset.sh --reset >/dev/null 2>&1; }
runs=()
for seed in 11 12 13; do
  reset
  $PY generator/maintenance_generate.py --seed $seed --events 100000 >/dev/null
  IDLE_EXIT_SECS=3 $M/target/release/maintenance-sync > "$TMP/drain-$seed.log" 2>&1
  # Drain time: partition assignment -> last committed transaction (from log timestamps).
  runs+=("$($PY - "$TMP/drain-$seed.log" <<'PY'
import json, re, sys
from datetime import datetime
lines = open(sys.argv[1]).read().splitlines()
ts = lambda l: datetime.fromisoformat(l[1:24].replace("Z", "+00:00"))
start = next(ts(l) for l in lines if "assigned partitions" in l)
end = next(ts(l) for l in reversed(lines) if "idle with zero lag" in l)
summary = json.loads(re.search(r"summary (\{.*\})", lines[-1]).group(1))
secs = (end - start).total_seconds() - 3  # idle wait
print(json.dumps({"mode": "drain", "consumed": summary["consumed"], "seconds": round(secs, 2),
                  "events_per_s": round(summary["consumed"] / secs), "db_transactions": summary["db_transactions"]}))
PY
)")
done
for seed in 21 22 23; do
  reset
  IDLE_EXIT_SECS=5 $M/target/release/maintenance-sync > "$TMP/paced-$seed.log" 2>&1 &
  pid=$!
  sleep 2
  $PY generator/maintenance_generate.py --seed $seed --events 60000 --rate 2000 >/dev/null
  sleep 3
  curl -s 127.0.0.1:9465/metrics > "$TMP/paced-$seed.prom"
  wait $pid
  runs+=("$($PY - "$TMP/paced-$seed.prom" <<'PY'
import json, re, sys
b = [(float(m.group(1)), int(m.group(2))) for m in re.finditer(r'maintenance_record_latency_seconds_bucket\{le="([0-9.e+]+)"\} (\d+)', open(sys.argv[1]).read())]
n = b[-1][1] if b else 0
q = lambda f: next((le for le, c in b if c >= f * n), None)
print(json.dumps({"mode": "paced_2000_eps", "records": n, "p50_le_s": q(0.5), "p99_le_s": q(0.99)}))
PY
)")
done
rm -rf "$TMP"
printf '%s\n' "${runs[@]}" | $PY -c '
import json, statistics, sys
runs = [json.loads(l) for l in sys.stdin if l.strip()]
d = [r["events_per_s"] for r in runs if r["mode"] == "drain"]
p = [r["p99_le_s"] for r in runs if r["mode"] != "drain"]
print(json.dumps({"note": "single VM, single broker, local Postgres; not production capacity; latency is a histogram bucket upper bound",
  "runs": runs, "drain_events_per_s_median": statistics.median(d), "paced_p99_le_s_median": statistics.median(p)}, indent=1))' | tee "$out"
