#!/usr/bin/env bash
# Recovery tests (SYNTHETIC DEMO): repeated kill -9 and a rebalance with a SIGKILLed second instance, each compared
# against a clean Rust run over the same seeded input. Do not run concurrently with the benchmark (one broker).
# Usage: recovery_test.sh <out_dir>
set -euo pipefail
cd "$(dirname "$0")/../../.."
D=$1; M=migrated/maintenance-sync; PY=.venv/bin/python; T=maintenance.events.raw; G=maintenance-rust-shadow-cg
mkdir -p "$D"
dump() {
  $PY $M/tools/db_dump.py service_records_shadow "$D/$1-db.jsonl" >/dev/null
  $PY tools/dump_topic.py maintenance.events.quarantine.shadow "$D/$1-q.jsonl" >/dev/null
}
check() {
  $PY $M/tools/check_recovery.py --clean-db "$D/clean-db.jsonl" --clean-q "$D/clean-q.jsonl" --test-db "$D/$1-db.jsonl" \
    --test-q "$D/$1-q.jsonl" --topic $T --group $G --name "$1" --out "$D/recovery-$1.json"
}
scripts/maintenance_topics.sh --reset >/dev/null
$M/scripts/shadow_reset.sh --reset >/dev/null 2>&1
$PY generator/maintenance_generate.py --seed 7 --events 100000 > "$D/generator.json"

IDLE_EXIT_SECS=5 $M/run.sh > "$D/clean.log" 2>&1
dump clean

$M/scripts/shadow_reset.sh --reset >/dev/null 2>&1
kills=0
while true; do
  IDLE_EXIT_SECS=5 $M/target/release/maintenance-sync >> "$D/kill9.log" 2>&1 &
  pid=$!
  if [[ $kills -lt 5 ]]; then
    sleep 1.5
    if kill -0 $pid 2>/dev/null; then kill -9 $pid; wait $pid 2>/dev/null || true; kills=$((kills + 1)); continue; fi
  fi
  wait $pid
  break
done
echo "{\"kill9_count\": $kills}" > "$D/kill9-meta.json"
dump kill9
rc=0; check kill9 || rc=1

$M/scripts/shadow_reset.sh --reset >/dev/null 2>&1
IDLE_EXIT_SECS=20 $M/target/release/maintenance-sync > "$D/rebalance-a.log" 2>&1 &
a=$!
sleep 1
METRICS_ADDR=127.0.0.1:9466 IDLE_EXIT_SECS=20 $M/target/release/maintenance-sync > "$D/rebalance-b.log" 2>&1 &
b=$!
sleep 3
kill -9 $b; wait $b 2>/dev/null || true
wait $a
dump rebalance
check rebalance || rc=1
exit $rc
