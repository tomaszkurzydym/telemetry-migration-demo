#!/usr/bin/env bash
# One seeded shadow run (SYNTHETIC DEMO): reset, generate, legacy job, Rust port, dumps, comparison, privacy audit.
# Usage: shadow_run.sh [--seed N] [--events N] [--run NAME]   Output: migrated/maintenance-sync/run/<NAME>/ (git-ignored)
set -euo pipefail
cd "$(dirname "$0")/../../.."
SEED=42; EVENTS=20000; RUN=demo
while [[ $# -gt 0 ]]; do case $1 in --seed) SEED=$2; shift 2;; --events) EVENTS=$2; shift 2;; --run) RUN=$2; shift 2;; *) echo "unknown $1"; exit 2;; esac; done
M=migrated/maintenance-sync; D=$M/run/$RUN; PY=.venv/bin/python
rm -rf "$D"; mkdir -p "$D"
scripts/maintenance_topics.sh --reset >/dev/null
$M/scripts/shadow_reset.sh --reset >/dev/null
$PY generator/maintenance_generate.py --seed "$SEED" --events "$EVENTS" | tee "$D/generator.json"
t0=$(date +%s.%N); old-jobs/maintenance-sync/run.sh > "$D/legacy.log" 2>&1; t1=$(date +%s.%N)
IDLE_EXIT_SECS=5 $M/run.sh > "$D/rust.log" 2>&1; t2=$(date +%s.%N)
echo "{\"legacy_wall_s\": $(echo "$t1 - $t0" | bc), \"rust_wall_s_incl_5s_idle\": $(echo "$t2 - $t1" | bc)}" | tee "$D/timing.json"
$PY tools/dump_topic.py maintenance.events.raw "$D/raw.jsonl"
$PY tools/dump_topic.py maintenance.events.quarantine "$D/legacy-q.jsonl"
$PY tools/dump_topic.py maintenance.events.quarantine.shadow "$D/rust-q.jsonl"
$PY $M/tools/db_dump.py service_records "$D/legacy-db.jsonl"
$PY $M/tools/db_dump.py service_records_shadow "$D/rust-db.jsonl"
grep -h "summary" "$D/rust.log" | tail -1
rc=0
$PY $M/tools/compare.py --raw "$D/raw.jsonl" --legacy-db "$D/legacy-db.jsonl" --rust-db "$D/rust-db.jsonl" \
  --legacy-q "$D/legacy-q.jsonl" --rust-q "$D/rust-q.jsonl" --out "$D/diff.json" || rc=1
$M/tools/audit_privacy.sh "$D" "$D/privacy.json" || rc=1
exit $rc
