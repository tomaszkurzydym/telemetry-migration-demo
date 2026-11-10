#!/usr/bin/env bash
# Reproduces the maintenance-sync migration evidence (SYNTHETIC DEMO). Reports: migrated/maintenance-sync/reports/.
#   migrated/maintenance-sync/scripts/evidence.sh          # tests, startup checks, seeded shadow run, privacy, controls, data loss
#   migrated/maintenance-sync/scripts/evidence.sh --full   # also kill -9 / rebalance recovery and the benchmark (sequential)
set -euo pipefail
cd "$(dirname "$0")/../../.."
[ -f .env ] || { echo "FATAL: create .env from .env.example (SERVICE_DB_PASSWORD)" >&2; exit 1; }
set -a; . ./.env; set +a
M=migrated/maintenance-sync; R=$M/reports; PY=.venv/bin/python
SBT="${SBT:-$(command -v sbt || echo "$HOME/tools/sbt/bin/sbt")}"
SEED="${SEED:-42}"; EVENTS="${EVENTS:-20000}"; RUN="seed${SEED}"
mkdir -p "$R/tests" "$R/captures"
VER="$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . ':!migrated/maintenance-sync/reports' || echo -dirty)"
echo "code_version=$VER seed=$SEED events=$EVENTS" | tee "$R/evidence-run.txt"
stamp() { $PY -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p)); d["code_version"]=sys.argv[2]; json.dump(d,open(sys.argv[3],"w"),indent=1)' "$1" "$VER" "$2"; }

echo "== unit + fixture tests"
(cd $M && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release) > "$R/tests/rust.txt" 2>&1
(cd old-jobs/maintenance-sync && "$SBT" -batch test) > "$R/tests/legacy.txt" 2>&1
grep -E "test result" "$R/tests/rust.txt"; grep -E "SKIPPED" "$R/tests/rust.txt" && { echo "FATAL: DB tests skipped"; exit 1; }
grep -E "Tests:" "$R/tests/legacy.txt"; ! grep -q "CANCELED \*\*\*" "$R/tests/legacy.txt"

echo "== startup refusals (exit 78)"
B=$M/target/release/maintenance-sync
{ (set +e; env -u SERVICE_DB_PASSWORD $B; echo "rust, no SERVICE_DB_PASSWORD: exit=$?")
  (set +e; SERVICE_DB_SHADOW_TABLE=service_records $B; echo "rust, legacy table as target: exit=$?")
  (set +e; MAINTENANCE_SHADOW_QUARANTINE_TOPIC=maintenance.events.quarantine $B; echo "rust, legacy quarantine topic: exit=$?")
  (set +e; cd old-jobs/maintenance-sync && env -u SERVICE_DB_PASSWORD "$SBT" -batch "runMain demo.oldjobs.maintenance.MaintenanceSyncJob" 2>&1 | grep -E "FATAL"; echo "legacy, no SERVICE_DB_PASSWORD: sbt exit=${PIPESTATUS[0]}")
} > "$R/captures/startup-refusals.txt" 2>&1
cat "$R/captures/startup-refusals.txt"
[[ $(grep -c "exit=78" "$R/captures/startup-refusals.txt") == 3 ]]

echo "== seeded shadow run (legacy vs Rust)"
$M/scripts/shadow_run.sh --seed "$SEED" --events "$EVENTS" --run "$RUN" | tail -4
D=$M/run/$RUN
stamp "$D/diff.json" "$R/diff-$RUN.json"; stamp "$D/privacy.json" "$R/privacy-$RUN.json"
cp "$D/generator.json" "$R/generator-$RUN.json"
awk '/\] quarantined/ && n++ < 3' "$D/rust.log" > "$R/captures/rust-failure-log-sample.txt"

echo "== comparator negative controls"
$PY $M/tools/compare_negative_control.py "$D" /tmp/negctl.json | grep verdict; stamp /tmp/negctl.json "$R/compare-negative-controls.json"

echo "== fail closed on deleted input"
$M/scripts/dataloss_test.sh /tmp/dataloss.json; stamp /tmp/dataloss.json "$R/dataloss.json"

if [[ "${1:-}" == "--full" ]]; then
  echo "== recovery (kill -9 x5, rebalance with SIGKILLed instance)"
  $M/scripts/recovery_test.sh $M/run/recovery
  for n in kill9 rebalance; do stamp $M/run/recovery/recovery-$n.json "$R/recovery-$n.json"; done
  echo "== benchmark"
  $M/scripts/benchmark.sh /tmp/bench.json >/dev/null; stamp /tmp/bench.json "$R/benchmark.json"
  grep -E "median" "$R/benchmark.json"
fi
echo "== evidence complete: $VER"
