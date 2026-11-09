#!/usr/bin/env bash
# Reproduces the evidence in docs/EVIDENCE.md (SYNTHETIC DEMO). Outputs under reports/.
#   scripts/evidence.sh            # tests + seeded shadow run + verifier + privacy + read captures + k8s dry-run
#   scripts/evidence.sh --full     # also tools/inject_fault.sh all and tools/bench.py
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "FATAL: create .env from .env.example (LOCATION_ENC_KEY)" >&2; exit 1; }
set -a; . ./.env; set +a
PY=.venv/bin/python
SBT="${SBT:-$(command -v sbt || echo "$HOME/tools/sbt/bin/sbt")}"
KUBECTL="${KUBECTL:-$(command -v kubectl || echo "$HOME/tools/kubectl")}"
SEED="${SEED:-42}"; EVENTS="${EVENTS:-50000}"; RUN="seed${SEED}"
R=reports; mkdir -p "$R/tests" "$R/captures" "run/$RUN"
VER="$(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo -dirty)"
echo "code_version=$VER seed=$SEED events=$EVENTS" | tee "$R/evidence-run.txt"

echo "== unit tests"
(cd rust-processor && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release) > "$R/tests/rust.txt" 2>&1
(cd legacy && "$SBT" -batch test) > "$R/tests/legacy.txt" 2>&1
grep -E "test result" "$R/tests/rust.txt"; grep -E "Tests:" "$R/tests/legacy.txt"

echo "== startup hard-fail without LOCATION_ENC_KEY"
{ (env -u LOCATION_ENC_KEY rust-processor/target/release/telemetry-processor; echo "rust exit=$?") 2>&1 || true
  (cd legacy && env -u LOCATION_ENC_KEY "$SBT" -batch "runMain demo.legacy.LegacyJob" 2>&1 | grep -E "FATAL|Nonzero exit|exit code" ; true); } > "$R/captures/startup-without-key.txt"
cat "$R/captures/startup-without-key.txt"

echo "== seeded shadow run"
scripts/topics.sh --reset >/dev/null
$PY generator/generate.py --seed "$SEED" --events "$EVENTS" 2>/dev/null | tee "run/$RUN/generator.json"
rm -rf legacy/target/checkpoints
legacy/run.sh > "run/$RUN/legacy.log" 2>&1
EXIT_WHEN_IDLE_SECS=3 METRICS_ADDR=127.0.0.1:9100 SUMMARY_FILE="run/$RUN/rust-summary.json" \
  rust-processor/target/release/telemetry-processor 2> "run/$RUN/rust.log"
$PY tools/compare.py --run "$RUN"
grep -h "quarantined" "run/$RUN/rust.log" | head -3 > "$R/captures/rust-failure-log-sample.txt" || true

echo "== contract verifier (both sides)"
$PY tools/verify_contract.py "run/$RUN/rust.jsonl" --input "run/$RUN/raw.jsonl" --report "$R/verify-rust-$RUN.json" | tail -3
$PY tools/verify_contract.py "run/$RUN/legacy.jsonl" --input "run/$RUN/raw.jsonl" --report "$R/verify-legacy-$RUN.json" | tail -3

echo "== privacy audit"
tools/audit_privacy.sh "run/$RUN" "$R/privacy-audit-$RUN.json" | tail -4

echo "== authorized / unauthorized / wrong-key reads"
$PY tools/decrypt_consumer.py --limit 3 > "$R/captures/read-authorized.txt"
$PY tools/decrypt_consumer.py --limit 3 --no-key > "$R/captures/read-unauthorized.txt"
$PY tools/decrypt_consumer.py --limit 3 --wrong-key > "$R/captures/read-wrong-key.txt"
docker exec tmd-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.battery.normalized.shadow --from-beginning --max-messages 2 --isolation-level read_committed \
  > "$R/captures/read-plain-kafka-console-consumer.txt" 2>/dev/null
head -2 "$R/captures/read-unauthorized.txt"

echo "== k8s dry-run (needs an API server for discovery; see scripts/k8s_dry_run.sh)"
scripts/k8s_dry_run.sh || echo "k8s dry-run skipped/failed; see $R/k8s-dry-run.txt"

if [ "${1:-}" = "--full" ]; then
  tools/inject_fault.sh all
  $PY tools/bench.py
fi
echo "evidence complete: code_version=$VER"
