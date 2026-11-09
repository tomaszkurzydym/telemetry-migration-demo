#!/usr/bin/env bash
# Create (or with --reset: delete + recreate) all demo topics and consumer groups.
set -euo pipefail
KC="docker exec tmd-kafka /opt/kafka/bin"
BS=localhost:9092
TOPICS=(
  telemetry.battery.raw
  telemetry.battery.normalized
  telemetry.battery.quarantine
  telemetry.battery.normalized.shadow
  telemetry.battery.quarantine.shadow
)
STATE_TOPIC=telemetry.battery.rust-shadow.state
PARTITIONS=${PARTITIONS:-6}
GROUPS_TO_RESET=(legacy-cg rust-shadow-cg)

if [[ "${1:-}" == "--reset-shadow" ]]; then
  # Reset only the Rust shadow path (outputs, state, group offsets); keeps raw input and legacy outputs.
  TOPICS=(telemetry.battery.normalized.shadow telemetry.battery.quarantine.shadow)
  GROUPS_TO_RESET=(rust-shadow-cg)
  set -- --reset
fi
if [[ "${1:-}" == "--reset" ]]; then
  for g in "${GROUPS_TO_RESET[@]}"; do
    $KC/kafka-consumer-groups.sh --bootstrap-server $BS --delete --group "$g" >/dev/null 2>&1 || true
  done
  for t in "${TOPICS[@]}" "$STATE_TOPIC"; do
    $KC/kafka-topics.sh --bootstrap-server $BS --delete --topic "$t" >/dev/null 2>&1 || true
  done
  # topic deletion is asynchronous
  for _ in $(seq 1 60); do
    left=$($KC/kafka-topics.sh --bootstrap-server $BS --list | grep -cxF -e "$STATE_TOPIC" "${TOPICS[@]/#/-e}" || true)
    [[ "$left" == "0" ]] && break
    sleep 1
  done
fi

for t in "${TOPICS[@]}"; do
  $KC/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic "$t" \
    --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
done
# Rust processor state changelog: compacted, partition-aligned with the input topic.
$KC/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic "$STATE_TOPIC" \
  --partitions "$PARTITIONS" --replication-factor 1 \
  --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.1 >/dev/null
$KC/kafka-topics.sh --bootstrap-server $BS --list | grep '^telemetry\.battery\.'
