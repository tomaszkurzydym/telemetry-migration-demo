#!/usr/bin/env bash
# Create (or with --reset: delete + recreate) the maintenance-sync topics, reset the legacy job's checkpoint and
# empty the service_records table (SYNTHETIC DEMO).
set -euo pipefail
cd "$(dirname "$0")/.."
KC="docker exec tmd-kafka /opt/kafka/bin"
BS=localhost:9092
TOPICS=(maintenance.events.raw maintenance.events.quarantine)
PARTITIONS=${PARTITIONS:-6}
if [[ "${1:-}" == "--reset" ]]; then
  $KC/kafka-consumer-groups.sh --bootstrap-server $BS --delete --group maintenance-legacy-cg >/dev/null 2>&1 || true
  for t in "${TOPICS[@]}"; do $KC/kafka-topics.sh --bootstrap-server $BS --delete --topic "$t" >/dev/null 2>&1 || true; done
  for _ in $(seq 1 60); do
    left=$($KC/kafka-topics.sh --bootstrap-server $BS --list | grep -cxF "${TOPICS[@]/#/-e}" || true)
    [[ "$left" == "0" ]] && break
    sleep 1
  done
  # CHECKPOINT_DIR is resolved relative to the job directory, as in run.sh.
  (cd old-jobs/maintenance-sync && rm -rf "${CHECKPOINT_DIR:-target/checkpoints/maintenance-sync}")
  docker exec tmd-service-db sh -c 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "TRUNCATE service_records"'
fi
for t in "${TOPICS[@]}"; do
  $KC/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic "$t" \
    --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
done
$KC/kafka-topics.sh --bootstrap-server $BS --list | grep '^maintenance\.'
