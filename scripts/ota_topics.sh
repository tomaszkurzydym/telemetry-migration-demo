#!/usr/bin/env bash
# Create (or with --reset: delete + recreate) the ota-updates topics, ensure the vehicle_ota_status table exists, and
# with --reset also reset the legacy job's checkpoint and empty the table (SYNTHETIC DEMO).
set -euo pipefail
cd "$(dirname "$0")/.."
# Same settings as old-jobs/ota-updates/run.sh (e.g. OTA_CHECKPOINT_DIR).
if [ -f .env ]; then set -a; . ./.env; set +a; fi
KC="docker exec tmd-kafka /opt/kafka/bin"
BS=localhost:9092
TOPICS=(ota.events.raw ota.events.quarantine)
PARTITIONS=${PARTITIONS:-6}
PSQL=(docker exec -i -e PGOPTIONS=--client-min-messages=warning tmd-service-db sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"')
# The compose init script only runs on an empty volume; apply the (idempotent) schema here too.
"${PSQL[@]}" < old-jobs/ota-updates/sql/schema.sql
if [[ "${1:-}" == "--reset" ]]; then
  $KC/kafka-consumer-groups.sh --bootstrap-server $BS --delete --group ota-legacy-cg >/dev/null 2>&1 || true
  for t in "${TOPICS[@]}"; do $KC/kafka-topics.sh --bootstrap-server $BS --delete --topic "$t" >/dev/null 2>&1 || true; done
  for _ in $(seq 1 60); do
    left=$($KC/kafka-topics.sh --bootstrap-server $BS --list | grep -cxF "${TOPICS[@]/#/-e}" || true)
    [[ "$left" == "0" ]] && break
    sleep 1
  done
  # OTA_CHECKPOINT_DIR is resolved relative to the job directory, as in run.sh.
  (cd old-jobs/ota-updates && rm -rf "${OTA_CHECKPOINT_DIR:-target/checkpoints/ota-updates}")
  echo "TRUNCATE vehicle_ota_status;" | "${PSQL[@]}"
fi
for t in "${TOPICS[@]}"; do
  $KC/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic "$t" \
    --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
done
$KC/kafka-topics.sh --bootstrap-server $BS --list | grep '^ota\.'
