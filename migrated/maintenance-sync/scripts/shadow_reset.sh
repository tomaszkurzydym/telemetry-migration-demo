#!/usr/bin/env bash
# Creates (or with --reset: empties) the shadow targets of migrated/maintenance-sync: shadow table, stored positions
# and the shadow quarantine topic. Never touches the legacy table or topics (SYNTHETIC DEMO).
set -euo pipefail
cd "$(dirname "$0")/../../.."
KC="docker exec tmd-kafka /opt/kafka/bin"
BS=localhost:9092
T=maintenance.events.quarantine.shadow
PSQL=(docker exec -i tmd-service-db sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"')
"${PSQL[@]}" < migrated/maintenance-sync/sql/shadow_schema.sql
if [[ "${1:-}" == "--reset" ]]; then
  echo "TRUNCATE service_records_shadow; DELETE FROM maintenance_shadow_offsets WHERE consumer_group = '${MAINTENANCE_SHADOW_GROUP:-maintenance-rust-shadow-cg}';" | "${PSQL[@]}"
  $KC/kafka-topics.sh --bootstrap-server $BS --delete --topic $T >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do $KC/kafka-topics.sh --bootstrap-server $BS --list | grep -qxF $T || break; sleep 1; done
  $KC/kafka-consumer-groups.sh --bootstrap-server $BS --delete --group "${MAINTENANCE_SHADOW_GROUP:-maintenance-rust-shadow-cg}" >/dev/null 2>&1 || true
fi
$KC/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic $T --partitions "${PARTITIONS:-6}" --replication-factor 1 >/dev/null
echo "shadow targets ready: service_records_shadow, maintenance_shadow_offsets, $T"
