#!/usr/bin/env bash
# Fail-closed test (contract §8, SYNTHETIC DEMO): if records after the stored position were deleted from Kafka,
# the Rust port must exit 65 without moving its stored position or writing rows. Uses its own topic/group/table.
# Usage: dataloss_test.sh <report.json>
set -euo pipefail
cd "$(dirname "$0")/../../.."
out=$1; M=migrated/maintenance-sync; PY=.venv/bin/python
KC="docker exec tmd-kafka /opt/kafka/bin"; BS=localhost:9092
T=maintenance.test.dataloss; G=maintenance-dataloss-test-cg; TB=service_records_dataloss_test
PSQL=(docker exec -i tmd-service-db sh -c 'psql -qtA -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"')
export MAINTENANCE_INPUT_TOPIC=$T MAINTENANCE_SHADOW_GROUP=$G SERVICE_DB_SHADOW_TABLE=$TB \
  MAINTENANCE_SHADOW_QUARANTINE_TOPIC=$T.quarantine.shadow METRICS_ADDR=127.0.0.1:9467 IDLE_EXIT_SECS=3
for t in $T $T.quarantine.shadow; do $KC/kafka-topics.sh --bootstrap-server $BS --delete --topic $t >/dev/null 2>&1 || true; done
for _ in $(seq 1 60); do $KC/kafka-topics.sh --bootstrap-server $BS --list | grep -q "^$T" || break; sleep 1; done
for t in $T $T.quarantine.shadow; do $KC/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions 1 --replication-factor 1 >/dev/null; done
echo "DROP TABLE IF EXISTS $TB; CREATE TABLE $TB (LIKE service_records INCLUDING ALL); DELETE FROM maintenance_shadow_offsets WHERE consumer_group = '$G';" | "${PSQL[@]}"
q() { echo "$1" | "${PSQL[@]}"; }
$PY generator/maintenance_generate.py --seed 3 --events 300 --topic $T >/dev/null
$M/run.sh > /tmp/dataloss-1.log 2>&1; first_rc=$?
stored=$(q "SELECT next_offset FROM maintenance_shadow_offsets WHERE consumer_group='$G'")
rows=$(q "SELECT count(*) FROM $TB")
$PY generator/maintenance_generate.py --seed 4 --events 300 --topic $T >/dev/null
del=$((stored + 50))
docker exec tmd-kafka sh -c "echo '{\"partitions\":[{\"topic\":\"$T\",\"partition\":0,\"offset\":$del}],\"version\":1}' > /tmp/del.json && /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server $BS --offset-json-file /tmp/del.json" >/dev/null
set +e; $M/run.sh > /tmp/dataloss-2.log 2>&1; rc=$?; set -e
stored2=$(q "SELECT next_offset FROM maintenance_shadow_offsets WHERE consumer_group='$G'")
rows2=$(q "SELECT count(*) FROM $TB")
msg=$(grep -o 'FATAL data loss.*' /tmp/dataloss-2.log | head -1 | sed 's/"/\\"/g')
verdict=FAIL
[[ $first_rc == 0 && $stored -gt 0 && $rows -gt 0 && $rc == 65 && $stored2 == "$stored" && $rows2 == "$rows" ]] && verdict=PASS
printf '{"verdict":"%s","first_run_exit":%s,"stored_offset":%s,"rows":%s,"deleted_up_to":%s,"second_run_exit":%s,"stored_offset_after":%s,"rows_after":%s,"log":"%s"}\n' \
  $verdict $first_rc "$stored" "$rows" $del $rc "$stored2" "$rows2" "$msg" | tee "$out"
echo "DROP TABLE $TB; DELETE FROM maintenance_shadow_offsets WHERE consumer_group = '$G';" | "${PSQL[@]}"
[[ $verdict == PASS ]]
