# maintenance-sync (legacy, SYNTHETIC DEMO)

Spark Structured Streaming job that syncs vehicle maintenance work-order events into the service database.
All data is synthetic, produced by `generator/maintenance_generate.py`.

```
Kafka maintenance.events.raw (key = VIN)
  └─ validate + normalize (MaintenanceNormalizer)
       ├─ valid    → upsert Postgres service_records (one row per work_order_id)
       └─ rejected → Kafka maintenance.events.quarantine (key = VIN, technician_notes redacted)
```

## What it does today

- **Input fields:** `vin`, `work_order_id` (`WO-NNNNNNNN`), `ts_ms`, `status` (`OPEN`, `IN_PROGRESS`,
  `COMPLETED`, `CANCELLED`), `service_center_id` (`SC-NNN`), `odometer`, `odometer_unit` (`km`/`mi`), optional
  `dtc_codes`, `parts` (`[{part_no, qty}]`), `labor_min`, `technician_notes`.
- **Validation:** checks run in a fixed order and the first failure decides the error code: `MALFORMED_JSON`,
  `MISSING_REQUIRED_FIELD`, `KEY_MISMATCH`, `INVALID_WORK_ORDER`, `INVALID_TS`, `INVALID_STATUS`,
  `INVALID_SERVICE_CENTER`, `INVALID_UNIT`, `INVALID_ODOMETER`, `INVALID_DTC`, `INVALID_PARTS`, `INVALID_LABOR`.
- **Normalization:** odometer → `odometer_km` (1 dp, half-up, 0–2,000,000); DTC codes trimmed, upper-cased,
  de-duplicated and sorted; `parts` → `parts_count` (sum of `qty`); `technician_notes` dropped.
- **Database write:** batched upsert keyed by `work_order_id`. An event older than the stored row (by `event_time`)
  does not overwrite it, and replaying an already-applied event changes nothing (not even `updated_at`). Schema: [`sql/schema.sql`](sql/schema.sql).
- **Delivery:** at-least-once per micro-batch (Spark checkpoint). DB rows are idempotent; quarantine records may repeat
  after a crash.
- **Startup:** exits with code 78 before Spark starts if `SERVICE_DB_PASSWORD` is missing.
- **Privacy:** `technician_notes` never reaches the database, the quarantine topic or the logs. Failure logs carry
  `work_order_id` and `error_code` only.

## Known defect

**DEFECT-M1:** miles are converted with factor `1.6` instead of `1.609344`, e.g. 100 mi is stored as 160.0 km
instead of 160.9 km. The 2,000,000 km range check is applied after this conversion, so mile readings up to
1,250,000 mi are accepted. Pinned in `MaintenanceCharacterizationSpec`. Kept on purpose: a port should fix it and
the shadow comparison should show it as the only expected difference.

## Run locally

```bash
docker compose up -d kafka service-db        # needs SERVICE_DB_PASSWORD in .env (see .env.example)
scripts/maintenance_topics.sh --reset
.venv/bin/python generator/maintenance_generate.py --seed 42 --events 20000
old-jobs/maintenance-sync/run.sh
docker exec tmd-service-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select status, count(*) from service_records group by 1"'
```

The database lives in the `service-db-data` volume. Postgres sets the password only when it creates an empty
volume, so after changing `SERVICE_DB_PASSWORD` in `.env` recreate it: `docker compose rm -sf service-db &&
docker volume rm telemetry-migration-demo_service-db-data && docker compose up -d service-db`.

Tests: `(cd old-jobs/maintenance-sync && sbt -batch test)`.
