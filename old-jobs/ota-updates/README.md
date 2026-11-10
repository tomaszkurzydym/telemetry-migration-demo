# old-jobs/ota-updates (legacy, SYNTHETIC DEMO)

Legacy Scala 2.12 / Spark 3.5 Structured Streaming job that tracks vehicle over-the-air (OTA) software updates. Each
vehicle reports progress events per campaign and ECU; the job keeps the latest state of every rollout in the service
database. It is legacy code awaiting migration: do not change its behaviour, pin it with tests (see `../../AGENTS.md`).
All data is synthetic.

```
Kafka ota.events.raw (key = VIN)
  └─ OtaNormalizer.validate
       ├─ valid    → foreachBatch: OtaDb.upsert → Postgres vehicle_ota_status
       └─ rejected → Kafka ota.events.quarantine
```

## Input (`ota.events.raw`, JSON, key = VIN)

| Field | Required | Rule |
|---|---|---|
| `vin` | yes | non-empty string; must equal the Kafka key |
| `campaign_id` | yes | `OTA-NNNN-NNNN` |
| `ts_ms` | yes | integer epoch ms, `0..253402300799999` |
| `ecu` | yes | `BMS`, `VCU`, `INFOTAINMENT`, `ADAS`, `TCU` (case-sensitive) |
| `state` | yes | `OFFERED`, `DOWNLOADING`, `DOWNLOADED`, `INSTALLING`, `SUCCEEDED`, `FAILED`, `ROLLED_BACK` |
| `from_version`, `to_version` | yes | `N.N.N`, 1–4 digits each; `to_version` must be newer (see DEFECT-O1) |
| `package_sha256` | yes | 64 hex characters, any case |
| `progress_pct` | no | integer `0..100` |
| `failure_code` | for `FAILED`/`ROLLED_BACK` only | `E-NNN`; must be absent (or null) for other states |
| `session_token` | no | **sensitive**, possibly nested (e.g. under `diagnostics`); never stored or logged |

Other fields (e.g. `bytes_downloaded`, `diagnostics`) are ignored. A JSON `null` counts as absent.

## Validation order and error codes

The first failing check decides the code:

1. `MALFORMED_JSON`: not strict UTF-8, not a single JSON object, or trailing tokens.
2. `MISSING_REQUIRED_FIELD`: a required field is absent or null, or `vin` is not a non-empty string.
3. `KEY_MISMATCH`: Kafka key missing or different from `vin`.
4. `INVALID_CAMPAIGN`, 5. `INVALID_TS`, 6. `INVALID_ECU`, 7. `INVALID_STATE`, 8. `INVALID_VERSION`
   (`from_version` then `to_version`).
9. `DOWNGRADE_REJECTED`: `to_version` is not newer than `from_version` (equal versions included).
10. `INVALID_PACKAGE_HASH`, 11. `INVALID_PROGRESS`, 12. `INVALID_FAILURE_CODE`.

Empty strings count as present: `"campaign_id": ""` gives `INVALID_CAMPAIGN`, not `MISSING_REQUIRED_FIELD`
(except `vin`).

## Normalization

`package_sha256` is lower-cased. Versions, ids and `failure_code` are kept verbatim. `session_token` and unknown
fields are dropped.

## Database writes (`vehicle_ota_status`, key `(vin, campaign_id, ecu)`)

- Within a micro-batch, rows are applied per key in `(event_time, partition, offset)` order.
- An event older than the stored row (`event_time`) is ignored; an equal or newer one replaces it.
- `SUCCEEDED` and `ROLLED_BACK` are final: later events for that key are ignored, even if newer. `FAILED` is not
  final; a retry (`DOWNLOADING` again) replaces it.
- Replaying an already-applied event changes nothing, `updated_at` included.
- `written_by = 'legacy-ota-updates'`, `schema_v = 1`.

## Delivery and startup

- At-least-once per micro-batch, checkpoint in `OTA_CHECKPOINT_DIR` (default `target/checkpoints/ota-updates`),
  consumer group `ota-legacy-cg`. Database writes are idempotent; quarantine records may repeat after a crash.
- `failOnDataLoss=false`: input already deleted by Kafka retention is skipped silently.
- Missing `SERVICE_DB_PASSWORD` (or a bad `SERVICE_DB_PORT`): exits with code 78 before Spark starts.
- Environment: `KAFKA_BOOTSTRAP`, `OTA_INPUT_TOPIC`, `OTA_QUARANTINE_TOPIC`, `OTA_CHECKPOINT_DIR`,
  `SERVICE_DB_*`, `TRIGGER` (`available-now`, default, or continuous).

## Quarantine (`ota.events.quarantine`, key = VIN)

`{"schema_v":1,"error_code":…,"campaign_id":…,"vin":…,"ecu":…,"source":{"topic","partition","offset"},"payload":…}`.
`campaign_id` and `ecu` are kept only when well-formed, otherwise null. `payload` is the original record with every
`session_token` value replaced by `[REDACTED]` at any depth; a malformed record is not retained
(`[MALFORMED: N bytes, not retained]`). Redaction is by key name only.

## Privacy

`session_token` never reaches the database, the quarantine topic or the logs. Log lines carry only `campaign_id`,
`ecu` and `error_code`.

## Known defect

- **DEFECT-O1:** the downgrade guard compares versions as strings. An upgrade across a digit width
  (`1.9.4 → 1.10.2`) is rejected as `DOWNGRADE_REJECTED`, while a real downgrade (`2.10.1 → 2.9.0`) is accepted
  and stored. Pinned in `OtaCharacterizationSpec`.

## Run locally

```bash
docker compose up -d kafka service-db
scripts/ota_topics.sh --reset          # topics, schema, empty table, fresh checkpoint
.venv/bin/python generator/ota_generate.py --seed 42 --events 20000
old-jobs/ota-updates/run.sh            # reads SERVICE_DB_PASSWORD from ../../.env
```

The schema is applied by `scripts/ota_topics.sh` because the compose init script only runs on an empty volume.

## Tests

```bash
cd old-jobs/ota-updates && sbt test
```

`OtaDbSpec` needs the local `service-db` with the schema applied; it is cancelled, not failed, when unavailable.
