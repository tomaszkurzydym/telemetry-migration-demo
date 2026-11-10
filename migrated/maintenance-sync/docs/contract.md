# maintenance-sync contract (v1) — SYNTHETIC DEMO

Written from the characterized legacy job `old-jobs/maintenance-sync` (tests in its `src/test`). Both
implementations are held to this document; `fixtures/golden.json` encodes it by hand. Deviations of the legacy
job are listed in §9 and are the only differences the shadow comparison accepts.

## 1. Topics, tables, groups

| | Legacy | Rust port (shadow) |
|---|---|---|
| Input (key = VIN, value = JSON) | `maintenance.events.raw` | same topic, read only |
| Consumer group | `maintenance-legacy-cg` | `maintenance-rust-shadow-cg` |
| Service DB table | `service_records` | `service_records_shadow` (same columns) |
| Quarantine topic (key = VIN or null) | `maintenance.events.quarantine` | `maintenance.events.quarantine.shadow` |
| `written_by` | `legacy-maintenance-sync` | `rust-maintenance-sync` |

The port refuses to start (exit 78) if configured to write to `service_records` or `maintenance.events.quarantine`.

## 2. Input record

JSON object, UTF-8. Required: `vin`, `work_order_id`, `ts_ms`, `status`, `service_center_id`, `odometer`,
`odometer_unit`. Optional: `dtc_codes`, `parts`, `labor_min`, `technician_notes` (free text, sensitive).
Unknown fields are ignored. "Present" means the key exists and is not JSON `null`.

## 3. Validation (in this order; the first failing check decides `error_code`)

| # | Check | error_code |
|---|---|---|
| 1 | Value is strict UTF-8, one JSON object, no trailing tokens | `MALFORMED_JSON` |
| 2 | Every required field present; `vin` is a non-empty string | `MISSING_REQUIRED_FIELD` |
| 3 | Kafka key equals `vin` (null key never matches) | `KEY_MISMATCH` |
| 4 | `work_order_id` string matching `WO-[0-9]{8}` | `INVALID_WORK_ORDER` |
| 5 | `ts_ms` integer literal (no `.`/exponent), 0 ≤ ts_ms ≤ 253402300799999 | `INVALID_TS` |
| 6 | `status` ∈ {`OPEN`,`IN_PROGRESS`,`COMPLETED`,`CANCELLED`} (case-sensitive) | `INVALID_STATUS` |
| 7 | `service_center_id` string matching `SC-[0-9]{3}` | `INVALID_SERVICE_CENTER` |
| 8 | `odometer_unit` ∈ {`km`,`mi`} (case-sensitive) | `INVALID_UNIT` |
| 9 | `odometer` per §4 | `INVALID_ODOMETER` |
| 10 | `dtc_codes` absent, or array of ≤ 32 strings each matching `[PBCU][0-9A-F]{4}` after trim + upper-case | `INVALID_DTC` |
| 11 | `parts` absent, or array of ≤ 100 objects with `part_no` matching `PN-[0-9A-Z]{6}` and integer `qty` 1..999 | `INVALID_PARTS` |
| 12 | `labor_min` absent, or integer literal 0..1440 | `INVALID_LABOR` |

Empty strings are present: they fail the field's own check (e.g. `"service_center_id": ""` → `INVALID_SERVICE_CENTER`),
except `vin` (check 2). Regexes match the whole string. Duplicate JSON keys: last value wins.

## 4. Odometer

`odometer` must be a JSON number, value `d` (exact decimal):
- `d` ≥ 0 and `d` ≤ 2000000 (raw value, before unit conversion); at most 3 decimal places after removing trailing
  zeros (`-0` is 0). Exponent notation is allowed (`1.5e2` = 150). Anything else → `INVALID_ODOMETER`. That
  includes a number whose exponent puts it out of range or past 3 decimals, however extreme the exponent
  (e.g. `1e9999999999`, `1e-9999999999`); it is never parsed into an unbounded value.
- `km = d` for `km`; `km = d × 1.609344` for `mi`; then rounded to 1 decimal place, half-up.
- `km` ≤ 2000000.0, else `INVALID_ODOMETER`. So the largest accepted mile reading is 1242742.415 mi
  (→ 2000000.049… → 2000000.0 km); 1242742.416 mi (→ 2000000.1 km) is rejected.

## 5. Normalization (valid record → one row)

| Column | Value |
|---|---|
| `work_order_id`, `vin`, `status`, `service_center_id` | as validated |
| `event_time` | `ts_ms` as UTC timestamp, millisecond precision |
| `odometer_km` | §4, `numeric(9,1)` |
| `dtc_codes` | trimmed (chars ≤ U+0020), upper-cased, de-duplicated, sorted ascending; absent → `{}` |
| `parts_count` | Σ `qty`; absent → 0 |
| `labor_min` | value or NULL |
| `schema_v` | 1 |

`technician_notes` is never written to the database.

## 6. Service DB write rules (per `work_order_id`)

- Upsert on `work_order_id`. An incoming row replaces the stored one only if
  `incoming.event_time >= stored.event_time` **and** some business column differs. So a late (older) event never
  overwrites, an identical replay is a no-op, and `updated_at` moves only on a real change.
- Ties on `event_time`: the record with the higher input offset wins (all events of a work order share a VIN, so
  they are in one partition and are applied in offset order).
- The final table after consuming a topic is therefore a function of the input only.

## 7. Quarantine record

```json
{"schema_v":1,"error_code":"INVALID_STATUS","work_order_id":"WO-00000001","vin":"SYN-VIN-0001",
 "source":{"topic":"maintenance.events.raw","partition":2,"offset":7},"payload":"<redacted original JSON>"}
```
- `work_order_id`: the input's value only if it is a string matching `WO-[0-9]{8}`, else null.
  `vin`: the input's value if it is a non-empty string, else null. Both null for `MALFORMED_JSON`.
- `payload`: a JSON string holding the parsed input, re-serialized, with every `technician_notes` value at any depth replaced by
  `"[REDACTED]"`. `MALFORMED_JSON` payloads are not retained: `"[MALFORMED: <n> bytes, not retained]"`
  (n = value length in bytes; 0 for a null value). Payloads are compared as JSON values (numbers by value), not
  byte-for-byte: number formatting and key escaping may differ between implementations.
- Kafka key: `vin`, or null. Identity of a quarantine record is `source` (topic, partition, offset).

## 8. Delivery, startup, logging

- **Legacy:** at-least-once per Spark micro-batch; DB upserts make row replays harmless; quarantine may repeat;
  expired offsets are skipped silently (`failOnDataLoss=false`).
- **Port:** the consumed position is stored in Postgres (`maintenance_shadow_offsets`) in the same transaction as
  the row upserts, so the DB rows and the position cannot diverge across crashes, restarts or rebalances. A
  stale writer is fenced: the transaction aborts if the stored position moved. Quarantine records are produced
  (acks=all, idempotent) and flushed before that transaction commits, so they are at-least-once and may repeat
  after a crash; consumers de-duplicate on `source`.
- **Port, data loss:** if a stored position is below the partition's earliest available offset, or the broker
  reports an out-of-range offset, the port stops with exit code 65 and writes nothing for that partition.
- Missing `SERVICE_DB_PASSWORD` → exit 78 before connecting to Kafka or the DB.
- Logs: `work_order_id` and `error_code` only for rejects; never payloads or `technician_notes`.

## 9. Accepted legacy deviations (labelled in the shadow comparison)

| Id | Legacy | Contract |
|---|---|---|
| `DEFECT-M1` | miles × 1.6 (100 mi → 160.0 km; accepts up to 1250000 mi) | × 1.609344 (100 mi → 160.9 km; up to 1242742 mi) |
| `DEFECT-M2` | `odometer` with an exponent beyond Jackson's range (e.g. `1e-9999999999`) fails the whole record as `MALFORMED_JSON` | `INVALID_ODOMETER` (the record is valid JSON) |

Not a deviation but a stated limit: JSON nesting deeper than 128 levels → `MALFORMED_JSON` in the port; the
generator never produces such input.
