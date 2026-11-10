# Evidence: maintenance-sync migration (SYNTHETIC DEMO)

Reproduce: `migrated/maintenance-sync/scripts/evidence.sh --full` (needs `docker compose up -d kafka service-db`).
All results below are from code version **`8a63005`** (stamped as `code_version` in every report under
[`../reports/`](../reports/)), one VM, one Kafka broker, local Postgres 16.6. Synthetic data only.

| Check | Result | Report |
|---|---|---|
| Rust tests (unit + independent golden fixtures, fmt, clippy `-D warnings`) | 6 passed; DB-backed fixture sequences ran (not skipped) | `tests/rust.txt` |
| Legacy characterization + same golden fixtures | 19 passed | `tests/legacy.txt` |
| Startup refusals | Rust exit 78 without `SERVICE_DB_PASSWORD`, with the legacy table, with the legacy quarantine topic; legacy job refuses without password | `captures/startup-refusals.txt` |
| Shadow comparison, seed 42, 20,000 events | **PASS, 0 unexplained** | `diff-seed42.json` |
| Comparator negative controls (12 tamperings) | each makes the comparator FAIL | `compare-negative-controls.json` |
| Privacy (`SYN-NOTE-nnnnnn`) | 0 hits in Rust DB rows, quarantine, logs; positive control 8,029 hits in raw input | `privacy-seed42.json` |
| Fail closed on deleted input | Kafka records deleted past the stored position → exit 65, position and rows unchanged | `dataloss.json` |
| kill -9 ×5 during a 100k-event run | final table = clean run (29,188 rows, 0 missing/extra/different); 6,727 quarantine sources, 0 missing; 24 repeated deliveries | `recovery-kill9.json` |
| Rebalance (2 instances, one SIGKILLed) | same: 0 missing/extra/different; 41 repeated quarantine deliveries | `recovery-rebalance.json` |
| Benchmark | backlog drain median 2,758 ev/s (2,734–2,817); paced 2,000 ev/s: p50 and p99 ≤ 0.5 s (p99 per run: 0.5, 0.5, 0.5 s) | `benchmark.json` |

## Shadow comparison in detail

Legacy wrote 5,928 rows and 1,104 quarantine records; Rust 5,866 and 1,322. 4,188 rows are identical. Every other
difference is attributed to **DEFECT-M1** (legacy miles ×1.6, Rust ×1.609344) by recomputing the mile record:

- 1,678 rows whose `odometer_km` differs (e.g. 100 mi → 160.0 legacy, 160.9 Rust);
- 62 work orders with no Rust row and 218 extra Rust quarantine records: mile readings that pass the 2,000,000 km
  limit only with the wrong factor;
- 2 records where Rust's earlier `INVALID_ODOMETER` replaces legacy's later `INVALID_DTC`/`INVALID_PARTS`.

Explained records are still checked: every Rust quarantine payload must equal its redacted source record, and a
row explained by a different winner must be the corrected normalization of a real event of that work order, with
every newer event of it quarantined by Rust.
DEFECT-M2 (extreme negative exponents) does not occur in the seeded data; it is covered by the golden fixtures.

## Delivery guarantee (what is and isn't claimed)

- Rows and the consumed Kafka position commit in **one Postgres transaction**: after kill -9 or a rebalance the
  table equals a clean run's, with no lost or extra rows, and stored positions end at the high watermarks.
- Quarantine is **at-least-once**: records are acknowledged by Kafka before the DB transaction, so a crash can
  repeat them (24 and 41 repeats above). Consumers identify them by source `(topic, partition, offset)`.
- Not claimed: end-to-end exactly-once, global ordering, production capacity. The benchmark is a single VM;
  latency (Kafka record timestamp → committed transaction) is a histogram bucket bound, not an exact p99.
