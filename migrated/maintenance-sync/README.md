# migrated/maintenance-sync (SYNTHETIC DEMO)

Rust port of [`old-jobs/maintenance-sync`](../../old-jobs/maintenance-sync/): Kafka `maintenance.events.raw` →
validate/normalize → Postgres service DB, rejects → quarantine topic. Built with the `migrate-streaming-job` skill.
All data is synthetic; nothing here is customer code or data.

It runs **in shadow** and never touches the legacy outputs:

| | Legacy | Rust port |
|---|---|---|
| Consumer group | `maintenance-legacy-cg` | `maintenance-rust-shadow-cg` |
| Service DB table | `service_records` | `service_records_shadow` (startup refuses `service_records`) |
| Quarantine topic | `maintenance.events.quarantine` | `maintenance.events.quarantine.shadow` (startup refuses the legacy topic) |
| Position | Spark checkpoint; `failOnDataLoss=false` | `maintenance_shadow_offsets`, same DB transaction as the rows; fails closed (exit 65) on deleted input |
| Miles → km | ×1.6 (DEFECT-M1) | ×1.609344 |

- [`docs/contract.md`](docs/contract.md): the behaviour both sides are judged against, incl. approved deviations DEFECT-M1/M2.
- [`fixtures/golden.json`](fixtures/golden.json): hand-written expectations (from `fixtures/build_golden.py`), run by
  `tests/golden.rs` and by the legacy `GoldenFixtureSpec`.
- [`docs/EVIDENCE.md`](docs/EVIDENCE.md): what was checked and the results.

## Run

```bash
cp ../../.env.example ../../.env            # once; sets SERVICE_DB_PASSWORD (synthetic)
docker compose up -d kafka service-db        # from the repo root
scripts/shadow_reset.sh                      # create shadow table, offsets table, shadow quarantine topic
./run.sh                                     # IDLE_EXIT_SECS=5 to exit when caught up; metrics on 127.0.0.1:9465/metrics
```

Exit codes: 0 ok · 2 runtime error (restart resumes from the stored position) · 3 fenced by another writer ·
65 input deleted before it was processed · 78 bad config (missing `SERVICE_DB_PASSWORD`, legacy target, missing shadow tables).

## Verify

```bash
scripts/shadow_run.sh --seed 42 --events 20000   # legacy + Rust on the same input, comparison + privacy audit
scripts/evidence.sh --full                       # everything in docs/EVIDENCE.md, reports stamped with the commit
```
