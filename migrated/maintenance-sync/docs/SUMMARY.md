# Summary: maintenance-sync migration (SYNTHETIC DEMO)

The Spark job `old-jobs/maintenance-sync` (Kafka → Postgres service DB) has a Rust port in shadow in
`migrated/maintenance-sync/`. Against the same 20,000 seeded events it matches legacy with **0 unexplained
differences**; every difference is the planted miles-to-km defect (DEFECT-M1), which Rust fixes.

Approved changes from legacy behaviour (decided by a human before the contract was written):
1. Miles → km uses 1.609344 (legacy 1.6).
2. Deleted Kafka input stops the job (exit 65) instead of being skipped silently.
3. Extreme exponents such as `1e-9999999999` are `INVALID_ODOMETER`, not `MALFORMED_JSON` (DEFECT-M2).
4. The Kafka position is stored in Postgres in the same transaction as the rows.

Verified: technician notes never reach rows, quarantine or logs; startup refuses missing secrets and legacy targets;
kill -9 and rebalance leave the table identical to a clean run. Quarantine may repeat after a crash.

Before cutover (human decisions): run shadow against real traffic volumes, agree alerting on exit 65/78 and lag,
and decide how downstream consumers handle the corrected odometer values. Details: [`EVIDENCE.md`](EVIDENCE.md).
