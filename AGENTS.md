# AGENTS.md

Synthetic demonstration (see `SYNTHETIC-DEMO.md`): a battery-telemetry validation/normalization job migrated from
Scala 2.12 / Spark 3.5 Structured Streaming (`legacy/`) to a Rust rdkafka processor (`rust-processor/`), with
field-level AES-256-GCM encryption of vehicle location. The Rust port runs in shadow (`rust-shadow-cg`,
`*.shadow` topics) and is proven against legacy by the tools in `tools/`. `docs/contract.md` is the source of truth.

## Setup and commands

```bash
cp .env.example .env            # put a fresh key in LOCATION_ENC_KEY (generator command is in the file)
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
docker compose up -d kafka      # apache/kafka:3.7.2, one KRaft broker, container tmd-kafka
set -a; . ./.env; set +a
(cd rust-processor && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release)
(cd legacy && sbt -batch test)
scripts/evidence.sh             # tests + seeded shadow run + verifier + privacy + reads + k8s dry-run
scripts/evidence.sh --full      # also fault injection + benchmark (~25 min)
```

Run both test suites before every commit. Run `scripts/evidence.sh --full` before any PR that changes
processing behaviour, crypto, the contract or the tools, and update `docs/EVIDENCE.md` / `docs/SUMMARY.md`.

## Project rules

- **Synthetic only.** VINs are `SYN-VIN-NNNN`, locations are open-ocean coordinates, keys come from git-ignored `.env`.
  Never add real-looking customer data or claim anything originated from a customer.
- **No plaintext coordinates** in outputs, quarantine, logs, reports or fixtures. Fixtures use `@LOC-*` tokens.
  Failure logs carry only `event_id` + `error_code`. `tools/audit_privacy.sh` must report 0 hits.
- **Contract first.** Behaviour changes start in `docs/contract.md` and `fixtures/golden.json` (expected values
  written by hand from the contract, never captured from an implementation), then Rust and legacy. Use the
  `change-contract` skill.
- **Crypto wire format is shared by three implementations.** Any change to AAD/nonce/encoding must update
  `rust-processor/src/crypto.rs`, `legacy/.../LegacyCrypto.scala` and `tools/tmdlib.py` together. AAD is
  `telemetry.location.v2|<vin utf-8 byte length>|<vin>|<event_id>`.
- **DEFECT-1 stays in legacy.** Legacy truncates `cell_delta_mv`; Rust rounds half-up. It is pinned by
  characterization tests and must be the only difference class reported by `tools/compare.py`.
- **Keys:** missing/invalid `LOCATION_ENC_KEY` must fail startup (exit 78) before Kafka is touched. Never print keys.
- **Claims:** never claim exactly-once beyond the tested Kafka-to-Kafka boundary, global ordering, production-scale
  performance, KMS-grade key management, compliance certification, or a Kubernetes deployment (`k8s/` is dry-run only).

## Pitfalls

- Fault injection and the benchmark share the single broker: never run them concurrently.
- `kubectl apply --dry-run=client` still needs API discovery; `scripts/k8s_dry_run.sh` uses a throwaway kind cluster.
- `reports/` is tracked, so reports written late in an evidence run are tagged `<sha>-dirty`. Commit code first,
  run evidence, then commit reports in a separate "Evidence: regenerate ..." commit.
- Use a different `--run` name for ad-hoc runs (e.g. `demo`) so committed `reports/*-seed42.json` are not overwritten.
- Raw dumps under `run/` contain synthetic plaintext input and are git-ignored; keep them out of commits.

## Old jobs and migrations

- `old-jobs/<job>/` holds legacy jobs awaiting migration. Treat them as legacy: do not change their behaviour;
  pin it with characterization tests. Known defects are labelled `DEFECT-<X>n` in code and tests.
- A port of `old-jobs/<job>/` goes in `migrated/<job>/` as a self-contained project (code, contract, fixtures,
  tests, README, evidence), built with the `migrate-streaming-job` skill. A port in shadow never writes to the
  legacy job's topics or tables.
- `old-jobs/maintenance-sync`: Kafka `maintenance.events.raw` → Postgres `service_records` (compose service
  `service-db`, container `tmd-service-db`) + quarantine topic `maintenance.events.quarantine`.
  `technician_notes` is free text and must never reach the database, quarantine or logs.
  Setup: `docker compose up -d kafka service-db && scripts/maintenance_topics.sh --reset`, then
  `.venv/bin/python generator/maintenance_generate.py --seed 42 --events 20000` and `old-jobs/maintenance-sync/run.sh`.

## Skills (`.agents/skills/`)

| Skill | Use when |
|---|---|
| `migrate-streaming-job` | Porting another streaming job with the same characterize → contract → shadow → evidence method |
| `change-contract` | Changing validation/normalization/encryption behaviour in both implementations |
| `regenerate-evidence` | Re-running and re-publishing the evidence pack after a change |
| `run-demo` | Running the 15-minute live demo without touching committed evidence |
