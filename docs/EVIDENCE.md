# Evidence — telemetry-migration-demo

> **Synthetic demonstration** ([SYNTHETIC-DEMO.md](../SYNTHETIC-DEMO.md)). Every number below comes from a file under
> `reports/` produced by a committed command. Raw topic dumps (`*.jsonl`) are regenerable and gitignored.

## Provenance
- **Code version:** all results were produced by commit **`602a983`** (`reports/evidence-run.txt`, `code_version`
  fields in each report). The run started from a clean tree. Reports written later in the run carry `602a983-dirty`
  only because the run itself rewrites the tracked files under `reports/`; no file outside `reports/` changed during
  the run. The commit after it adds only `docs/` and `reports/`.
- **Command:** `scripts/evidence.sh --full` (needs Kafka from `docker compose up -d kafka` and a `.env` with a fresh
  `LOCATION_ENC_KEY`). It runs all sections below in order and stops on the first failure.
- **Environment** (`reports/bench.json` → `environment`): 8 vCPU Intel Xeon VM, 31 GiB, Ubuntu 22.04.5, kernel 6.8,
  Docker 29.9.2, `apache/kafka:3.7.2` (1 combined KRaft broker, RF=1, 6 partitions, same host), rustc 1.99.1,
  OpenJDK 17.0.20.1 + sbt 1.10.7 (Scala 2.12.18, Spark 3.5.7), Python 3.12.16, kubectl v1.31.4, kind v0.24.0.

## 1. Tests — `reports/tests/rust.txt`, `reports/tests/legacy.txt`
```
(cd rust-processor && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release)
test result: ok. 41 passed; 0 failed   (lib: contract, crypto, pipeline, redact)
test result: ok. 1 passed; 0 failed    (tests/golden.rs — all fixtures/golden.json cases)
(cd legacy && sbt -batch test)
Tests: succeeded 24, failed 0          (LegacyCharacterizationSpec 9 incl. "DEFECT-1: cell_delta_mv is truncated"; GoldenFixtureSpec 15)
```
The crypto tests cover: round-trip, a fresh nonce per call, tampered ciphertext, tag, and nonce rejected, wrong key,
wrong `key_id`, AAD bound to (vin, event_id), and bad key length or encoding. The golden fixtures (`fixtures/golden.json`)
were written by hand from the contract, not captured from either implementation. Both implementations run them; the
legacy spec expects the DEFECT-1 value where the case says so.

## 2. Startup hard-fail — `reports/captures/startup-without-key.txt`
```
FATAL startup: LOCATION_ENC_KEY is not set; refusing to start
rust exit=78
[error] Nonzero exit code returned from runner: 78      (legacy Spark job)
```

## 3. Shadow comparison — `reports/diff-seed42.json`
Seed 42, 50,000 events (generator mix: 45,554 valid, 1,473 invalid, 1,469 out-of-order, 483 malformed, 1,021 duplicate).
The legacy job (`legacy-cg` → `telemetry.battery.normalized`) and Rust (`rust-shadow-cg` → `telemetry.battery.normalized.shadow`)
consumed the same `telemetry.battery.raw`. `tools/compare.py --run seed42`:

| Check | Result |
|---|---|
| Records (legacy / Rust) | 47,023 / 47,023, joined on (vin, event_id): 47,023 |
| Missing in Rust / extra in Rust / duplicates | 0 / 0 / 0 |
| Unexplained field mismatches | **0** |
| Intentional `DEFECT-1` (`cell_delta_mv`) | 23,587. Legacy truncates, the contract and Rust round half-up (contract §4) |
| Location crypto | 33,022 pairs. Both sides round-trip under the expected key, plaintext equal 33,022/33,022, 66,044 distinct nonces, 0 repeats |
| Per-VIN ordering violations (legacy / Rust) | 0 / 0 |
| Quarantine | 1,956 / 1,956. 0 missing, 0 extra, 0 error-code mismatches |
| **Verdict** | **PASS** |

Contract verifier (`tools/verify_contract.py <dump> --input raw.jsonl`): `reports/verify-rust-seed42.json` and
`reports/verify-legacy-seed42.json` both report `ok: true`, 47,023 records, 33,022 with `location_enc`, every
ciphertext GCM-verified, and 0 violations. Covered: schema_v 2, key = VIN, no lat/lon/location keys, base64 and
nonce/tag lengths, `key_id`, and location presence parity with the input.

## 4. Privacy — `reports/privacy-audit-seed42.json`
`tools/audit_privacy.sh run/seed42`. It scanned 8 files: both normalized dumps, both quarantine dumps, both processor
logs, and every file under `fixtures/`. Result: **0 plaintext coordinate hits**. Positive control: the same patterns
find 34,925 hits in the raw input, so the patterns do match. `location_enc` is present for all 33,022 outputs whose
input had a location, on both sides, with 0 missing and 0 unexpected. **PASS**.
Failure logging shows only event_id and error_code (`reports/captures/rust-failure-log-sample.txt`):
```
WARN  telemetry_processor] quarantined event_id=evt-42-00000021 error_code=OUT_OF_RANGE_TEMP
```

## 5. Authorized vs unauthorized reads — `reports/captures/read-*.txt`
`tools/decrypt_consumer.py --limit 3 [--no-key | --wrong-key]` on `telemetry.battery.normalized.shadow`:
```
AUTHORIZED   SYN-VIN-0134 evt-42-00000013 lat=10.39321 lon=-29.40071 (key_id=local-dev-1, GCM tag verified)
UNAUTHORIZED SYN-VIN-0134 evt-42-00000013 location_enc.ciphertext=KbFgIjwUamzXPsIhgtJy54ww... nonce=vRMhZ3gkB/jvLBwJ key_id=local-dev-1
WRONG-KEY    SYN-VIN-0134 evt-42-00000013 DECRYPT REJECTED (InvalidTag: authentication failed)
```
(The coordinates are synthetic open-ocean points. Printing them on purpose is what the authorized capture demonstrates.)
`read-plain-kafka-console-consumer.txt` shows a plain Kafka client. It sees only `location_enc`.

## 6. Recovery — `reports/recovery/{recovery.log,kill9.json,rebalance.json}`
`tools/inject_fault.sh all`, seed 7, 300,000 events. Each scenario first runs a clean baseline. It then resets only
the shadow path (shadow topics, state topic, `rust-shadow-cg` offsets), runs the faulted scenario, and compares
`read_committed` dumps with `tools/check_recovery.py`.

| Scenario | Fault | Committed outputs (baseline / faulted) | Lost | Duplicate | Field diffs | Quarantine |
|---|---|---|---|---|---|---|
| kill9 | SIGKILL after 75,000 commits, restart on the same `transactional.id`, SIGKILL again after 75,426 more, restart and drain | 282,067 / 282,067 | 0 | 0 | 0 | 12,044 / 12,044 |
| rebalance | 1 instance → add a 2nd (rebalance) → SIGKILL the 2nd after 30,000 (partitions return after session timeout) → add a 3rd → drain | 282,067 / 282,067 | 0 | 0 | 0 | 12,044 / 12,044 |

Aborted or zombie writes really happened: `read_uncommitted` sees 936 (kill9) and 464 (rebalance) extra records.
`read_committed` consumers never see them. The faulted logs show the state topic being restored on each assignment
(for example `state restored partitions=[0, 1, 2] records=4853 vins=95 in 531ms`).
**Boundary:** this shows no lost and no duplicate *committed* effects inside Kafka→Kafka (input offsets + outputs +
state in one transaction). It says nothing about systems downstream of Kafka, or about consumers using `read_uncommitted`.

## 7. Benchmark — `reports/bench.json`, `reports/bench/`
`tools/bench.py`: 1 warm-up run (discarded) + 3 seeded runs (seeds 1001–1003). Each run has a **backlog** phase
(200,000 events pre-produced, then drained) and a **paced** phase (60,000 events at 2,000 ev/s, processor already
running). Processor settings: 1 instance, `BATCH_MAX=500`, `BATCH_MAX_MS=100`, one transaction per batch.

| Run | Backlog ev/s | Backlog max lag | Paced ev/s | Paced p50 / p95 / p99 ms | Paced max lag |
|---|---|---|---|---|---|
| 1 | 3,938.2 | 199,000 | 1,986.0 | 211 / 303 / 326 | 613 |
| 2 | 4,013.7 | 199,000 | 1,992.0 | 211 / 305 / 337 | 758 |
| 3 | 4,034.9 | 199,000 | 1,992.2 | 209 / 301 / 310 | 617 |

How to read these numbers:
- The backlog-phase latency (p50 about 25 s) is mostly time spent waiting in the queue, so it is not reported as
  processing latency.
- Paced latency is generator timestamp → committed-output visibility. It includes the batch window and the
  transaction commit.
- Paced throughput equals the offered rate (2,000 ev/s), so the processor kept up at that rate.
- This is one co-located, single-broker VM with RF=1. These numbers are **not** a production capacity figure.

## 8. Kubernetes — `reports/k8s-dry-run.txt`
`scripts/k8s_dry_run.sh`. kubectl needs an API server for type discovery, so the script starts a throwaway local
`kind` control plane. It then runs `kubectl apply --dry-run=client -f k8s/` and `--dry-run=server` on every manifest
(namespaced objects are validated in `default`, because the namespace itself is only dry-run). Finally it checks that
nothing exists (`No resources found`) and deletes the cluster. All 7 objects were accepted. **The manifests have never
been deployed or run.** They are a production follow-up (see `k8s/README.md` and `docs/SUMMARY.md`).
