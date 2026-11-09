# Executive summary — telemetry-migration-demo

> **Synthetic demonstration** ([SYNTHETIC-DEMO.md](../SYNTHETIC-DEMO.md)). No customer code or data; all VINs,
> locations and keys are generated. Nothing here is a production deployment or a compliance claim.

## What was done
A battery-health telemetry validation/normalization job was ported from **Scala 2.12 / Spark 3.5 Structured
Streaming** (`legacy/`) to a **Rust Kafka processor** (`rust-processor/`, rdkafka). Both implement
[`docs/contract.md`](contract.md), including field-level **AES-256-GCM** encryption of vehicle location
(`location_enc {ciphertext, nonce, key_id}`, random 12-byte nonce, AAD bound to VIN + event_id). The Rust port
ran in **shadow mode** — own consumer group `rust-shadow-cg`, own `*.shadow` topics — next to the legacy job
(`legacy-cg`) on the same seeded input, and was compared record by record.

## Results (details and reproduction commands: [EVIDENCE.md](EVIDENCE.md), code version `53aad3a`)
| Area | Result |
|---|---|
| Unit / fixture tests | Rust 42 passed (41 unit + golden), fmt + clippy `-D warnings` clean; Scala 23 passed (8 characterization + 15 golden) |
| Shadow diff (seed 42, 50,000 events) | **PASS** — 47,023 joined, 0 missing, 0 extra, **0 unexplained mismatches**, 0 ordering violations; quarantine parity 1,956 / 1,956 |
| DEFECT-1 | 23,587 `cell_delta_mv` diffs, all classified: legacy truncates, contract/Rust round half-up — **fixed in Rust** |
| Encryption | 33,022 / 33,022 location pairs decrypt to equal plaintext; every ciphertext authenticates under the expected key; 66,044 nonces, 0 repeats |
| Privacy audit | **PASS** — 0 plaintext coordinate hits across outputs, quarantine, logs, fixtures (positive control on raw input: 34,925 hits); `location_enc` present for every input with location |
| Authorized vs unauthorized read | key holder recovers coordinates; no key → opaque ciphertext only; wrong key → GCM rejection |
| Fault: kill -9 ×2 + restart (300k events) | **PASS** — 282,067 committed outputs = clean baseline; 0 lost, 0 duplicate, 0 field diffs |
| Fault: rebalance (scale 1→2, SIGKILL one, add one) | **PASS** — same counts; 0 lost, 0 duplicate |
| Startup without key | Rust exits 78, legacy JVM exits 78 (sbt reports it), both before touching Kafka |
| Benchmark (1 warm-up + 3 seeded runs, one 8-vCPU VM) | backlog drain median **4,064 ev/s** (4,005–4,089); paced 2,000 ev/s: median p50 212 ms, median p99 316 ms, max lag 671 |
| k8s manifests | `kubectl apply --dry-run` (client + server) only — never deployed |

## What this does *not* claim
- **Exactly-once only inside the tested Kafka→Kafka boundary**: input offsets, normalized/quarantine outputs and
  state-changelog records are committed in one Kafka transaction; consumers must use `read_committed`. Nothing
  downstream of Kafka is covered.
- **Ordering is per VIN within a partition**, not global.
- **Performance numbers are one single-broker VM** (RF=1, everything co-located). They are a regression baseline,
  not a capacity figure.
- **Key management is a demo**: one static key from an environment variable. Not KMS-grade.
- No compliance certification of any kind.

## Production follow-ups (not done here)
1. **KMS envelope encryption** — per-key-version data keys wrapped by a KMS key; processors fetch/unwrap at startup;
   no raw key in env or k8s Secret.
2. **Key rotation** — `key_id` is already on every record; add multi-key decrypt, rotation schedule, re-encryption
   or expiry policy for old key versions.
3. **Per-consumer keys / scoped decrypt** — separate keys or a decrypt service per authorized consumer class so one
   leak does not expose every location; audit every decrypt.
4. **Topic ACLs** — only the processor principal may write `normalized`/`quarantine`/`state`; read on location-bearing
   topics limited to approved principals; no wildcard ACLs. Plus TLS/SASL.
5. **Kafka durability** — RF ≥ 3, `min.insync.replicas` ≥ 2, multi-broker benchmark on production-like hardware.
6. **Kubernetes** — the `k8s/` StatefulSet (stable `transactional.id` per pod) needs a real image pipeline, external
   secrets, autoscaling policy and a real cluster test.
7. **Cutover** — run shadow on live-like traffic for an agreed period, decide on DEFECT-1 consumer impact
   (downstream consumers currently see truncated values), then switch consumers to the Rust topics.
