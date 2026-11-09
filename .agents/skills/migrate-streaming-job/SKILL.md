---
name: migrate-streaming-job
description: Migrate a stateful streaming job (e.g. Spark Structured Streaming) to a new Kafka stream processor with shadow verification and an evidence pack, following the method proven in this repo. Use when asked to port, rewrite or replatform a streaming/Kafka job and prove behavioural parity.
argument-hint: <legacy job location and target stack>
---

# Migrate a streaming job with proof

This repo is the reference implementation. The deliverable is not just the new code: it is a PR whose
**evidence pack** lets a reviewer approve the migration without reading every line.

Request: $ARGUMENTS

## 0. Gather inputs (ask for anything missing, state assumptions in the PR)

1. Legacy job: source location, input/output topics, consumer group, keying, state, how it is run locally.
2. Target stack and pinned versions (here: Rust stable + `rdkafka`, Kafka 3.7.x).
3. Sensitive fields and how they must be protected (here: `location` → AES-256-GCM `location_enc`).
4. Delivery guarantee required and the boundary it must hold for (here: Kafka-to-Kafka transactions).
5. Data allowed for testing. Default: **synthetic only**, seeded generator.

## 1. Characterize the legacy job before changing anything

- Write characterization tests that pin current behaviour, **including bugs**. Label each bug `DEFECT-n` and
  ask a human whether it is a bug to fix or a requirement to keep.
- Reference: `legacy/src/test/scala/demo/legacy/LegacyCharacterizationSpec.scala` (DEFECT-1).

## 2. Write the contract and independent fixtures

- `docs/contract.md`: topics, input schema, validation and error codes, normalization, encryption wire format,
  ordering/dedupe/out-of-order scope, quarantine format, delivery semantics, logging. Make each rule testable.
- `fixtures/golden.json`: expected outputs **specified by hand from the contract**, never captured from either
  implementation. No sensitive plaintext: use tokens (`@LOC-A`) substituted at test time.
- Run the golden fixtures against the legacy job too (`GoldenFixtureSpec.scala`) with per-field overrides only
  for documented defects.

## 3. Build the new processor in shadow

- Separate consumer group and `*.shadow` output/quarantine topics; never write to the legacy topics.
- Fail closed on missing/invalid secrets before consuming. Log only identifiers + error codes.
- Make the guarantee real inside the stated boundary (here: transactional producer, offsets sent in the
  transaction, state topic restored on assignment, rebalance callbacks). Expose Prometheus metrics.
- Reference: `rust-processor/src/{main,pipeline,contract,crypto,metrics,redact}.rs`, `rust-processor/tests/golden.rs`.

## 4. Build the verification harness (each check must be able to fail)

| Check | Reference | Must fail when |
|---|---|---|
| Shadow diff joined on business key | `tools/compare.py` | missing/extra/field/ordering diff not explained by a `DEFECT-n`; empty input |
| Schema/contract verifier | `tools/verify_contract.py` | invalid schema, plaintext sensitive keys, bad ciphertext |
| Privacy audit with positive control | `tools/audit_privacy.sh` | any plaintext hit; missing/empty dumps; positive control finds 0 |
| Authorized / unauthorized / wrong-key reads | `tools/decrypt_consumer.py` | wrong key decrypts |
| Fault injection vs clean baseline | `tools/inject_fault.sh`, `tools/check_recovery.py` | any lost, extra or duplicate committed output |
| Benchmark, warm-up + 3 seeded runs | `tools/bench.py` | (report only: environment-labelled, no capacity claims) |
| Deployment manifests | `k8s/`, `scripts/k8s_dry_run.sh` | dry-run rejects; resources remain afterwards |

Comparing encrypted fields: decrypt both sides and compare plaintext; random nonces make ciphertexts differ.

## 5. Evidence pack and PR

- One script reproduces everything (`scripts/evidence.sh --full`), stamping the code version into reports.
- `docs/EVIDENCE.md`: every number with the command and commit that produced it. `docs/SUMMARY.md`: results,
  what is **not** claimed, production follow-ups (KMS envelope encryption, key rotation, per-consumer keys, ACLs).
- Commit code, run evidence, commit reports separately. Open the PR with the evidence table in the description.

## Escalate to a human (do not decide alone)

- Any contract change or legacy behaviour classified as bug vs requirement.
- Any claim broader than what a check proves.
- Access: repo creation, integration permissions, real data or real keys.
