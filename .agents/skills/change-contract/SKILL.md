---
name: change-contract
description: Change validation, normalization, encryption or quarantine behaviour of the battery telemetry pipeline consistently across the contract, golden fixtures, Rust processor, legacy Spark job and tools. Use for any behaviour change in this repo.
argument-hint: <behaviour change>
---

# Change the processing contract

Request: $ARGUMENTS

Behaviour must stay identical in Rust and legacy (except documented `DEFECT-n`), or the shadow diff
stops being apples-to-apples. If the change alters what downstream consumers receive, confirm it with
a human before implementing.

## Order of work

1. **Contract:** edit the relevant section of `docs/contract.md` (validation §3, normalization §4,
   encryption §5, ordering §6, quarantine §7, delivery §8, logging §9). Give exact boundary values.
2. **Fixtures:** add/adjust cases in `fixtures/golden.json` by hand from the contract (boundary, just
   outside, malformed). Use `@LOC-*` tokens; never commit coordinates. Update the expected case count in
   `rust-processor/tests/golden.rs` if records were added.
3. **Rust:** `rust-processor/src/contract.rs` (validation/normalization), `crypto.rs`, `redact.rs`,
   `pipeline.rs` (state/ordering). Add unit tests for the boundaries.
4. **Legacy:** `legacy/src/main/scala/demo/legacy/LegacyNormalizer.scala`, `LegacyCrypto.scala`,
   `VinLogic.scala`; add a characterization test. Do not fix DEFECT-1.
5. **Tools:** if the wire format or schema changed, update `tools/tmdlib.py`, `tools/verify_contract.py`,
   `tools/compare.py`. AAD changes must land in Rust, Scala and Python in the same PR.

## Verify

```bash
(cd rust-processor && cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release)
(cd legacy && sbt -batch test)
scripts/evidence.sh            # diff must still be PASS with only DEFECT-1 differences
```

Then use the `regenerate-evidence` skill with `scripts/evidence.sh --full` (required for every behaviour change, per AGENTS.md).

## Pitfalls

- Decimal fields: both sides enforce the 96-bit range in contract §3; keep hostile exponents
  (`1e9999999999`) from overflowing or allocating (Rust saturating arithmetic, Scala `Try` + `Long`).
- Redaction is recursive (`location`, `lat`, `lon`, `latitude`, `longitude` at any depth) in both sides.
