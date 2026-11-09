---
name: regenerate-evidence
description: Re-run the full evidence pack (tests, shadow diff, privacy audit, reads, recovery, benchmark, k8s dry-run) for telemetry-migration-demo and update EVIDENCE.md, SUMMARY.md and the PR description. Use after any code change before opening or updating a PR.
---

# Regenerate the evidence pack

## Preconditions

- All code changes committed (reports record `git rev-parse --short HEAD`; uncommitted code makes them `-dirty`).
- `.env` with a fresh synthetic `LOCATION_ENC_KEY`; `.venv` installed; `docker compose up -d kafka` healthy.
- Nothing else using the broker (no demo run, no other fault test or benchmark).

## Run

```bash
set -o pipefail; scripts/evidence.sh --full 2>&1 | tee /tmp/evidence.log; echo "exit=$?"   # ~25 min; must print exit=0
```

Check, and stop to investigate if any fails:
- `reports/tests/*.txt`: all Rust and Scala tests pass.
- `reports/captures/startup-without-key.txt`: both exit before Kafka (Rust exit 78).
- `reports/diff-seed42.json`: `verdict: PASS`, `unexplained_total: 0`, only `DEFECT-1` differences, 0 repeated nonces.
- `reports/verify-{rust,legacy}-seed42.json`: `"ok": true`, empty `violations`.
- `reports/privacy-audit-seed42.json`: `plaintext_hits_total: 0` **and** positive control > 0.
- `reports/recovery/{kill9,rebalance}.json`: PASS, 0 lost / extra / duplicate.
- `reports/bench.json`: 3 measured runs + warm-up, environment recorded.
- `reports/k8s-dry-run.txt`: client + server dry-run OK, no resources remain.

## Publish

1. Update every number and the code version in `docs/EVIDENCE.md` and `docs/SUMMARY.md` from the reports
   (never from memory). Explain any `-dirty` tag (reports rewritten late in the run; only `reports/` changed).
2. Commit as `Evidence: regenerate all reports at <sha>`; `run/` and `reports/**/*.jsonl` stay ignored.
3. Update the PR description's evidence table to match.
