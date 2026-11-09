---
name: run-demo
description: Run the 15-minute live demo of telemetry-migration-demo (fail-closed startup, shadow run and diff, encrypted reads, privacy audit) without overwriting committed evidence. Use when preparing or rehearsing the demo.
---

# Run the live demo

Use the run name `demo` throughout so committed `reports/*-seed42.json` are not modified.

## Prepare (off-screen)

```bash
cp -n .env.example .env   # fill LOCATION_ENC_KEY if empty
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
docker compose up -d kafka
(cd rust-processor && cargo build --release) && (cd legacy && sbt -batch compile)
set -a; . ./.env; set +a
```

Do one full rehearsal to warm the caches.

## Live sequence

```bash
env -u LOCATION_ENC_KEY rust-processor/target/release/telemetry-processor; echo exit=$?   # exit 78
scripts/topics.sh --reset && rm -rf legacy/target/checkpoints run/demo && mkdir -p run/demo
.venv/bin/python generator/generate.py --seed 42 --events 20000
legacy/run.sh 2>&1 | tee run/demo/legacy.log | grep -vE 'WARN|INFO' | tail -6
EXIT_WHEN_IDLE_SECS=60 METRICS_ADDR=127.0.0.1:9100 SUMMARY_FILE=run/demo/rust-summary.json \
  rust-processor/target/release/telemetry-processor 2> run/demo/rust.log
.venv/bin/python tools/compare.py --run demo                 # PASS, 0 unexplained, DEFECT-1 only
.venv/bin/python tools/decrypt_consumer.py --limit 3          # authorized
.venv/bin/python tools/decrypt_consumer.py --limit 3 --no-key # ciphertext only
.venv/bin/python tools/decrypt_consumer.py --limit 3 --wrong-key  # GCM rejects
tools/audit_privacy.sh run/demo reports/privacy-audit-demo.json   # 0 hits, positive control > 0
```

Show recovery, benchmark and k8s from committed reports (`reports/recovery/*.json`, `reports/bench.json`,
`reports/k8s-dry-run.txt`); they take too long to run live.

Do not quote latency from `rust-summary.json` in this flow: Rust starts after legacy finished, so it measures
backlog queueing. Quote the paced benchmark instead.

## Clean up

```bash
rm -rf run/demo reports/diff-demo.json reports/privacy-audit-demo.json
```
