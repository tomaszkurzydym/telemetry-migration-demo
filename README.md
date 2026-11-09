# telemetry-migration-demo

> **Synthetic demonstration — see [SYNTHETIC-DEMO.md](SYNTHETIC-DEMO.md).** No customer code or data.

Migration of a battery-health telemetry validation/normalization job from **Scala 2.12 / Spark 3.5
Structured Streaming** (`legacy/`) to a **Rust Kafka stream processor** (`rust-processor/`), with
field-level **AES-256-GCM** encryption of vehicle location. The Rust port runs in shadow mode
(separate consumer group, `*.shadow` topics) and is verified against the legacy job.

| Path | What |
|---|---|
| `docs/contract.md` | Validation / normalization / encryption / ordering contract |
| `docs/SUMMARY.md` | Executive summary + production follow-ups |
| `docs/EVIDENCE.md` | Every result, with the command and code version that produced it |
| `docker-compose.yml` | Kafka 3.7.2 single broker (KRaft) + opt-in service stubs |
| `generator/` | Seeded synthetic event generator → `telemetry.battery.raw` |
| `fixtures/` | Golden input/expected pairs, specified by hand from the contract |
| `legacy/` | Spark job (incl. intentional DEFECT-1) + characterization tests |
| `rust-processor/` | Rust processor: rdkafka, transactions, Prometheus `/metrics`, `crypto.rs` |
| `tools/` | Shadow comparator, contract verifier, privacy audit, decrypting consumer, fault injection, benchmark |
| `k8s/` | Manifests validated with `kubectl apply --dry-run` only — production follow-up |

## Quick start

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env    # then put a fresh key in LOCATION_ENC_KEY (command in the file)
docker compose up -d kafka && scripts/topics.sh --reset
.venv/bin/python generator/generate.py --seed 42 --events 20000
legacy/run.sh                                                    # legacy -> telemetry.battery.normalized
(cd rust-processor && cargo build --release)
set -a; . ./.env; set +a; EXIT_WHEN_IDLE_SECS=3 rust-processor/target/release/telemetry-processor
.venv/bin/python tools/compare.py --run seed42                    # -> reports/diff-seed42.json
```

Tests: `(cd rust-processor && cargo test)`, `(cd legacy && sbt test)`. Full evidence reproduction: `scripts/evidence.sh`.

Agents: rules and pitfalls in [AGENTS.md](AGENTS.md); reusable skills in `.agents/skills/` (`migrate-streaming-job`, `change-contract`, `regenerate-evidence`, `run-demo`).
