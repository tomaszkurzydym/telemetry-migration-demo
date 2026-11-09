#!/usr/bin/env python3
"""Local benchmark for the Rust shadow processor (SYNTHETIC DEMO, single-broker laptop/VM setup).

One warm-up run (discarded) + 3 seeded measured runs. Each run has two phases on fresh topics:
  A. backlog drain: input produced first, then the processor drains it -> throughput, max lag.
  B. paced: processor running, generator paced at --rate ev/s -> end-to-end latency, lag under load.
Writes reports/bench.json and prints a Markdown table. Numbers describe THIS environment only.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "rust-processor/target/release/telemetry-processor"
PY = str(ROOT / ".venv/bin/python")
LAG_RE = re.compile(r"^telemetry_consumer_lag\{[^}]*\} (\d+)", re.M)


def sh(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL, cwd=ROOT).strip()
    except Exception:
        return "unknown"


def env_details() -> dict:
    return {
        "code_version": sh("git rev-parse --short HEAD") + ("-dirty" if sh("git status --porcelain --untracked-files=no") else ""),
        "cpu_model": sh("lscpu | sed -n 's/^Model name:[[:space:]]*//p'"),
        "nproc": os.cpu_count(),
        "mem_total": sh("free -h | awk '/^Mem:/{print $2}'"),
        "kernel": platform.release(),
        "os": sh(". /etc/os-release && echo $PRETTY_NAME"),
        "docker": sh("docker version --format '{{.Server.Version}}'"),
        "kafka_image": sh("docker inspect tmd-kafka --format '{{.Config.Image}}'"),
        "kafka_topology": "1 broker (KRaft combined), RF=1, 6 partitions, same host as processor and generator",
        "rustc": sh("cd rust-processor && rustc --version"),
        "python": platform.python_version(),
        "processor_config": {"BATCH_MAX": os.environ.get("BATCH_MAX", "500"), "BATCH_MAX_MS": os.environ.get("BATCH_MAX_MS", "100"),
                             "instances": 1, "transactions": "one Kafka transaction per batch"},
    }


def sample_lag(stop: threading.Event, out: list[int]) -> None:
    while not stop.is_set():
        try:
            body = urllib.request.urlopen("http://127.0.0.1:9100/metrics", timeout=1).read().decode()
            vals = [int(v) for v in LAG_RE.findall(body)]
            if vals:
                out.append(sum(vals))
        except Exception:
            pass
        time.sleep(0.25)


def run_processor(summary: Path, log: Path, idle: int) -> subprocess.Popen:
    env = dict(os.environ, EXIT_WHEN_IDLE_SECS=str(idle), SUMMARY_FILE=str(summary), METRICS_ADDR="127.0.0.1:9100", RUST_LOG="warn")
    return subprocess.Popen([str(BIN)], env=env, stderr=log.open("w"), cwd=ROOT)


def phase(name: str, seed: int, events: int, rate: float, tag: str, out_dir: Path) -> dict:
    subprocess.run(["scripts/topics.sh", "--reset"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    gen = [PY, "generator/generate.py", "--seed", str(seed), "--events", str(events), "--rate", str(rate)]
    summary, log = out_dir / f"{tag}-{name}-summary.json", out_dir / f"{tag}-{name}.log"
    lags: list[int] = []
    stop = threading.Event()
    if name == "backlog":
        subprocess.run(gen, cwd=ROOT, check=True, capture_output=True)
        p = run_processor(summary, log, idle=3)
    else:
        p = run_processor(summary, log, idle=5)
        time.sleep(2)
    t = threading.Thread(target=sample_lag, args=(stop, lags), daemon=True)
    t.start()
    if name == "paced":
        subprocess.run(gen, cwd=ROOT, check=True, capture_output=True)
    p.wait(timeout=900)
    stop.set()
    s = json.loads(summary.read_text())
    return {"phase": name, "seed": seed, "events": events, "rate": rate or None, "exit_code": p.returncode,
            "consumed": s["totals"].get("consumed"), "txn_committed": s["totals"].get("txn_committed"),
            "throughput_events_per_s": round(s["throughput_events_per_s"], 1), "e2e_latency_ms": s["e2e_latency_ms"],
            "max_consumer_lag": max(lags) if lags else None, "lag_samples": len(lags)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--paced-events", type=int, default=60_000)
    ap.add_argument("--rate", type=float, default=2000)
    ap.add_argument("--seeds", default="1001,1002,1003")
    ap.add_argument("--warmup-seed", type=int, default=1000)
    a = ap.parse_args()
    out_dir = ROOT / "reports/bench"
    out_dir.mkdir(parents=True, exist_ok=True)
    for line in (ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)
    result = {"environment": env_details(), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "method": __doc__.strip(), "warmup": [], "runs": []}
    print("warm-up (discarded)...", flush=True)
    result["warmup"].append(phase("backlog", a.warmup_seed, a.events // 4, 0, "warmup", out_dir))
    for i, seed in enumerate(int(s) for s in a.seeds.split(",")):
        for name, ev, rate in (("backlog", a.events, 0), ("paced", a.paced_events, a.rate)):
            r = phase(name, seed, ev, rate, f"run{i + 1}", out_dir)
            print(json.dumps(r), flush=True)
            result["runs"].append(r)
    agg = {}
    for name in ("backlog", "paced"):
        rs = [r for r in result["runs"] if r["phase"] == name]
        tp = [r["throughput_events_per_s"] for r in rs]
        agg[name] = {"throughput_events_per_s": {"min": min(tp), "median": statistics.median(tp), "max": max(tp)},
                     "p50_ms_median": statistics.median(r["e2e_latency_ms"]["p50"] for r in rs),
                     "p99_ms_median": statistics.median(r["e2e_latency_ms"]["p99"] for r in rs),
                     "max_lag_max": max((r["max_consumer_lag"] or 0) for r in rs)}
    result["aggregate"] = agg
    (ROOT / "reports/bench.json").write_text(json.dumps(result, indent=2) + "\n")
    print("\n| run | phase | seed | events | throughput ev/s | p50 ms | p95 ms | p99 ms | max lag |")
    print("|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(result["runs"]):
        l = r["e2e_latency_ms"]
        print(f"| {i // 2 + 1} | {r['phase']} | {r['seed']} | {r['events']} | {r['throughput_events_per_s']} | {l['p50']} | {l['p95']} | {l['p99']} | {r['max_consumer_lag']} |")


if __name__ == "__main__":
    main()
