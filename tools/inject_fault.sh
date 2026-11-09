#!/usr/bin/env bash
# Fault injection for the Rust shadow processor (SYNTHETIC DEMO).
#   tools/inject_fault.sh kill9      # SIGKILL mid-stream twice, restart, drain
#   tools/inject_fault.sh rebalance  # scale 1->2 instances, SIGKILL one (session-timeout rebalance), add a third
#   tools/inject_fault.sh all
# For each scenario: seeded input -> clean baseline run -> reset shadow path -> faulted run over the same
# input -> tools/check_recovery.py (no lost outputs, no duplicate committed outputs, identical content).
# Boundary tested: Kafka input topic -> Kafka output/quarantine/state topics, read_committed consumers.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
SCEN="${1:-all}"
EVENTS="${EVENTS:-300000}"
SEED="${SEED:-7}"
BIN=rust-processor/target/release/telemetry-processor
OUT=reports/recovery
PY=.venv/bin/python
mkdir -p "$OUT"
RUNLOG="$OUT/recovery.log"
log() { echo "[$(date -u +%H:%M:%S.%3N)] $*" | tee -a "$RUNLOG"; }

start() { # start <instance> <metrics-port> <logfile>
  PROCESSOR_INSTANCE_ID="$1" METRICS_ADDR="127.0.0.1:$2" EXIT_WHEN_IDLE_SECS=5 RUST_LOG=info \
    "$BIN" 2>>"$3" &
  PID=$!
}
consumed() { curl -s "127.0.0.1:$1/metrics" | awk '/^telemetry_events_consumed_total/{print $2}' || echo 0; }
wait_consumed() { # wait until instance on port has committed >= N records (bounded)
  for _ in $(seq 1 600); do c=$(consumed "$1"); [ "${c:-0}" -ge "$2" ] 2>/dev/null && return 0; sleep 0.05; done
}
dump() { # dump <dir> [uncommitted]
  mkdir -p "$1"
  $PY tools/dump_topic.py telemetry.battery.normalized.shadow "$1/rust.jsonl" | tee -a "$RUNLOG"
  $PY tools/dump_topic.py telemetry.battery.quarantine.shadow "$1/rust_q.jsonl" | tee -a "$RUNLOG"
  [ "${2:-}" = uncommitted ] && $PY tools/dump_topic.py telemetry.battery.normalized.shadow "$1/rust_uncommitted.jsonl" --uncommitted | tee -a "$RUNLOG"
  return 0
}
prepare() { # prepare <scenario>: fresh topics + seeded input + clean baseline
  log "== $1: reset topics, generate seed=$SEED events=$EVENTS"
  scripts/topics.sh --reset >/dev/null
  $PY generator/generate.py --seed "$SEED" --events "$EVENTS" 2>/dev/null | tee -a "$RUNLOG"
  log "$1: clean baseline run"
  start 0 9100 "$OUT/$1-baseline.log"; pid=$PID; wait "$pid"; log "baseline exit=$?"
  dump "$OUT/$1/baseline"
  scripts/topics.sh --reset-shadow >/dev/null
  log "$1: shadow path reset (output, quarantine, state topics + rust-shadow-cg offsets)"
}

kill9() {
  prepare kill9
  L="$OUT/kill9-faulted.log"; : > "$L"
  start 0 9100 "$L"; pid=$PID; log "kill9: started instance 0 pid=$pid"
  wait_consumed 9100 $((EVENTS / 4)); log "kill9: committed=$(consumed 9100); SIGKILL pid=$pid"; kill -9 "$pid"; wait "$pid" 2>/dev/null || true
  start 0 9100 "$L"; pid=$PID; log "kill9: restarted instance 0 (same transactional.id) pid=$pid"
  wait_consumed 9100 $((EVENTS / 4)); log "kill9: committed since restart=$(consumed 9100); SIGKILL pid=$pid"; kill -9 "$pid"; wait "$pid" 2>/dev/null || true
  start 0 9100 "$L"; pid=$PID; log "kill9: restarted instance 0 pid=$pid; draining"
  wait "$pid"; log "kill9: drained exit=$?"
  dump "$OUT/kill9/faulted" uncommitted
  $PY tools/check_recovery.py "$OUT/kill9/baseline" "$OUT/kill9/faulted" --scenario kill9 --report "$OUT/kill9.json" | tee -a "$RUNLOG"
}

rebalance() {
  prepare rebalance
  L="$OUT/rebalance-faulted.log"; : > "$L"
  start 0 9100 "$L"; p0=$PID; log "rebalance: started instance 0 pid=$p0"
  wait_consumed 9100 $((EVENTS / 6))
  start 1 9101 "$L"; p1=$PID; log "rebalance: scale-up, started instance 1 pid=$p1"
  wait_consumed 9101 $((EVENTS / 10)); log "rebalance: instance 1 committed=$(consumed 9101); SIGKILL pid=$p1 (partitions return after session timeout)"
  kill -9 "$p1"; wait "$p1" 2>/dev/null || true
  sleep 2
  start 2 9102 "$L"; p2=$PID; log "rebalance: started instance 2 pid=$p2"
  wait "$p0" "$p2" || true; log "rebalance: all instances drained"
  dump "$OUT/rebalance/faulted" uncommitted
  $PY tools/check_recovery.py "$OUT/rebalance/baseline" "$OUT/rebalance/faulted" --scenario rebalance --report "$OUT/rebalance.json" | tee -a "$RUNLOG"
}

: > "$RUNLOG"
log "code_version=$(git rev-parse --short HEAD 2>/dev/null || echo uncommitted)$(git diff --quiet HEAD 2>/dev/null || echo -dirty) events=$EVENTS seed=$SEED"
case "$SCEN" in
  kill9) kill9 ;;
  rebalance) rebalance ;;
  all) kill9; rebalance ;;
  *) echo "usage: $0 kill9|rebalance|all" >&2; exit 2 ;;
esac
grep -h '"verdict"' "$OUT"/*.json >/dev/null && ! grep -l '"verdict": "FAIL"' "$OUT"/kill9.json "$OUT"/rebalance.json 2>/dev/null
