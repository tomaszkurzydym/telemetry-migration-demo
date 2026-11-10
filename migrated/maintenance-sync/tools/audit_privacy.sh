#!/usr/bin/env bash
# Privacy audit (SYNTHETIC DEMO): no technician-note text in the shadow table, shadow quarantine or Rust logs.
# Positive control: the same pattern must match the raw input. Usage: audit_privacy.sh <run_dir> <report.json>
set -euo pipefail
dir=$1; out=$2; pat='SYN-NOTE-[0-9]{6}'
count() { [ -s "$1" ] || { echo "missing or empty: $1" >&2; echo -1; return; }; { grep -oE "$pat" "$1" || true; } | wc -l; }
raw=$(count "$dir/raw.jsonl"); db=$(count "$dir/rust-db.jsonl"); q=$(count "$dir/rust-q.jsonl"); log=$(count "$dir/rust.log")
redacted=$({ grep -o 'REDACTED' "$dir/rust-q.jsonl" || true; } | wc -l)
verdict=PASS
[[ "$raw" -gt 0 && "$db" == 0 && "$q" == 0 && "$log" == 0 ]] || verdict=FAIL
printf '{"verdict":"%s","pattern":"%s","positive_control_raw_hits":%s,"rust_db_hits":%s,"rust_quarantine_hits":%s,"rust_log_hits":%s,"redacted_markers_in_quarantine":%s}\n' \
  "$verdict" "$pat" "$raw" "$db" "$q" "$log" "$redacted" | tee "$out"
[[ $verdict == PASS ]]
