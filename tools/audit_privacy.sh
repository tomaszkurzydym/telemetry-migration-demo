#!/usr/bin/env bash
# Privacy audit (SYNTHETIC DEMO): scan output/quarantine dumps, logs and fixtures for plaintext
# coordinates; confirm location_enc is present for every normalized record whose input had a location.
# Usage: tools/audit_privacy.sh <run-dir> [report.json]   (run-dir holds raw/legacy/rust/*_q .jsonl + *.log)
# Exit 0 only when there are zero hits and zero missing location_enc. Matches are counted, never printed.
set -euo pipefail
RUN="${1:?usage: audit_privacy.sh <run-dir> [report.json]}"
REPORT="${2:-$RUN/privacy-audit.json}"
cd "$(dirname "$0")/.."

# Plaintext coordinate patterns: lat/lon/latitude/longitude keys with a numeric value (raw or escaped JSON),
# and a "location" key holding an object.
PATTERNS=(
  '\\?"(lat|lon|latitude|longitude)\\?"[[:space:]]*:[[:space:]]*-?[0-9]'
  '\\?"location\\?"[[:space:]]*:[[:space:]]*\{'
)
targets=()
for f in "$RUN"/legacy.jsonl "$RUN"/rust.jsonl "$RUN"/legacy_q.jsonl "$RUN"/rust_q.jsonl "$RUN"/*.log $(find fixtures -type f | sort); do
  [ -e "$f" ] && targets+=("$f")
done
[ -d logs ] && while IFS= read -r f; do targets+=("$f"); done < <(find logs -type f)

missing_dumps=()
for n in raw legacy rust; do [ -s "$RUN/$n.jsonl" ] || missing_dumps+=("\"$n\""); done
missing_json=$(IFS=,; echo "${missing_dumps[*]}")

total=0
files_json=""
for f in "${targets[@]}"; do
  n=0
  for p in "${PATTERNS[@]}"; do
    c=$(grep -Ec -- "$p" "$f" || true)
    n=$((n + c))
  done
  total=$((total + n))
  files_json+="{\"file\":\"$f\",\"lines_scanned\":$(wc -l < "$f"),\"hits\":$n},"
done

# Positive control: the raw input dump MUST contain coordinates (proves the patterns actually match).
control=0
[ -e "$RUN/raw.jsonl" ] && control=$(grep -Ec -- "${PATTERNS[0]}" "$RUN/raw.jsonl" || true)

presence=$(.venv/bin/python - "$RUN" <<'PY'
import json, sys
run = sys.argv[1]
def rows(n):
    try:
        return [json.loads(l) for l in open(f"{run}/{n}.jsonl") if l.strip()]
    except FileNotFoundError:
        return []
had = {}
for r in rows("raw"):
    try:
        v = json.loads(r["value"])
    except Exception:
        continue
    if isinstance(v, dict) and isinstance(v.get("event_id"), str):
        had.setdefault((r["key"], v["event_id"]), v.get("location") is not None)
res = {}
for side in ("legacy", "rust"):
    out = rows(side)
    if not out:
        continue
    miss = unexpected = with_loc = 0
    for r in out:
        v = json.loads(r["value"])
        h = had.get((r["key"], v.get("event_id")))
        with_loc += h is True
        if h and "location_enc" not in v: miss += 1
        if h is False and "location_enc" in v: unexpected += 1
    res[side] = {"records": len(out), "input_had_location": with_loc, "missing_location_enc": miss, "unexpected_location_enc": unexpected}
print(json.dumps(res))
PY
)
missing=$(echo "$presence" | .venv/bin/python -c 'import json,sys; d=json.load(sys.stdin); print(sum(v["missing_location_enc"]+v["unexpected_location_enc"] for v in d.values()))')
verdict=PASS
{ [ "$total" -ne 0 ] || [ "$missing" -ne 0 ] || [ "$control" -eq 0 ] || [ ${#missing_dumps[@]} -ne 0 ]; } && verdict=FAIL
cat > "$REPORT" <<JSON
{
  "run_dir": "$RUN",
  "code_version": "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)$(git diff --quiet HEAD 2>/dev/null || echo -dirty)",
  "patterns": ["(lat|lon|latitude|longitude) key with numeric value", "location key with object value"],
  "files": [${files_json%,}],
  "plaintext_hits_total": $total,
  "positive_control_raw_input_hits": $control,
  "location_enc_presence": $presence,
  "missing_or_empty_dumps": [$missing_json],
  "verdict": "$verdict"
}
JSON
cat "$REPORT"
[ "$verdict" = PASS ]
