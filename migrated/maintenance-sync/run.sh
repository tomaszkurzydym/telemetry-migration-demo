#!/usr/bin/env bash
# Runs the Rust maintenance-sync shadow processor (SYNTHETIC DEMO). Requires SERVICE_DB_PASSWORD in env or ../../.env.
set -euo pipefail
cd "$(dirname "$0")"
if [ -f ../../.env ]; then set -a; . ../../.env; set +a; fi
[ -x target/release/maintenance-sync ] || cargo build --release --quiet
exec target/release/maintenance-sync "$@"
