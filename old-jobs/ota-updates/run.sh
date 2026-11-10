#!/usr/bin/env bash
# Runs the legacy ota-updates Spark job locally (SYNTHETIC DEMO). Requires SERVICE_DB_PASSWORD in env or ../../.env.
set -euo pipefail
cd "$(dirname "$0")"
if [ -f ../../.env ]; then set -a; . ../../.env; set +a; fi
SBT="${SBT:-$(command -v sbt || echo "$HOME/tools/sbt/bin/sbt")}"
exec "$SBT" -batch "runMain demo.oldjobs.ota.OtaUpdatesJob"
