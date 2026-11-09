#!/usr/bin/env bash
# Drop every {prefix}* schema of the solution (CASCADE: tables, volumes, checkpoints, landing files).
# Never drops the catalog. Plan 3 extends this with Lakebase synced tables, the app and the Genie space.
set -euo pipefail
PREFIX="${1:?usage: scripts/teardown.sh <schema_prefix> --yes [catalog]}"
[[ "${2:-}" == "--yes" ]] || { echo "refusing to drop ${PREFIX}* schemas without --yes" >&2; exit 2; }
CATALOG="${3:-hls_jagjit_lb_playground_catalog}"
cd "$(dirname "$0")/.."
for logical in bronze silver gold ref sim serving; do
  python3 scripts/run_sql.py "DROP SCHEMA IF EXISTS ${CATALOG}.${PREFIX}${logical} CASCADE"
done
