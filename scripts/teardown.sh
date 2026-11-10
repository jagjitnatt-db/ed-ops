#!/usr/bin/env bash
# Delete the synced tables and the Genie space, then drop every {prefix}* schema (CASCADE: tables, volumes,
# checkpoints, landing files). Never drops the catalog. Run `databricks bundle destroy` afterwards for jobs, the app
# and the Lakebase project.
set -euo pipefail
PREFIX="${1:?usage: scripts/teardown.sh <schema_prefix> --yes [catalog] [genie title]}"
[[ "${2:-}" == "--yes" ]] || { echo "refusing to drop ${PREFIX}* schemas without --yes" >&2; exit 2; }
CATALOG="${3:-hls_jagjit_lb_playground_catalog}"
TITLE="${4:-ED Ops Crowding Assistant (dev)}"
cd "$(dirname "$0")/.."
python3 scripts/teardown_serving.py --prefix "$PREFIX" --catalog "$CATALOG" --genie-title "$TITLE"
for logical in bronze silver gold ref sim serving; do
  python3 scripts/run_sql.py "DROP SCHEMA IF EXISTS ${CATALOG}.${PREFIX}${logical} CASCADE"
done
