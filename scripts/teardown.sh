#!/usr/bin/env bash
# Delete the synced tables and the Genie space, then drop every {prefix}* schema (CASCADE: tables, volumes,
# checkpoints, landing files). Never drops the catalog. Run `databricks bundle destroy` afterwards for jobs, the app
# and the Lakebase project.
# Usage: DATABRICKS_CONFIG_PROFILE=<profile> scripts/teardown.sh <schema_prefix> --yes [catalog] [genie title]
# The catalog defaults to the bundle's var.catalog for BUNDLE_TARGET (default dev) from variable-overrides.json.
set -euo pipefail
PREFIX="${1:?usage: scripts/teardown.sh <schema_prefix> --yes [catalog] [genie title]}"
[[ "${2:-}" == "--yes" ]] || { echo "refusing to drop ${PREFIX}* schemas without --yes" >&2; exit 2; }
cd "$(dirname "$0")/.."
CATALOG="${3:-$(python3 scripts/local_vars.py catalog || true)}"
[[ -n "$CATALOG" ]] || { echo "catalog not given and not set in .databricks/bundle/${BUNDLE_TARGET:-dev}/variable-overrides.json" >&2; exit 2; }
TITLE="${4:-ED Ops Crowding Assistant (${BUNDLE_TARGET:-dev})}"
python3 scripts/teardown_serving.py --prefix "$PREFIX" --catalog "$CATALOG" --genie-title "$TITLE"
for logical in bronze silver gold ref sim serving; do
  python3 scripts/run_sql.py "DROP SCHEMA IF EXISTS ${CATALOG}.${PREFIX}${logical} CASCADE"
done
