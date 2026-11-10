#!/usr/bin/env bash
# Build the React frontend, then deploy the bundle (the dist/ folder is git-ignored but synced via sync.include).
# Usage: scripts/deploy.sh -t <target> -p <profile> [--var name=value ...]
# catalog and warehouse_id come from .databricks/bundle/<target>/variable-overrides.json (or --var).
set -euo pipefail
cd "$(dirname "$0")/.."
npm --prefix app/frontend ci
npm --prefix app/frontend run build
databricks bundle deploy "$@"
