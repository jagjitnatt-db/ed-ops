#!/usr/bin/env bash
# Build the React frontend, then deploy the bundle (the dist/ folder is git-ignored but synced via sync.include).
# Usage: scripts/deploy.sh -t dev -p fevm-playground [--var name=value ...]
set -euo pipefail
cd "$(dirname "$0")/.."
npm --prefix app/frontend ci
npm --prefix app/frontend run build
databricks bundle deploy "$@"
