#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/test-env.sh
exec .venv/bin/python -m pytest "$@"
