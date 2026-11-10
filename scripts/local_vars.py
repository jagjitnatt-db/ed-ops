#!/usr/bin/env python3
"""Environment-specific values for the dev scripts, from the same place the bundle reads them:
an environment variable (BUNDLE_VAR_<name>) first, then .databricks/bundle/<target>/variable-overrides.json
(target from BUNDLE_TARGET, default dev). Nothing environment-specific is committed.

Usage: scripts/local_vars.py <name>   # prints the value, exits 2 if it is not set
"""

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def get(name: str, target: str | None = None) -> str | None:
    if os.environ.get(f"BUNDLE_VAR_{name}"):
        return os.environ[f"BUNDLE_VAR_{name}"]
    path = ROOT / ".databricks" / "bundle" / (target or os.environ.get("BUNDLE_TARGET", "dev")) / "variable-overrides.json"
    if path.exists():
        value = json.loads(path.read_text()).get(name)
        return str(value) if value not in (None, "") else None
    return None


def require(name: str) -> str:
    value = get(name)
    if value is None:
        raise SystemExit(f"{name} is not set: export BUNDLE_VAR_{name}=... or add it to "
                         f".databricks/bundle/<target>/variable-overrides.json (see variable-overrides.example.json)")
    return value


def profile_args() -> list[str]:
    """CLI profile from DATABRICKS_CONFIG_PROFILE (the CLI also reads it itself); empty means the CLI's default."""
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    return ["-p", profile] if profile else []


if __name__ == "__main__":
    value = get(sys.argv[1]) if len(sys.argv) == 2 else None
    if value is None:
        sys.exit(2)
    print(value)
