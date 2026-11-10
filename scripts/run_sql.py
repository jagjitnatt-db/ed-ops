#!/usr/bin/env python3
"""Run SQL on a Databricks SQL warehouse through the Databricks CLI (dev tooling for smoke checks).

Usage: scripts/run_sql.py "SELECT 1" ["SELECT 2" ...]
Config: warehouse from BUNDLE_VAR_warehouse_id or .databricks/bundle/<target>/variable-overrides.json;
        CLI profile from DATABRICKS_CONFIG_PROFILE (see scripts/local_vars.py).
"""

import json
import subprocess
import sys
import time

from local_vars import profile_args, require


def _api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", *profile_args(), "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    return json.loads(subprocess.check_output(cmd))


def run(statement: str) -> list[list]:
    r = _api("post", "/api/2.0/sql/statements",
             {"warehouse_id": require("warehouse_id"), "statement": statement, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = _api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"FAILED: {statement}\n{r['status'].get('error', {}).get('message', '')}")
    return r.get("result", {}).get("data_array", []) or []


if __name__ == "__main__":
    for stmt in sys.argv[1:]:
        print(f">> {stmt}")
        for row in run(stmt):
            print("  ", row)
