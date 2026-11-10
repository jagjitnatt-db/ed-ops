#!/usr/bin/env python3
"""Delete the Lakebase synced tables and the Genie space (run before dropping the schemas).
Usage: scripts/teardown_serving.py --prefix ed_ --catalog <catalog> --genie-title "<title>"
(CLI profile from DATABRICKS_CONFIG_PROFILE, as for every script.)"""

import argparse
import json
import subprocess

from local_vars import profile_args

TABLES = ("facility_current", "facility_snapshot_5min", "ed_board_current", "facility_narrative", "network_narrative", "facility")


def cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["databricks", *profile_args(), *args], capture_output=True, text=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--prefix", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--genie-title", required=True)
    a = p.parse_args()
    for t in TABLES:
        name = f"synced_tables/{a.catalog}.{a.prefix}serving.{t}"
        r = cli("postgres", "delete-synced-table", name)
        print(f"{name}: {'deleted' if r.returncode == 0 else r.stderr.strip()[:120]}")
    spaces = json.loads(cli("api", "get", "/api/2.0/genie/spaces?page_size=100").stdout or "{}").get("spaces", [])
    for s in spaces:
        if s.get("title") == a.genie_title:
            cli("api", "delete", f"/api/2.0/genie/spaces/{s['space_id']}")
            print(f"deleted Genie space {s['space_id']} ({a.genie_title})")


if __name__ == "__main__":
    main()
