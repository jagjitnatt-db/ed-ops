#!/usr/bin/env python3
"""Delete the Lakebase synced tables and the Genie space (run before dropping the schemas).
Usage: scripts/teardown_serving.py --prefix ed_ --catalog <catalog> --genie-title "<title>" [--profile fevm-playground]"""

import argparse
import json
import subprocess

TABLES = ("facility_current", "facility_snapshot_5min", "ed_board_current", "facility_narrative", "network_narrative", "facility")


def cli(profile: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["databricks", "-p", profile, *args], capture_output=True, text=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--prefix", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--genie-title", required=True)
    p.add_argument("--profile", default="fevm-playground")
    a = p.parse_args()
    for t in TABLES:
        name = f"synced_tables/{a.catalog}.{a.prefix}serving.{t}"
        r = cli(a.profile, "postgres", "delete-synced-table", name)
        print(f"{name}: {'deleted' if r.returncode == 0 else r.stderr.strip()[:120]}")
    spaces = json.loads(cli(a.profile, "api", "get", "/api/2.0/genie/spaces?page_size=100").stdout or "{}").get("spaces", [])
    for s in spaces:
        if s.get("title") == a.genie_title:
            cli(a.profile, "api", "delete", f"/api/2.0/genie/spaces/{s['space_id']}")
            print(f"deleted Genie space {s['space_id']} ({a.genie_title})")


if __name__ == "__main__":
    main()
