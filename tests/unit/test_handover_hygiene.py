"""Guards for the customer handover: nothing in the repo may depend on Databricks-internal package mirrors."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_frontend_lockfile_uses_the_public_npm_registry():
    lock = json.loads((ROOT / "app" / "frontend" / "package-lock.json").read_text())
    resolved = {name: p["resolved"] for name, p in lock["packages"].items() if p.get("resolved")}
    assert resolved, "lockfile has no resolved packages"
    private = {name: url for name, url in resolved.items() if not url.startswith("https://registry.npmjs.org/")}
    assert not private, f"{len(private)} packages resolve outside registry.npmjs.org, e.g. {next(iter(private.values()))}"
