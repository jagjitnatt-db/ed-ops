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


def _tracked_text_files():
    import subprocess

    names = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    for name in names:
        path = ROOT / name
        try:
            yield name, path.read_text()
        except (UnicodeDecodeError, FileNotFoundError):
            continue


def test_no_workspace_or_app_hostnames_are_committed():
    import re

    host = re.compile(r"[a-z0-9-]+\.(cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com|databricksapps\.com)")
    hits = [f"{name}: {m.group(0)}" for name, text in _tracked_text_files() for m in host.finditer(text)]
    assert not hits, hits[:5]


def test_environment_values_are_bundle_variables_without_defaults():
    import yaml

    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for name in ("catalog", "warehouse_id"):
        assert "default" not in bundle["variables"][name], f"{name} must be supplied per environment"
    for target, spec in bundle["targets"].items():
        assert "host" not in (spec.get("workspace") or {}), f"target {target} pins a workspace host"
        assert not ({"catalog", "warehouse_id"} & set(spec.get("variables") or {})), f"target {target} pins environment values"
    example = json.loads((ROOT / "variable-overrides.example.json").read_text())
    assert {"catalog", "warehouse_id"} <= set(example)


def test_scripts_do_not_hardcode_profiles_or_warehouses():
    import re

    warehouse = re.compile(r"\b[0-9a-f]{16}\b")
    profile_default = re.compile(r"""--profile["'], default=|environ\.get\(["']DATABRICKS_[A-Z_]*PROFILE["'], ["']""")
    for name, text in _tracked_text_files():
        if name.startswith(("scripts/", "src/", "resources/", "app/backend/")) or name == "databricks.yml":
            assert not warehouse.search(text), f"{name} contains a warehouse-like id"
            assert not profile_default.search(text), f"{name} hardcodes a CLI profile"
