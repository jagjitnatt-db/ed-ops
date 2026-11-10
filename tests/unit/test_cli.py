import importlib
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_every_console_script_resolves():
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert len(scripts) == 9
    for name, target in scripts.items():
        module, attr = target.split(":")
        assert callable(getattr(importlib.import_module(module), attr)), name


def test_bundle_tasks_reference_existing_entry_points():
    scripts = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])
    jobs = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())["resources"]["jobs"]
    used = {t["python_wheel_task"]["entry_point"] for job in jobs.values() for t in job["tasks"]}
    assert used == scripts
    assert [t["task_key"] for t in jobs["ed_setup"]["tasks"]] == [
        "create_objects", "seed_reference", "backfill_generate", "backfill_ingest", "backfill_gold",
    ]
    cluster = jobs["ed_streaming"]["job_clusters"][0]["new_cluster"]
    assert cluster["spark_version"] == "18.x-aarch64-scala2.13"  # Graviton (r7gd) needs the aarch64 runtime
    assert cluster["runtime_engine"] == "PHOTON"
    assert cluster["data_security_mode"] == "USER_ISOLATION"
    assert cluster["aws_attributes"]["availability"] == "ON_DEMAND"
    assert cluster["node_type_id"] == cluster["driver_node_type_id"] == "${var.streaming_node_type}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["variables"]["streaming_node_type"]["default"] == "r7gd.2xlarge"
    params = jobs["ed_setup"]["tasks"][0]["python_wheel_task"]["named_parameters"]
    from dataclasses import fields

    from ed_ops.config import Config

    assert set(params) <= {f.name for f in fields(Config)}
