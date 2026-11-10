import importlib
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_every_console_script_resolves():
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert len(scripts) == 22
    for name, target in scripts.items():
        module, attr = target.split(":")
        assert callable(getattr(importlib.import_module(module), attr)), name


def _jobs():
    jobs = {}
    for name in ("jobs.yml", "validate.yml"):
        jobs.update(yaml.safe_load((ROOT / "resources" / name).read_text())["resources"]["jobs"])
    return jobs


def test_bundle_tasks_reference_existing_entry_points():
    from dataclasses import fields

    from ed_ops.config import Config

    scripts = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])
    jobs = _jobs()
    used = {t["python_wheel_task"]["entry_point"] for job in jobs.values() for t in job["tasks"]}
    assert used == scripts
    assert [t["task_key"] for t in jobs["ed_setup"]["tasks"]] == [
        "create_objects", "seed_reference", "backfill_generate", "backfill_ingest", "backfill_gold",
        "backfill_notes_ai", "refresh_drivers", "backfill_narratives", "governance_checks", "serving_setup", "genie_space",
    ]
    assert {t["task_key"] for t in jobs["ed_streaming"]["tasks"]} == {
        "generator", "bronze_ingest", "silver_visits", "gold_metrics", "notes_ai", "ai_retry", "narratives",
    }
    cluster = jobs["ed_streaming"]["job_clusters"][0]["new_cluster"]
    assert cluster["spark_version"] == "18.x-scala2.13"  # Databricks picks the ARM build for Graviton nodes itself
    assert cluster["runtime_engine"] == "PHOTON"
    assert cluster["data_security_mode"] == "USER_ISOLATION"
    assert cluster["aws_attributes"]["availability"] == "ON_DEMAND"
    assert cluster["node_type_id"] == cluster["driver_node_type_id"] == "${var.streaming_node_type}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["variables"]["streaming_node_type"]["default"] == "r7gd.2xlarge"
    names = {f.name for f in fields(Config)}
    for job in ("ed_setup", "ed_validate"):
        assert set(jobs[job]["tasks"][0]["python_wheel_task"]["named_parameters"]) <= names
    for task in jobs["ed_setup"]["tasks"]:
        if task["task_key"] not in ("create_objects", "seed_reference", "governance_checks", "serving_setup", "genie_space"):
            assert task["python_wheel_task"]["named_parameters"]["streaming_job_id"] == "${resources.jobs.ed_streaming.id}"
    gold = next(t for t in jobs["ed_setup"]["tasks"] if t["task_key"] == "backfill_gold")
    assert {"task_key": "backfill_notes_ai"} in gold["depends_on"]  # under-triage flags must exist before history
    validate = jobs["ed_validate"]
    params = validate["tasks"][0]["python_wheel_task"]["named_parameters"]
    assert params["schema_prefix"] == "ed_validate_" and params["facility_count"] == "5"
    assert next(t for t in validate["tasks"] if t["task_key"] == "teardown")["run_if"] == "ALL_DONE"


def test_serving_resources_and_customer_target():
    jobs = _jobs()
    setup_keys = [t["task_key"] for t in jobs["ed_setup"]["tasks"]]
    assert setup_keys[-2:] == ["serving_setup", "genie_space"]
    assert [t["task_key"] for t in jobs["ed_genie_benchmarks"]["tasks"]] == ["genie_benchmarks"]
    common = jobs["ed_setup"]["tasks"][0]["python_wheel_task"]["named_parameters"]
    assert common["app_name"] == "${resources.apps.ed_ops.name}" and common["warehouse_id"] == "${var.warehouse_id}"
    app = yaml.safe_load((ROOT / "resources" / "app.yml").read_text())["resources"]["apps"]["ed_ops"]
    assert app["source_code_path"] == "../app" and set(app["user_api_scopes"]) == {"dashboards.genie", "sql"}
    kinds = {next(k for k in r if k not in ("name", "description")) for r in app["resources"]}
    assert kinds == {"postgres", "sql_warehouse"}
    env = {e["name"]: e["value"] for e in app["config"]["env"]}
    assert {"SERVING_SCHEMA", "LAKEBASE_ENDPOINT", "GENIE_SPACE_TITLE", "LOCAL_TZ"} <= set(env)
    project = yaml.safe_load((ROOT / "resources" / "lakebase.yml").read_text())["resources"]["postgres_projects"]["ed_ops_lakebase"]
    assert project["project_id"] == "${var.lakebase_project_id}"
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "app/frontend/dist/**" in bundle["sync"]["include"]
    customer = bundle["targets"]["customer"]
    assert customer["mode"] == "production" and customer["run_as"] == {"service_principal_name": "${var.run_as_service_principal}"}
    assert bundle["targets"]["dev"]["variables"]["warehouse_id"] == "abef38163a09f120"
    assert (ROOT / "scripts" / "deploy.sh").stat().st_mode & 0o111
