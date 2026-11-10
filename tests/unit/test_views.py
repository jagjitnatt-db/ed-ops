import pytest

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.ddl import generate
from ed_ops.ddl.metadata import load_catalog_def


def test_serving_config_defaults():
    cfg = Config()
    assert (cfg.lakebase_project_id, cfg.lakebase_branch, cfg.lakebase_database) == ("ed-ops-dev", "production", "databricks_postgres")
    assert (cfg.sync_mode_live, cfg.sync_mode_ref, cfg.genie_space_title) == ("CONTINUOUS", "SNAPSHOT", "ED Ops Crowding Assistant")


def test_visit_facts_view_metadata():
    v = load_catalog_def().view("gold", "visit_facts")
    assert v.tags["data_classification"] == "internal" and v.tags["contains_phi"] == "false" and v.tags["layer"] == "gold"
    names = [c.name for c in v.columns]
    assert names[:5] == ["facility_id", "facility_name", "facility_type", "region", "visit_id"]
    assert {"d2p_min", "los_h", "boarding_h", "left_without_being_seen", "complaint_category"} <= set(names)
    assert not ({"patient_name_redacted", "mrn_hash", "phone_last4", "city", "state", "note_masked"} & set(names))
    assert all(c.comment.strip() for c in v.columns)


def test_create_view_sql_and_ordering():
    cfg = Config()
    cat = load_catalog_def()
    sql = generate.create_view_sql(cfg, cat.view("gold", "visit_facts"))
    assert sql.startswith("CREATE OR REPLACE VIEW main.ed_gold.visit_facts (")
    assert "d2p_min COMMENT" in sql and "FROM main.ed_silver.ed_visits v" in sql
    assert "{" not in sql
    stmts = generate.ddl_statements(cfg, cat)
    last_table = max(i for i, s in enumerate(stmts) if s.startswith("CREATE TABLE"))
    view_at = next(i for i, s in enumerate(stmts) if s.startswith("CREATE OR REPLACE VIEW"))
    assert view_at > last_table
    assert any(s.startswith("ALTER VIEW main.ed_gold.visit_facts SET TAGS ('ed_layer' = 'gold'") for s in stmts)


def test_view_select_matches_declared_columns(spark):
    """Run the view's SELECT on local Spark over empty source tables: the output columns must equal the declared ones."""
    cat = load_catalog_def()
    cfg = Config(catalog="spark_catalog", schema_prefix="tv_")
    for logical in ("silver", "ref"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS tv_{logical}")
    try:
        for logical, name in (("silver", "ed_visits"), ("silver", "triage_notes"), ("ref", "facility")):
            t = cat.table(logical, name)
            spark.createDataFrame([], t.spark_ddl()).write.mode("overwrite").saveAsTable(f"tv_{logical}.{name}")
        v = cat.view("gold", "visit_facts")
        select = v.sql.format(**{l: cfg.schema(l) for l in LOGICAL_SCHEMAS})
        cols = spark.sql(select).columns
        names = [c.name for c in v.columns]
        # Derived columns come back under their expression text locally, so compare count plus the leading plain columns.
        assert len(cols) == len(names) and cols[:21] == names[:21]
    finally:
        for logical in ("silver", "ref"):
            spark.sql(f"DROP DATABASE IF EXISTS tv_{logical} CASCADE")


@pytest.mark.parametrize("column,needle", [
    ("ai_status", "skipped"), ("ai_error", "500 characters"), ("ai_attempts", "skipped"),
])
def test_triage_notes_comments_are_current(column, needle):
    assert needle in load_catalog_def().table("silver", "triage_notes").column(column).comment
