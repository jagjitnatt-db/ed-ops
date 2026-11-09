import pytest

from ed_ops.config import Config
from ed_ops.ddl import generate
from ed_ops.ddl.metadata import MetadataError, load_catalog_def, parse_catalog_def
from ed_ops.events_contract import EVENT_SCHEMA_DDL


def _minimal_doc(**table_overrides):
    table = {
        "schema": "gold",
        "name": "t",
        "comment": "A table.",
        "primary_key": ["id"],
        "cluster_by": ["id"],
        "cdf": True,
        "tags": {"data_classification": "internal", "contains_phi": "false"},
        "columns": [
            {"name": "id", "type": "STRING", "nullable": False, "comment": "Key."},
            {"name": "v", "type": "INT", "comment": "It's a value.", "tags": {"unit": "count"}},
        ],
    }
    table.update(table_overrides)
    return {"defaults": {"table_tags": {"domain": "ed", "data_origin": "synthetic"}}, "schemas": {"gold": "Gold."}, "tables": [table]}


def test_packaged_metadata_loads_and_is_complete():
    cat = load_catalog_def()
    names = {(t.schema, t.name) for t in cat.tables}
    assert ("silver", "ed_visits") in names
    assert ("gold", "facility_current") in names
    current = cat.table("gold", "facility_current")
    assert current.column_names[:2] == ["facility_id", "snapshot_ts"]
    assert current.column_names[-3:] == ["facility_name", "facility_type", "region"]
    for t in cat.tables:
        for key in ("layer", "domain", "data_classification", "contains_phi", "data_origin"):
            assert key in t.tags, (t.name, key)


def test_bronze_columns_match_event_contract(spark):
    bronze = load_catalog_def().table("bronze", "ed_events")
    contract = spark.createDataFrame([], EVENT_SCHEMA_DDL).schema
    assert bronze.column_names[: len(contract.fields)] == [f.name for f in contract.fields]
    table_schema = spark.createDataFrame([], bronze.spark_ddl()).schema
    assert table_schema["payload"].dataType == contract["payload"].dataType
    assert bronze.column_names[len(contract.fields):] == ["_rescued_data", "_source_file", "_ingest_ts", "ingest_date"]


def test_missing_column_comment_rejected():
    doc = _minimal_doc()
    doc["tables"][0]["columns"][1]["comment"] = ""
    with pytest.raises(MetadataError, match="comment"):
        parse_catalog_def(doc)


def test_nullable_primary_key_rejected():
    doc = _minimal_doc()
    doc["tables"][0]["columns"][0]["nullable"] = True
    with pytest.raises(MetadataError, match="primary key"):
        parse_catalog_def(doc)


def test_cluster_key_must_exist():
    with pytest.raises(MetadataError, match="cluster"):
        parse_catalog_def(_minimal_doc(cluster_by=["nope"]))


def test_create_table_sql_has_pk_clustering_cdf_and_escaped_comments():
    cfg = Config()
    t = parse_catalog_def(_minimal_doc()).table("gold", "t")
    sql = generate.create_table_sql(cfg, t)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS hls_jagjit_lb_playground_catalog.ed_gold.t (")
    assert "id STRING NOT NULL COMMENT 'Key.'" in sql
    assert "v INT COMMENT 'It\\'s a value.'" in sql
    assert "CONSTRAINT t_pk PRIMARY KEY (id)" in sql
    assert "CLUSTER BY (id)" in sql
    assert "'delta.enableChangeDataFeed' = 'true'" in sql


def test_tag_and_comment_statements():
    cfg = Config()
    t = parse_catalog_def(_minimal_doc()).table("gold", "t")
    tags = generate.tag_sqls(cfg, t)
    assert tags[0] == (
        "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t SET TAGS "
        "('layer' = 'gold', 'domain' = 'ed', 'data_origin' = 'synthetic', 'data_classification' = 'internal', 'contains_phi' = 'false')"
    )
    assert tags[1] == "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t ALTER COLUMN v SET TAGS ('unit' = 'count')"
    comments = generate.comment_sqls(cfg, t)
    assert comments[0] == "COMMENT ON TABLE hls_jagjit_lb_playground_catalog.ed_gold.t IS 'A table.'"
    assert comments[1] == "ALTER TABLE hls_jagjit_lb_playground_catalog.ed_gold.t ALTER COLUMN id COMMENT 'Key.'"


def test_ddl_statements_order_schemas_volumes_tables():
    stmts = generate.ddl_statements(Config(), load_catalog_def())
    first_volume = next(i for i, s in enumerate(stmts) if s.startswith("CREATE VOLUME"))
    first_table = next(i for i, s in enumerate(stmts) if s.startswith("CREATE TABLE"))
    last_schema = max(i for i, s in enumerate(stmts) if s.startswith("CREATE SCHEMA"))
    assert last_schema < first_volume < first_table
    assert sum(s.startswith("CREATE SCHEMA") for s in stmts) == 6


def test_spark_ddl_parses(spark):
    t = load_catalog_def().table("gold", "facility_snapshot_5min")
    df = spark.createDataFrame([], t.spark_ddl())
    assert df.columns == t.column_names
