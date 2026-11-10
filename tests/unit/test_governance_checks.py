from ed_ops.config import Config
from ed_ops.governance.checks import (
    ai_mask_miss_rate, check_comments, check_phi_tags, check_table_tags, phi_match_counts,
)

CFG = Config()


def test_missing_table_and_column_comments(spark):
    tables = spark.createDataFrame([("ed_gold", "a", "ok"), ("ed_gold", "b", " ")], "table_schema STRING, table_name STRING, comment STRING")
    columns = spark.createDataFrame([("ed_gold", "a", "x", "fine"), ("ed_gold", "a", "y", None)],
                                    "table_schema STRING, table_name STRING, column_name STRING, comment STRING")
    v = check_comments(tables, columns)
    assert {(x.check, x.object) for x in v} == {("comment", "ed_gold.b"), ("comment", "ed_gold.a.y")}


def test_required_table_tags_use_prefix(spark):
    tables = spark.createDataFrame([("ed_gold", "a", "c"), ("ed_gold", "b", "c")], "table_schema STRING, table_name STRING, comment STRING")
    full = [("ed_gold", "a", f"ed_{k}") for k in ("layer", "domain", "data_classification", "contains_phi", "data_origin")]
    tags = spark.createDataFrame(full + [("ed_gold", "b", "layer")], "schema_name STRING, table_name STRING, tag_name STRING")
    v = check_table_tags(tables, tags, CFG)
    assert {x.object for x in v} == {"ed_gold.b"}  # an unprefixed 'layer' tag does not count
    assert "ed_layer" in v[0].detail


def test_phi_tags_raw_phi_only_in_bronze_and_sim(spark):
    rows = [
        ("ed_bronze", "ed_events", "payload", "ed_phi_type"),  # raw PHI in bronze: allowed
        ("ed_silver", "triage_notes", "note_masked", "ed_phi_type"),
        ("ed_silver", "triage_notes", "note_masked", "ed_deid_method"),  # de-identified: allowed
        ("ed_silver", "ed_visits", "city", "ed_phi_type"),  # no deid_method: violation
        ("ed_gold", "facility_current", "x", "ed_phi_type"),  # any phi tag in gold: violation
        ("ed_gold", "facility_current", "x", "ed_deid_method"),
    ]
    v = check_phi_tags(spark.createDataFrame(rows, "schema_name STRING, table_name STRING, column_name STRING, tag_name STRING"), CFG)
    assert {(x.check, x.object) for x in v} == {
        ("raw_phi_outside_bronze_sim", "ed_silver.ed_visits.city"),
        ("phi_tag_in_serving_layer", "ed_gold.facility_current.x"),
    }


def test_phi_content_scan_counts_matches(spark):
    df = spark.createDataFrame(
        [("call 312-555-0101", "ok", "BP 120/80"), ("MRN-7000001", "fine", "DOB 01/02/1950"), (None, "x", "9/10")],
        "a STRING, b STRING, c STRING",
    )
    assert phi_match_counts(df, ["a", "b", "c"]) == {"a": 2, "b": 0, "c": 1}


def test_ai_mask_miss_rate(spark):
    notes = spark.createDataFrame([("pt Ava Smith arrived",), ("pt [MASKED] arrived",), (None,), ("[MASKED] ok",)], "note_masked STRING")
    assert ai_mask_miss_rate(notes) == 1 / 3
    assert ai_mask_miss_rate(spark.createDataFrame([(None,)], "note_masked STRING")) is None
