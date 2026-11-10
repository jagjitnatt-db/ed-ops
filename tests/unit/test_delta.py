import pytest

from ed_ops.delta import conform, merge_sql, table_def


def test_merge_sql_updates_non_keys_and_inserts_all():
    sql = merge_sql("c.s.t", "src_view", ["facility_id"], ["facility_id", "census", "gedwin"])
    assert sql == (
        "MERGE INTO c.s.t AS t USING src_view AS s\n"
        "ON t.facility_id = s.facility_id\n"
        "WHEN MATCHED THEN UPDATE SET t.census = s.census, t.gedwin = s.gedwin\n"
        "WHEN NOT MATCHED THEN INSERT (facility_id, census, gedwin) VALUES (s.facility_id, s.census, s.gedwin)"
    )


def test_conform_orders_and_casts(spark):
    t = table_def("ref", "physician_roster")
    df = spark.createDataFrame(
        [(3, "FAC001", "day")], "attendings_on_duty BIGINT, facility_id STRING, shift_name STRING"
    ).selectExpr("*", "timestamp'2026-11-09 07:00:00' AS shift_start", "timestamp'2026-11-09 15:00:00' AS shift_end")
    out = conform(df, t)
    assert out.columns == t.column_names
    assert dict(out.dtypes)["attendings_on_duty"] == "int"


def test_conform_rejects_missing_columns(spark):
    df = spark.createDataFrame([("FAC001",)], "facility_id STRING")
    with pytest.raises(ValueError, match="missing columns"):
        conform(df, table_def("ref", "physician_roster"))


def test_idempotent_append_options_include_checkpoint_version():
    from ed_ops.config import Config
    from ed_ops.delta import txn_options

    assert txn_options(Config(schema_prefix="ed_", checkpoint_version=2), "quarantine", 17) == {
        "txnAppId": "ed_quarantine_v2", "txnVersion": "17",
    }
