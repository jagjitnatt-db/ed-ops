from datetime import datetime

import pytest

from ed_ops.metrics.drivers import add_drivers, compute_baselines, with_null_drivers

DDL = "facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE"
TZ = "America/Chicago"


def test_top_drivers_ranked_by_absolute_z(spark):
    # Three history points at 18:00 UTC = 12:00 Chicago (CST) on different days.
    history = spark.createDataFrame(
        [
            ("FAC001", datetime(2026, 11, 2, 18), 0.4, 0.1, 4.0, 10, 0.1),
            ("FAC001", datetime(2026, 11, 3, 18), 0.5, 0.1, 5.0, 12, 0.2),
            ("FAC001", datetime(2026, 11, 4, 18), 0.6, 0.1, 6.0, 14, 0.3),
        ],
        DDL,
    )
    baselines = compute_baselines(history, TZ)
    current = spark.createDataFrame(
        [
            ("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35),  # z: 3.0, null(std 0), 0.5, 2.0, 1.5
            ("FAC001", datetime(2026, 11, 9, 3), 0.8, 0.5, 5.5, 16, 0.35),  # no baseline at this local hour
        ],
        DDL,
    )
    out = {r["snapshot_ts"]: r for r in add_drivers(current, baselines, TZ).collect()}
    top = out[datetime(2026, 11, 9, 18)]["top_drivers"]
    assert [d["driver"] for d in top] == ["occupancy_ratio", "arrival_surge_ratio", "high_acuity_share"]
    assert top[0]["z"] == pytest.approx(3.0)
    assert top[0]["baseline"] == pytest.approx(0.5) and top[0]["value"] == pytest.approx(0.8)
    surge = top[1]
    assert surge["value"] == pytest.approx(16 / 12) and surge["baseline"] == 1.0 and surge["z"] == pytest.approx(2.0)
    assert out[datetime(2026, 11, 9, 18)]["arrival_surge_ratio"] == pytest.approx(16 / 12)
    assert out[datetime(2026, 11, 9, 3)]["top_drivers"] == []
    assert out[datetime(2026, 11, 9, 3)]["arrival_surge_ratio"] is None


def test_add_drivers_replaces_existing_driver_columns(spark):
    history = spark.createDataFrame([("FAC001", datetime(2026, 11, 2, 18), 0.4, 0.1, 4.0, 10, 0.1)], DDL)
    current = with_null_drivers(spark.createDataFrame([("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35)], DDL))
    out = add_drivers(current, compute_baselines(history, TZ), TZ)
    assert out.columns.count("top_drivers") == 1 and out.columns.count("arrival_surge_ratio") == 1


def test_with_null_drivers_types(spark):
    df = with_null_drivers(spark.createDataFrame([("FAC001", datetime(2026, 11, 9, 18), 0.8, 0.5, 5.5, 16, 0.35)], DDL))
    assert dict(df.dtypes)["top_drivers"] == "array<struct<driver:string,value:double,baseline:double,z:double>>"
    assert dict(df.dtypes)["arrival_surge_ratio"] == "double"
