"""Per-facility local-hour baselines and the top deviating crowding drivers (spec section 7.4)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")
TOP_DRIVERS_TYPE = "ARRAY<STRUCT<driver: STRING, value: DOUBLE, baseline: DOUBLE, z: DOUBLE>>"


def _local_hour(tz: str):
    return F.hour(F.from_utc_timestamp(F.col("snapshot_ts"), tz))


def compute_baselines(snapshots: DataFrame, tz: str) -> DataFrame:
    aggs = []
    for d in DRIVER_COLUMNS:
        aggs.append(F.avg(F.col(d).cast("double")).alias(f"{d}_mean"))
        aggs.append(F.stddev_samp(F.col(d).cast("double")).alias(f"{d}_std"))
    return snapshots.withColumn("local_hour", _local_hour(tz)).groupBy("facility_id", "local_hour").agg(*aggs)


def add_drivers(snapshots: DataFrame, baselines: DataFrame, tz: str) -> DataFrame:
    df = snapshots.drop("top_drivers", "arrival_surge_ratio").withColumn("_local_hour", _local_hour(tz))
    df = df.join(baselines.withColumnRenamed("local_hour", "_local_hour"), ["facility_id", "_local_hour"], "left")
    candidates = F.array(
        *[
            F.struct(
                F.lit(d).alias("driver"),
                F.col(d).cast("double").alias("value"),
                F.col(f"{d}_mean").cast("double").alias("baseline"),
                F.try_divide(F.col(d).cast("double") - F.col(f"{d}_mean"), F.col(f"{d}_std")).alias("z"),
            )
            for d in DRIVER_COLUMNS
        ]
    )
    significant = F.filter(candidates, lambda s: F.abs(s["z"]) >= F.lit(1.0))
    ordered = F.array_sort(
        significant,
        lambda a, b: F.when(F.abs(a["z"]) > F.abs(b["z"]), F.lit(-1))
        .when(F.abs(a["z"]) < F.abs(b["z"]), F.lit(1))
        .otherwise(F.lit(0)),
    )
    df = df.withColumn("top_drivers", F.slice(ordered, 1, 3)).withColumn(
        "arrival_surge_ratio", F.try_divide(F.col("arrivals_1h").cast("double"), F.col("arrivals_1h_mean"))
    )
    baseline_cols = [f"{d}_{s}" for d in DRIVER_COLUMNS for s in ("mean", "std")]
    return df.drop("_local_hour", *baseline_cols)


def with_null_drivers(df: DataFrame) -> DataFrame:
    return df.withColumn("arrival_surge_ratio", F.lit(None).cast("double")).withColumn(
        "top_drivers", F.lit(None).cast(TOP_DRIVERS_TYPE)
    )
