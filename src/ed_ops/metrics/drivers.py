"""Per-facility local-hour baselines and the top deviating crowding drivers (spec section 7.4)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Snapshot columns that get a local-hour baseline (mean/std) ...
DRIVER_COLUMNS = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrivals_1h", "high_acuity_share")
# ... and the driver names reported in top_drivers (spec 7.4): arrivals are reported as arrival_surge_ratio.
DRIVER_NAMES = ("occupancy_ratio", "boarder_share", "patients_per_attending", "arrival_surge_ratio", "high_acuity_share")
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
    surge = F.try_divide(F.col("arrivals_1h").cast("double"), F.col("arrivals_1h_mean"))

    def z(d: str):
        return F.try_divide(F.col(d).cast("double") - F.col(f"{d}_mean"), F.col(f"{d}_std"))

    def entry(name: str, value, baseline, zscore):
        return F.struct(F.lit(name).alias("driver"), value.cast("double").alias("value"),
                        baseline.cast("double").alias("baseline"), zscore.alias("z"))

    candidates = F.array(
        *[
            entry("arrival_surge_ratio", surge, F.lit(1.0), z(d)) if d == "arrivals_1h"
            else entry(d, F.col(d), F.col(f"{d}_mean"), z(d))
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
    df = df.withColumn("top_drivers", F.slice(ordered, 1, 3)).withColumn("arrival_surge_ratio", surge)
    baseline_cols = [f"{d}_{s}" for d in DRIVER_COLUMNS for s in ("mean", "std")]
    return df.drop("_local_hour", *baseline_cols)


def with_null_drivers(df: DataFrame) -> DataFrame:
    return df.withColumn("arrival_surge_ratio", F.lit(None).cast("double")).withColumn(
        "top_drivers", F.lit(None).cast(TOP_DRIVERS_TYPE)
    )
