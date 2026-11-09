"""De-identified live ED board for gold.ed_board_current (insert/update only; no deletes)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def DEPARTED_LOOKBACK():
    return F.expr("INTERVAL 30 MINUTES")


def board_rows(visits: DataFrame, as_of: datetime) -> DataFrame:
    t = F.lit(as_of).cast("timestamp")
    arrival, departure = F.col("arrival_ts"), F.col("departure_ts")
    in_ed = (arrival <= t) & (departure.isNull() | (departure > t))
    departed_recently = (departure > t - DEPARTED_LOOKBACK()) & (departure <= t)
    greeted = F.coalesce(F.col("greet_ts") <= t, F.lit(False))
    bedded = F.coalesce(F.col("bed_assigned_ts") <= t, F.lit(False))
    boarder = F.coalesce((F.col("disposition") == "ADMIT") & (F.col("disposition_ts") <= t), F.lit(False))
    on_vent = F.coalesce(
        (F.col("vent_start_ts") <= t) & (F.col("vent_end_ts").isNull() | (F.col("vent_end_ts") > t)), F.lit(False)
    )
    df = visits.filter(arrival.isNotNull() & (in_ed | departed_recently)).withColumn("in_ed", in_ed)
    end = F.when(F.col("in_ed"), t).otherwise(departure)
    waited_until = F.when(greeted, F.col("greet_ts")).otherwise(end)
    minutes = lambda later, earlier: F.floor((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / 60).cast("int")
    return df.select(
        "facility_id",
        "visit_id",
        "in_ed",
        F.when(~F.col("in_ed"), F.lit("departed"))
        .when(boarder, F.lit("boarding"))
        .when(greeted | bedded, F.lit("in_bed"))
        .otherwise(F.lit("waiting"))
        .alias("status"),
        F.when(F.col("triage_ts") <= t, F.col("esi_level")).alias("esi_level"),
        "suggested_esi",
        "undertriage_flag",
        "complaint_category",
        "arrival_ts",
        F.when(~F.col("in_ed"), departure).alias("departed_ts"),
        minutes(end, arrival).alias("minutes_in_ed"),
        minutes(waited_until, arrival).alias("minutes_waiting"),
        "arrival_mode",
        F.when(F.col("in_ed"), on_vent).otherwise(F.lit(False)).alias("on_vent"),
        t.alias("as_of_ts"),
        F.current_timestamp().alias("computed_at"),
    )
