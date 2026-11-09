"""Point-in-time ED metrics for any set of (facility, snapshot_ts) pairs.

Every window join is turned into an equi-join on (facility_id, hour bucket) by exploding each visit or
event into the hour buckets it can affect, then filtering on the exact condition. This keeps the 14-day
backfill (thousands of snapshot times) tractable without relying on range-join hints.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ed_ops.metrics import formulas as fx

VISIT_COLUMNS = (
    "facility_id", "visit_id", "arrival_ts", "arrival_mode", "esi_level", "triage_ts", "bed_assigned_ts",
    "greet_ts", "vent_start_ts", "vent_end_ts", "disposition", "disposition_ts", "lwbs_ts", "departure_ts",
    "suggested_esi", "undertriage_flag", "complaint_category",
)

METRIC_COLUMNS = (
    "facility_id", "snapshot_ts", "census", "n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged",
    "waiting_now_n", "in_bed_n", "longest_current_wait_min", "d2p_mean_min", "d2p_median_min", "d2p_p90_min",
    "d2p_sample_n", "occupancy_ratio", "acuity_ratio", "attendings_on_duty", "edwin", "edwin_band", "gedwin",
    "gedwin_band", "gedwin_delta_1h", "boarders_n", "vents_n", "longest_boarder_h", "last_bedded_wait_h",
    "nedocs", "nedocs_level", "lwbs_rate_24h", "los_median_h_24h", "los_p90_h_24h", "boarding_median_h_24h",
    "admit_rate_24h", "arrivals_1h", "ambulance_arrivals_1h", "patients_per_attending", "boarder_share",
    "high_acuity_share", "undertriage_flags_n", "dq_flags", "computed_at",
)

_COUNT_COLUMNS = (
    "census", "n_esi1", "n_esi2", "n_esi3", "n_esi4", "n_esi5", "n_untriaged", "waiting_now_n", "in_bed_n",
    "boarders_n", "vents_n", "undertriage_flags_n", "d2p_sample_n", "arrivals_1h", "ambulance_arrivals_1h",
    "_weight_sum",
)

KEYS = ["facility_id", "snapshot_ts"]


# Column expressions are built lazily: classic PySpark needs an active SparkContext to create them.
def HOUR() -> Column:
    return F.expr("INTERVAL 1 HOUR")


def MAX_STAY() -> Column:
    return F.expr("INTERVAL 3 DAYS")


def WIN_1H() -> Column:
    return F.expr("INTERVAL 60 MINUTES")


def WIN_24H() -> Column:
    return F.expr("INTERVAL 24 HOURS")


def T() -> Column:
    return F.col("snapshot_ts")


def _seconds(later: Column, earlier: Column) -> Column:
    return (F.unix_timestamp(later) - F.unix_timestamp(earlier)).cast("double")


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0)))


def _candidates(df: DataFrame, start: Column, end: Column) -> DataFrame:
    return df.withColumn("_bucket", F.explode(F.sequence(F.date_trunc("hour", start), F.date_trunc("hour", end), HOUR())))


def _pairs(grid_b: DataFrame, candidates: DataFrame, condition: Column) -> DataFrame:
    return grid_b.join(candidates, ["facility_id", "_bucket"]).filter(condition)


def _window_pairs(grid_b: DataFrame, visits: DataFrame, ts_col: str, window: Column) -> DataFrame:
    ts = F.col(ts_col)
    candidates = _candidates(visits.filter(ts.isNotNull()), ts, ts + window)
    return _pairs(grid_b, candidates, (ts > T() - window) & (ts <= T()))


def _in_ed_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    arrival, departure = F.col("arrival_ts"), F.col("departure_ts")
    end = F.least(F.coalesce(departure, arrival + MAX_STAY()), arrival + MAX_STAY())
    pairs = _pairs(
        grid_b,
        _candidates(visits.filter(arrival.isNotNull()), arrival, end),
        (arrival <= T()) & (arrival > T() - MAX_STAY()) & (departure.isNull() | (departure > T())),
    )
    esi_t = F.when(F.col("triage_ts") <= T(), F.col("esi_level"))
    greeted = F.coalesce(F.col("greet_ts") <= T(), F.lit(False))
    bedded = F.coalesce(F.col("bed_assigned_ts") <= T(), F.lit(False))
    boarder = F.coalesce((F.col("disposition") == "ADMIT") & (F.col("disposition_ts") <= T()), F.lit(False))
    on_vent = F.coalesce(
        (F.col("vent_start_ts") <= T()) & (F.col("vent_end_ts").isNull() | (F.col("vent_end_ts") > T())), F.lit(False)
    )
    flagged = F.coalesce(F.col("undertriage_flag") & (F.col("triage_ts") <= T()), F.lit(False))
    pairs = pairs.withColumn("_esi_t", esi_t)
    return pairs.groupBy(*KEYS).agg(
        F.count(F.lit(1)).alias("census"),
        *[_count_if(F.col("_esi_t") == i).alias(f"n_esi{i}") for i in range(1, 6)],
        _count_if(F.col("_esi_t").isNull()).alias("n_untriaged"),
        F.sum(fx.triage_weight(F.col("_esi_t"))).alias("_weight_sum"),
        _count_if(~greeted).alias("waiting_now_n"),
        _count_if(bedded).alias("in_bed_n"),
        F.max(F.when(~greeted, _seconds(T(), arrival) / 60.0)).alias("longest_current_wait_min"),
        _count_if(boarder).alias("boarders_n"),
        _count_if(on_vent).alias("vents_n"),
        F.max(F.when(boarder, _seconds(T(), F.col("disposition_ts")) / 3600.0)).alias("longest_boarder_h"),
        _count_if(flagged).alias("undertriage_flags_n"),
    )


def _d2p_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    valid = visits.filter(F.col("greet_ts") >= F.col("arrival_ts"))
    pairs = _window_pairs(grid_b, valid, "greet_ts", WIN_1H())
    pairs = pairs.withColumn("_d2p", _seconds(F.col("greet_ts"), F.col("arrival_ts")) / 60.0)
    return pairs.groupBy(*KEYS).agg(
        F.avg("_d2p").alias("d2p_mean_min"),
        F.percentile("_d2p", 0.5).alias("d2p_median_min"),
        F.percentile("_d2p", 0.9).alias("d2p_p90_min"),
        F.count(F.lit(1)).alias("d2p_sample_n"),
    )


def _arrival_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "arrival_ts", WIN_1H())
    return pairs.groupBy(*KEYS).agg(
        F.count(F.lit(1)).alias("arrivals_1h"),
        _count_if(F.col("arrival_mode") == "ambulance").alias("ambulance_arrivals_1h"),
    )


def _last_bedded(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "bed_assigned_ts", WIN_24H())
    wait_h = _seconds(F.col("bed_assigned_ts"), F.col("arrival_ts")) / 3600.0
    return pairs.groupBy(*KEYS).agg(F.max_by(wait_h, F.col("bed_assigned_ts")).alias("last_bedded_wait_h"))


def _departure_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "departure_ts", WIN_24H())
    los_h = F.when(F.col("disposition") == "DISCHARGE", _seconds(F.col("departure_ts"), F.col("arrival_ts")) / 3600.0)
    boarding_h = F.when(F.col("disposition") == "ADMIT", _seconds(F.col("departure_ts"), F.col("disposition_ts")) / 3600.0)
    return pairs.groupBy(*KEYS).agg(
        F.try_divide(_count_if(F.col("lwbs_ts").isNotNull()).cast("double"), F.count(F.lit(1)).cast("double")).alias("lwbs_rate_24h"),
        F.percentile(los_h, 0.5).alias("los_median_h_24h"),
        F.percentile(los_h, 0.9).alias("los_p90_h_24h"),
        F.percentile(boarding_h, 0.5).alias("boarding_median_h_24h"),
    )


def _disposition_stats(grid_b: DataFrame, visits: DataFrame) -> DataFrame:
    pairs = _window_pairs(grid_b, visits, "disposition_ts", WIN_24H())
    return pairs.groupBy(*KEYS).agg(
        F.try_divide(_count_if(F.col("disposition") == "ADMIT").cast("double"), F.count(F.lit(1)).cast("double")).alias("admit_rate_24h")
    )


def _attendings(grid_b: DataFrame, roster: DataFrame) -> DataFrame:
    start, end = F.col("shift_start"), F.col("shift_end")
    pairs = _pairs(grid_b, _candidates(roster, start, end), (start <= T()) & (end > T()))
    return pairs.groupBy(*KEYS).agg(F.sum("attendings_on_duty").cast("int").alias("attendings_on_duty"))


def _metrics_at(visits: DataFrame, roster: DataFrame, facilities: DataFrame, times: DataFrame) -> DataFrame:
    grid = facilities.select("facility_id", "ed_beds", "inpatient_beds").crossJoin(F.broadcast(times))
    grid_b = grid.withColumn("_bucket", F.date_trunc("hour", T()))
    df = grid
    for part in (
        _in_ed_stats(grid_b, visits),
        _d2p_stats(grid_b, visits),
        _arrival_stats(grid_b, visits),
        _last_bedded(grid_b, visits),
        _departure_stats(grid_b, visits),
        _disposition_stats(grid_b, visits),
        _attendings(grid_b, roster),
    ):
        df = df.join(part, KEYS, "left")
    df = df.fillna(0, subset=list(_COUNT_COLUMNS))
    census, beds, attendings = F.col("census"), F.col("ed_beds"), F.col("attendings_on_duty")
    df = (
        df.withColumn("occupancy_ratio", F.try_divide(census.cast("double"), beds.cast("double")))
        .withColumn("acuity_ratio", fx.acuity_ratio(F.col("_weight_sum"), census))
        .withColumn("edwin", fx.edwin(census, beds, attendings, F.col("acuity_ratio")))
        .withColumn("edwin_band", fx.edwin_band(F.col("edwin")))
        .withColumn("gedwin", fx.gedwin(census, beds, F.col("acuity_ratio")))
        .withColumn("gedwin_band", fx.gedwin_band(F.col("gedwin"), census))
        .withColumn(
            "nedocs",
            fx.nedocs(census, beds, F.col("boarders_n"), F.col("inpatient_beds"), F.col("vents_n"),
                      F.col("longest_boarder_h"), F.col("last_bedded_wait_h")),
        )
        .withColumn("nedocs_level", fx.nedocs_level(F.col("nedocs")))
        .withColumn("patients_per_attending", F.try_divide(census.cast("double"), attendings.cast("double")))
        .withColumn("boarder_share", F.try_divide(F.col("boarders_n").cast("double"), census.cast("double")))
        .withColumn(
            "high_acuity_share", F.try_divide((F.col("n_esi1") + F.col("n_esi2")).cast("double"), census.cast("double"))
        )
        .withColumn(
            "dq_flags",
            F.array_compact(
                F.array(
                    F.when(attendings.isNull() | (attendings <= 0), F.lit("no_attending_on_duty")),
                    F.when(F.col("occupancy_ratio") > 1, F.lit("census_exceeds_beds")),
                )
            ),
        )
        .withColumn("computed_at", F.current_timestamp())
    )
    return df


def compute_snapshots(visits: DataFrame, roster: DataFrame, facilities: DataFrame, as_of: DataFrame) -> DataFrame:
    requested = as_of.select("snapshot_ts").distinct()
    times = requested.unionByName(requested.select((T() - HOUR()).alias("snapshot_ts"))).distinct()
    base = _metrics_at(visits, roster, facilities, times)
    prior = base.select("facility_id", (T() + HOUR()).alias("snapshot_ts"), F.col("gedwin").alias("_gedwin_prior"))
    return (
        base.join(F.broadcast(requested), "snapshot_ts")
        .join(prior, KEYS, "left")
        .withColumn("gedwin_delta_1h", F.col("gedwin") - F.col("_gedwin_prior"))
        .select(*METRIC_COLUMNS)
    )
