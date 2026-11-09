"""Gold ticker: every minute recompute facility_current, the trailing 30 minutes of 5-minute history and
the ED board from silver state; the same compute path backfills history (spec sections 7.5 and 8)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.delta import merge_into
from ed_ops.metrics.board import board_rows
from ed_ops.metrics.drivers import add_drivers, compute_baselines, with_null_drivers
from ed_ops.metrics.snapshots import VISIT_COLUMNS, compute_snapshots
from ed_ops.timeutil import five_minute_marks, floor_to, utcnow

HISTORY_RECOMPUTE = timedelta(minutes=30)
BASELINE_WINDOW = timedelta(days=14)
MAX_STAY = timedelta(days=3)


@dataclass
class TickFrames:
    current: DataFrame
    history: DataFrame
    board: DataFrame


def _materialize(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame(df.collect(), df.schema)


def _times(spark: SparkSession, times: list[datetime]) -> DataFrame:
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


def day_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks, t = [], start
    while t < end:
        nxt = min(t + timedelta(days=1), end)
        chunks.append((t, nxt))
        t = nxt
    return chunks


def load_visits(spark: SparkSession, cfg: Config) -> DataFrame:
    notes = spark.table(cfg.table("silver", "triage_notes")).select(
        "facility_id", "visit_id", "suggested_esi", "undertriage_flag", "complaint_category"
    )
    return spark.table(cfg.table("silver", "ed_visits")).join(notes, ["facility_id", "visit_id"], "left").select(*VISIT_COLUMNS)


def load_roster(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.table(cfg.table("ref", "physician_roster"))


def load_facilities(spark: SparkSession, cfg: Config) -> DataFrame:
    return spark.table(cfg.table("ref", "facility"))


def compute_tick_frames(visits: DataFrame, roster: DataFrame, facilities: DataFrame, baselines: DataFrame,
                        now: datetime, tz: str) -> TickFrames:
    spark = visits.sparkSession
    marks = five_minute_marks(now - HISTORY_RECOMPUTE, now)
    snaps = compute_snapshots(visits, roster, facilities, _times(spark, sorted(set(marks) | {now})))
    snaps = _materialize(add_drivers(snaps, baselines, tz))
    current = snaps.filter(F.col("snapshot_ts") == F.lit(now)).join(
        facilities.select("facility_id", "facility_name", "facility_type", "region"), "facility_id"
    )
    history = snaps.filter(F.col("snapshot_ts").isin(marks))
    return TickFrames(current=current, history=history, board=board_rows(visits, now))


class BaselineCache:
    """Baselines change slowly; recompute at most once per hour."""

    def __init__(self) -> None:
        self._hour: datetime | None = None
        self._df: DataFrame | None = None

    def get(self, spark: SparkSession, cfg: Config, now: datetime) -> DataFrame:
        hour = floor_to(now, 3600)
        if self._df is None or self._hour != hour:
            history = spark.table(cfg.table("gold", "facility_snapshot_5min")).filter(
                F.col("snapshot_ts") > F.lit(now - BASELINE_WINDOW)
            )
            self._df = _materialize(compute_baselines(history, cfg.timezone))
            self._hour = hour
        return self._df


def gold_tick(spark: SparkSession, cfg: Config, now: datetime, cache: BaselineCache) -> None:
    visits = load_visits(spark, cfg).filter(
        (F.col("arrival_ts") > F.lit(now - MAX_STAY))
        & (F.col("departure_ts").isNull() | (F.col("departure_ts") > F.lit(now - timedelta(hours=25))))
    )
    roster = load_roster(spark, cfg).filter(F.col("shift_end") > F.lit(now - timedelta(days=2)))
    frames = compute_tick_frames(visits, roster, load_facilities(spark, cfg), cache.get(spark, cfg, now), now, cfg.timezone)
    merge_into(frames.current, cfg, "gold", "facility_current")
    merge_into(frames.history, cfg, "gold", "facility_snapshot_5min")
    merge_into(frames.board, cfg, "gold", "ed_board_current")


def backfill_snapshots(spark: SparkSession, cfg: Config, start: datetime, end: datetime) -> None:
    """Compute 5-minute snapshots in (start, end] day by day, then add drivers from the stored history."""
    visits, roster, facilities = load_visits(spark, cfg), load_roster(spark, cfg), load_facilities(spark, cfg)
    for chunk_start, chunk_end in day_chunks(start, end):
        marks = five_minute_marks(chunk_start, chunk_end)
        if not marks:
            continue
        chunk_visits = visits.filter(
            (F.col("arrival_ts") > F.lit(chunk_start - MAX_STAY)) & (F.col("arrival_ts") <= F.lit(chunk_end))
        )
        snaps = with_null_drivers(compute_snapshots(chunk_visits, roster, facilities, _times(spark, marks)))
        merge_into(snaps, cfg, "gold", "facility_snapshot_5min")
    table = spark.table(cfg.table("gold", "facility_snapshot_5min"))
    baselines = _materialize(compute_baselines(table.filter(F.col("snapshot_ts") > F.lit(end - BASELINE_WINDOW)), cfg.timezone))
    window = table.filter((F.col("snapshot_ts") > F.lit(start)) & (F.col("snapshot_ts") <= F.lit(end)))
    merge_into(add_drivers(window, baselines, cfg.timezone), cfg, "gold", "facility_snapshot_5min")


def fill_gap(spark: SparkSession, cfg: Config, now: datetime) -> None:
    """After setup or downtime, backfill history older than the ticker's 30-minute recompute window."""
    last = spark.table(cfg.table("gold", "facility_snapshot_5min")).agg(F.max("snapshot_ts")).collect()[0][0]
    gap_end = floor_to(now - HISTORY_RECOMPUTE, 300)
    if last is not None and last < gap_end:
        backfill_snapshots(spark, cfg, last, gap_end)


def start_gold_metrics(spark: SparkSession, cfg: Config) -> StreamingQuery:
    fill_gap(spark, cfg, floor_to(utcnow(), 60))
    cache = BaselineCache()

    def tick(batch_df: DataFrame, _batch_id: int) -> None:
        gold_tick(batch_df.sparkSession, cfg, floor_to(utcnow(), 60), cache)

    return (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(tick)
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", cfg.checkpoint("gold_metrics"))
        .start()
    )
