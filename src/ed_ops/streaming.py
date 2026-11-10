"""ed_streaming job tasks (classic cluster, continuous). Each task runs one long-lived query."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from ed_ops.config import Config
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.events import LandingWriter
from ed_ops.generator.reference import ReferenceData, load_reference
from ed_ops.generator.roster import build_roster
from ed_ops.generator.stores import DeltaStore
from ed_ops.generator.tick import advance
from ed_ops.pipeline.bronze import start_bronze
from ed_ops.pipeline.gold_metrics import start_gold_metrics
from ed_ops.pipeline.gold_narratives import start_gold_narratives
from ed_ops.pipeline.silver_notes_ai import start_ai_retry, start_silver_notes_ai
from ed_ops.pipeline.silver_visits import start_silver_visits
from ed_ops.timeutil import floor_to, utcnow

ROSTER_HORIZON = timedelta(days=2)


def ensure_roster_horizon(spark: SparkSession, cfg: Config, now: datetime) -> ReferenceData:
    ref = load_reference(spark, cfg)
    horizon = ref.roster["shift_end"].max() if not ref.roster.empty else None
    if horizon is None or pd.isna(horizon) or horizon < now + ROSTER_HORIZON:
        roster = build_roster(ref.facilities, (now - timedelta(days=1)).date(), 9, cfg.timezone)
        merge_into(spark.createDataFrame(roster, table_def("ref", "physician_roster").spark_ddl()), cfg, "ref", "physician_roster")
        ref = load_reference(spark, cfg)
    return ref


class _ReferenceCache:
    def __init__(self) -> None:
        self.ref: ReferenceData | None = None
        self.hour: datetime | None = None

    def get(self, spark: SparkSession, cfg: Config, now: datetime) -> ReferenceData:
        hour = floor_to(now, 3600)
        if self.ref is None or self.hour != hour:
            self.ref = ensure_roster_horizon(spark, cfg, now)
            self.hour = hour
        return self.ref


def run_generator(spark: SparkSession, cfg: Config) -> None:
    cache = _ReferenceCache()

    def tick(batch_df: DataFrame, _batch_id: int) -> None:
        session = batch_df.sparkSession
        now = floor_to(utcnow(), cfg.tick_seconds)
        advance(DeltaStore(session, cfg), cache.get(session, cfg, now), cfg, now, live=True,
                writer=LandingWriter(cfg.landing_path), persist_each_chunk=True)

    (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(tick)
        .trigger(processingTime=f"{cfg.tick_seconds} seconds")
        .option("checkpointLocation", cfg.checkpoint("generator"))
        .start()
        .awaitTermination()
    )


def run_bronze(spark: SparkSession, cfg: Config) -> None:
    start_bronze(spark, cfg).awaitTermination()


def run_silver_visits(spark: SparkSession, cfg: Config) -> None:
    start_silver_visits(spark, cfg).awaitTermination()


def run_gold_metrics(spark: SparkSession, cfg: Config) -> None:
    start_gold_metrics(spark, cfg).awaitTermination()


def run_notes_ai(spark: SparkSession, cfg: Config) -> None:
    start_silver_notes_ai(spark, cfg).awaitTermination()


def run_ai_retry(spark: SparkSession, cfg: Config) -> None:
    start_ai_retry(spark, cfg).awaitTermination()


def run_narratives(spark: SparkSession, cfg: Config) -> None:
    start_gold_narratives(spark, cfg).awaitTermination()
