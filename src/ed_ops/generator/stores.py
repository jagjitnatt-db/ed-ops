"""Persistence for generator state and visit plans: in-memory (tests) and Delta (workspace)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.journey import empty_plan, normalize_plan
from ed_ops.generator.state import GeneratorState
from ed_ops.timeutil import utcnow

GENERATOR_ID = "main"


class InMemoryStore:
    def __init__(self, state: GeneratorState | None = None) -> None:
        self.state = state
        self.plan = empty_plan()

    def load_state(self) -> GeneratorState | None:
        return self.state

    def save_state(self, state: GeneratorState) -> None:
        self.state = state

    def load_plan(self, since: datetime) -> pd.DataFrame:
        return self.plan[self.plan["arrival_ts"] >= since].reset_index(drop=True)

    def upsert_plan(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        merged = pd.concat([self.plan, df], ignore_index=True) if not self.plan.empty else df
        self.plan = normalize_plan(merged.drop_duplicates(subset=["facility_id", "visit_id"], keep="last"))


class DeltaStore:
    def __init__(self, spark: SparkSession, cfg: Config) -> None:
        self.spark = spark
        self.cfg = cfg

    def load_state(self) -> GeneratorState | None:
        rows = (
            self.spark.table(self.cfg.table("sim", "generator_state"))
            .filter(F.col("generator_id") == GENERATOR_ID)
            .collect()
        )
        if not rows:
            return None
        r = rows[0]
        return GeneratorState(r.generator_id, r.backfill_start_ts, r.last_planned_ts, r.last_emitted_ts, r.live_started_ts)

    def save_state(self, state: GeneratorState) -> None:
        df = self.spark.createDataFrame(
            [(state.generator_id, state.backfill_start_ts, state.last_planned_ts, state.last_emitted_ts,
              state.live_started_ts, utcnow())],
            table_def("sim", "generator_state").spark_ddl(),
        )
        merge_into(df, self.cfg, "sim", "generator_state")

    def load_plan(self, since: datetime) -> pd.DataFrame:
        pdf = (
            self.spark.table(self.cfg.table("sim", "visit_plan"))
            .filter(F.col("arrival_ts") >= F.lit(since).cast("timestamp"))
            .toPandas()
        )
        return normalize_plan(pdf)

    def upsert_plan(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        merge_into(self.spark.createDataFrame(df, table_def("sim", "visit_plan").spark_ddl()), self.cfg, "sim", "visit_plan")
