"""ed_setup job tasks (serverless). Each task is idempotent and safe to re-run."""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import SparkSession

from ed_ops.config import Config
from ed_ops.ddl.apply import apply_ddl
from ed_ops.delta import merge_into, table_def
from ed_ops.generator.events import LandingWriter
from ed_ops.generator.facilities import build_facilities
from ed_ops.generator.population import build_patient_pool
from ed_ops.generator.reference import load_reference
from ed_ops.generator.roster import build_roster
from ed_ops.generator.stores import DeltaStore
from ed_ops.generator.tick import advance, initial_state
from ed_ops.governance.deid import ensure_salt
from ed_ops.pipeline import gold_metrics
from ed_ops.pipeline.bronze import start_bronze
from ed_ops.pipeline.silver_visits import start_silver_visits
from ed_ops.timeutil import floor_to, utcnow


def create_objects(spark: SparkSession, cfg: Config) -> None:
    n = apply_ddl(spark, cfg)
    ensure_salt(cfg)
    print(f"applied {n} DDL statements; salt present in secret scope {cfg.secret_scope}")


def seed_reference(spark: SparkSession, cfg: Config) -> None:
    now = utcnow()
    facilities = build_facilities(spark, cfg)
    merge_into(spark.createDataFrame(facilities, table_def("ref", "facility").spark_ddl()), cfg, "ref", "facility")
    roster = build_roster(facilities, (now - timedelta(days=cfg.backfill_days + 1)).date(), cfg.backfill_days + 9, cfg.timezone)
    merge_into(spark.createDataFrame(roster, table_def("ref", "physician_roster").spark_ddl()), cfg, "ref", "physician_roster")
    merge_into(build_patient_pool(spark, cfg.patient_pool_size, cfg.seed), cfg, "sim", "patient_pool")
    print(f"seeded {len(facilities)} facilities, {len(roster)} shifts, {cfg.patient_pool_size} patients")


def backfill_generate(spark: SparkSession, cfg: Config) -> None:
    store = DeltaStore(spark, cfg)
    now = floor_to(utcnow(), 60)
    if store.load_state() is None:
        store.save_state(initial_state(now, cfg.backfill_days))
    paths = advance(store, load_reference(spark, cfg), cfg, now, live=False,
                    writer=LandingWriter(cfg.landing_path), persist_each_chunk=False)
    print(f"wrote {len(paths)} landing files up to {now}")


def backfill_ingest(spark: SparkSession, cfg: Config) -> None:
    start_bronze(spark, cfg, available_now=True).awaitTermination()
    start_silver_visits(spark, cfg, available_now=True).awaitTermination()


def backfill_gold(spark: SparkSession, cfg: Config) -> None:
    state = DeltaStore(spark, cfg).load_state()
    if state is None:
        raise RuntimeError("generator state missing; run backfill-generate first")
    end = floor_to(state.last_emitted_ts, 300)
    gold_metrics.backfill_snapshots(spark, cfg, state.backfill_start_ts, end)
    gold_metrics.gold_tick(spark, cfg, end, gold_metrics.BaselineCache())
    print(f"gold history computed for ({state.backfill_start_ts}, {end}]")
