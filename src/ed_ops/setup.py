"""ed_setup job tasks (serverless). Idempotent. Once the generator has gone live, ed_streaming owns the generator,
bronze, silver and gold checkpoints, so generate/ingest/gold skip. The notes, driver and narrative tasks refuse to
run while ed_streaming is active: pause it first (docs/runbook-plan2.md)."""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ed_ops.ai import DatabricksAi
from ed_ops.config import Config
from ed_ops.governance.checks import run_checks
from ed_ops.pipeline.gold_narratives import narrative_tick
from ed_ops.pipeline.silver_notes_ai import start_silver_notes_ai
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


ACTIVE_WINDOW = timedelta(minutes=5)


def generator_live(live_started_ts: datetime | None) -> bool:
    return live_started_ts is not None


def streaming_active(live_started_ts: datetime | None, updated_at: datetime | None, now: datetime) -> bool:
    return generator_live(live_started_ts) and updated_at is not None and updated_at >= now - ACTIVE_WINDOW


def _state_row(spark: SparkSession, cfg: Config):
    rows = spark.table(cfg.table("sim", "generator_state")).filter(F.col("generator_id") == "main").collect()
    return rows[0] if rows else None


def _skip_if_live(spark: SparkSession, cfg: Config, task: str) -> bool:
    row = _state_row(spark, cfg)
    if row is not None and generator_live(row.live_started_ts):
        print(f"{task}: generator is live since {row.live_started_ts}; ed_streaming owns this step - skipping")
        return True
    return False


def _refuse_if_streaming(spark: SparkSession, cfg: Config, task: str) -> None:
    row = _state_row(spark, cfg)
    if row is not None and streaming_active(row.live_started_ts, row.updated_at, utcnow()):
        raise RuntimeError(f"{task}: ed_streaming is running (generator state updated {row.updated_at}). "
                           "Pause it and cancel its run first (docs/runbook-plan2.md).")


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
    if _skip_if_live(spark, cfg, "backfill_generate"):
        return
    store = DeltaStore(spark, cfg)
    now = floor_to(utcnow(), 60)
    if store.load_state() is None:
        store.save_state(initial_state(now, cfg.backfill_days))
    paths = advance(store, load_reference(spark, cfg), cfg, now, live=False,
                    writer=LandingWriter(cfg.landing_path), persist_each_chunk=False)
    print(f"wrote {len(paths)} landing files up to {now}")


def backfill_ingest(spark: SparkSession, cfg: Config) -> None:
    if _skip_if_live(spark, cfg, "backfill_ingest"):
        return
    start_bronze(spark, cfg, available_now=True).awaitTermination()
    start_silver_visits(spark, cfg, available_now=True).awaitTermination()


def backfill_gold(spark: SparkSession, cfg: Config) -> None:
    if _skip_if_live(spark, cfg, "backfill_gold"):
        return
    state = DeltaStore(spark, cfg).load_state()
    if state is None:
        raise RuntimeError("generator state missing; run backfill-generate first")
    end = floor_to(state.last_emitted_ts, 300)
    gold_metrics.backfill_snapshots(spark, cfg, state.backfill_start_ts, end)
    gold_metrics.gold_tick(spark, cfg, end, gold_metrics.BaselineCache())
    print(f"gold history computed for ({state.backfill_start_ts}, {end}]")


def backfill_notes_ai(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "backfill_notes_ai")
    start_silver_notes_ai(spark, cfg, available_now=True).awaitTermination()


def refresh_drivers(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "refresh_drivers")
    gold_metrics.refresh_drivers(spark, cfg)


def backfill_narratives(spark: SparkSession, cfg: Config) -> None:
    _refuse_if_streaming(spark, cfg, "backfill_narratives")
    narrative_tick(spark, cfg, DatabricksAi(cfg.llm_endpoint))


def governance_checks(spark: SparkSession, cfg: Config) -> None:
    violations = run_checks(spark, cfg)
    for v in violations:
        print(f"VIOLATION {v.check}: {v.object} - {v.detail}")
    if violations:
        raise RuntimeError(f"{len(violations)} governance violation(s); see output above")
