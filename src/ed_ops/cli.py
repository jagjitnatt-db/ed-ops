"""Console-script entry points used by the bundle's python_wheel_task (named parameters -> --key=value)."""

from __future__ import annotations

import os
import sys
import time
from typing import Callable

from pyspark.sql import SparkSession

from ed_ops import setup, streaming
from ed_ops.validation import tasks as validation
from ed_ops.config import Config, parse_config


def _run(task: Callable[[SparkSession, Config], None]) -> None:
    # Project convention is naive UTC; PySpark converts timestamps via the driver's OS time zone.
    os.environ["TZ"] = "UTC"
    time.tzset()
    cfg, rest = parse_config(sys.argv[1:])
    if rest:
        raise SystemExit(f"unknown arguments: {rest}")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    task(spark, cfg)


def create_objects() -> None:
    _run(setup.create_objects)


def seed_reference() -> None:
    _run(setup.seed_reference)


def backfill_generate() -> None:
    _run(setup.backfill_generate)


def backfill_ingest() -> None:
    _run(setup.backfill_ingest)


def backfill_gold() -> None:
    _run(setup.backfill_gold)


def stream_generator() -> None:
    _run(streaming.run_generator)


def stream_bronze() -> None:
    _run(streaming.run_bronze)


def stream_silver_visits() -> None:
    _run(streaming.run_silver_visits)


def stream_gold_metrics() -> None:
    _run(streaming.run_gold_metrics)


def backfill_notes_ai() -> None:
    _run(setup.backfill_notes_ai)


def refresh_drivers() -> None:
    _run(setup.refresh_drivers)


def backfill_narratives() -> None:
    _run(setup.backfill_narratives)


def governance_checks() -> None:
    _run(setup.governance_checks)


def stream_notes_ai() -> None:
    _run(streaming.run_notes_ai)


def stream_ai_retry() -> None:
    _run(streaming.run_ai_retry)


def stream_narratives() -> None:
    _run(streaming.run_narratives)


def validate_golden() -> None:
    _run(validation.validate_golden)


def validate_pipeline() -> None:
    _run(validation.validate_pipeline)


def teardown_schemas() -> None:
    _run(validation.teardown_schemas)
