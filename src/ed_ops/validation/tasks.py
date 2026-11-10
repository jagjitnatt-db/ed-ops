"""ed_validate job tasks."""

from __future__ import annotations

from pyspark.sql import SparkSession

from ed_ops.config import LOGICAL_SCHEMAS, Config
from ed_ops.governance.checks import run_checks
from ed_ops.validation.golden import check_golden
from ed_ops.validation.pipeline import evaluate_pipeline, pipeline_metrics


def validate_golden(spark: SparkSession, cfg: Config) -> None:
    mismatches = check_golden(spark)
    for line in mismatches:
        print(f"GOLDEN MISMATCH {line}")
    if mismatches:
        raise RuntimeError(f"golden scenario: {len(mismatches)} mismatch(es)")
    print("golden scenario matches hand-calculated EDWIN/gEDWIN/NEDOCS on this runtime")


def validate_pipeline(spark: SparkSession, cfg: Config) -> None:
    metrics = pipeline_metrics(spark, cfg)
    print(f"pipeline metrics: {metrics}")
    failures = evaluate_pipeline(metrics, cfg) + [f"{v.check}: {v.object} - {v.detail}" for v in run_checks(spark, cfg)]
    for f in failures:
        print(f"FAIL {f}")
    if failures:
        raise RuntimeError(f"ed_validate: {len(failures)} failure(s)")
    print("ed_validate: all pipeline and governance assertions passed")


def teardown_schemas(spark: SparkSession, cfg: Config) -> None:
    if "validate" not in cfg.schema_prefix:
        raise RuntimeError(f"refusing to drop schemas for prefix {cfg.schema_prefix!r}: only validate prefixes")
    for logical in LOGICAL_SCHEMAS:
        spark.sql(f"DROP SCHEMA IF EXISTS {cfg.schema(logical)} CASCADE")
    print(f"dropped {cfg.schema_prefix}* schemas")
