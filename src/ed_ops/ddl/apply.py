from __future__ import annotations

from pyspark.sql import SparkSession

from ed_ops.config import Config
from ed_ops.ddl.generate import ddl_statements
from ed_ops.ddl.metadata import CatalogDef, load_catalog_def


def apply_ddl(spark: SparkSession, cfg: Config, cat: CatalogDef | None = None) -> int:
    statements = ddl_statements(cfg, cat or load_catalog_def())
    for stmt in statements:
        try:
            spark.sql(stmt)
        except Exception as exc:
            raise RuntimeError(f"DDL statement failed:\n{stmt}") from exc
    return len(statements)
