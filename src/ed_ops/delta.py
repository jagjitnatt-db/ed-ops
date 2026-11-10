"""Typed writes into metadata-defined Delta tables."""

from __future__ import annotations

import uuid
from functools import lru_cache

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.ddl.metadata import TableDef, load_catalog_def


@lru_cache(maxsize=None)
def table_def(logical: str, name: str) -> TableDef:
    return load_catalog_def().table(logical, name)


def conform(df: DataFrame, t: TableDef) -> DataFrame:
    missing = [c for c in t.column_names if c not in df.columns]
    if missing:
        raise ValueError(f"{t.schema}.{t.name}: DataFrame is missing columns {missing}")
    return df.select(*[F.col(c.name).cast(c.type).alias(c.name) for c in t.columns])


def merge_sql(target: str, source_view: str, keys: list[str], columns: list[str]) -> str:
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    updates = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def merge_into(df: DataFrame, cfg: Config, logical: str, name: str, keys: list[str] | None = None) -> None:
    t = table_def(logical, name)
    keys = list(keys or t.primary_key)
    if not keys:
        raise ValueError(f"{logical}.{name} has no primary key; pass keys explicitly")
    view = f"src_{name}_{uuid.uuid4().hex[:8]}"
    conform(df, t).createOrReplaceTempView(view)
    spark = df.sparkSession
    try:
        spark.sql(merge_sql(cfg.table(logical, name), view, keys, t.column_names))
    finally:
        spark.catalog.dropTempView(view)


def txn_options(cfg: Config, app: str, version: int) -> dict[str, str]:
    """Delta idempotent-write options. The checkpoint version is part of the app id because a checkpoint reset
    restarts micro-batch ids at 0."""
    return {"txnAppId": f"{cfg.schema_prefix}{app}_v{cfg.checkpoint_version}", "txnVersion": str(version)}


def append_into(df: DataFrame, cfg: Config, logical: str, name: str, txn: tuple[str, int] | None = None) -> None:
    writer = conform(df, table_def(logical, name)).write.mode("append")
    if txn is not None:
        writer = writer.options(**txn_options(cfg, *txn))
    writer.saveAsTable(cfg.table(logical, name))
