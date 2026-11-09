"""Auto Loader ingestion of landing JSON into bronze.ed_events (append-only)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.events_contract import EVENT_SCHEMA_DDL


def add_ingest_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("_ingest_ts")))
    )


def start_bronze(spark: SparkSession, cfg: Config, available_now: bool = False) -> StreamingQuery:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA_DDL)
        .load(cfg.landing_path)
    )
    writer = add_ingest_metadata(stream).writeStream.option("checkpointLocation", cfg.checkpoint("bronze"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="30 seconds")
    return writer.toTable(cfg.table("bronze", "ed_events"))
