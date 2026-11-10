"""bronze.ed_events -> silver.ed_visits: validate, de-identify, fold per visit, idempotent COALESCE MERGE.
Duplicates are removed within each micro-batch; across batches the MERGE is idempotent (plan deviation 1)."""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.config import Config
from ed_ops.delta import append_into
from ed_ops.events_contract import EVENT_TYPES
from ed_ops.governance.deid import DEID_COLUMNS, deidentify, salt_expr

REASON_CODES = (
    "unparseable", "missing_event_ts", "missing_facility_id", "missing_visit_id", "invalid_event_type",
    "invalid_esi", "unknown_facility",
)
STAGE_COLUMNS = (
    "arrival_ts", "arrival_mode", "esi_level", "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts",
    "vent_end_ts", "disposition", "disposition_ts", "lwbs_ts", "departure_ts",
)
FOLD_COLUMNS = STAGE_COLUMNS + DEID_COLUMNS


def validate_events(events: DataFrame, facility_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    known = F.broadcast(facility_ids.select("facility_id").distinct().withColumn("_known_facility", F.lit(True)))
    df = events.join(known, "facility_id", "left")
    esi = F.col("payload.esi_level")
    event_type = F.col("event_type")
    reason = (
        F.when(F.col("_rescued_data").isNotNull(), F.lit("unparseable"))
        .when(F.col("event_ts").isNull(), F.lit("missing_event_ts"))
        .when(F.col("facility_id").isNull(), F.lit("missing_facility_id"))
        .when(F.col("visit_id").isNull(), F.lit("missing_visit_id"))
        .when(event_type.isNull() | ~event_type.isin(*EVENT_TYPES), F.lit("invalid_event_type"))
        .when((event_type == "TRIAGE") & (esi.isNull() | ~esi.between(1, 5)), F.lit("invalid_esi"))
        .when(F.col("_known_facility").isNull(), F.lit("unknown_facility"))
    )
    df = df.withColumn("reason_code", reason)
    valid = df.filter(F.col("reason_code").isNull()).drop("_known_facility", "reason_code")
    rejected = df.filter(F.col("reason_code").isNotNull()).drop("_known_facility")
    return valid, rejected


def quarantine_rows(rejected: DataFrame) -> DataFrame:
    return rejected.select(
        "event_id", "event_type", "event_ts", "emitted_ts", "facility_id", "visit_id", "reason_code",
        F.col("payload.esi_level").alias("esi_level"),
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("quarantined_at"),
        "ingest_date",
    )


def _first(event_type: str) -> F.Column:
    return F.min(F.when(F.col("event_type") == event_type, F.col("event_ts")))


def _value(event_type: str, column: str) -> F.Column:
    return F.max(F.when(F.col("event_type") == event_type, F.col(column)))


def fold_events(events: DataFrame) -> DataFrame:
    """One partial visit row per (facility_id, visit_id); expects deidentify() output."""
    return (
        events.groupBy("facility_id", "visit_id")
        .agg(
            _first("ARRIVAL").alias("arrival_ts"),
            _value("ARRIVAL", "payload.arrival_mode").alias("arrival_mode"),
            _value("TRIAGE", "payload.esi_level").alias("esi_level"),
            _first("TRIAGE").alias("triage_ts"),
            _first("BED_ASSIGNED").alias("bed_assigned_ts"),
            _first("PROVIDER_GREET").alias("greet_ts"),
            _first("VENT_START").alias("vent_start_ts"),
            _first("VENT_END").alias("vent_end_ts"),
            _value("DISPOSITION", "payload.disposition").alias("disposition"),
            _first("DISPOSITION").alias("disposition_ts"),
            _first("LWBS").alias("lwbs_ts"),
            _first("DEPARTURE").alias("_departure_ts"),
            *[F.max(c).alias(c) for c in DEID_COLUMNS],
            F.max("event_ts").alias("last_event_ts"),
        )
        .withColumn("departure_ts", F.coalesce(F.col("_departure_ts"), F.col("lwbs_ts")))
        .select("facility_id", "visit_id", *FOLD_COLUMNS, "last_event_ts")
    )


def visits_merge_sql(target: str, source_view: str) -> str:
    sets = ",\n  ".join(f"t.{c} = coalesce(t.{c}, s.{c})" for c in FOLD_COLUMNS)
    columns = ["facility_id", "visit_id", *FOLD_COLUMNS, "last_event_ts", "updated_at"]
    values = ", ".join("current_timestamp()" if c == "updated_at" else f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        "ON t.facility_id = s.facility_id AND t.visit_id = s.visit_id\n"
        f"WHEN MATCHED THEN UPDATE SET\n  {sets},\n"
        "  t.last_event_ts = greatest(t.last_event_ts, s.last_event_ts),\n"
        "  t.updated_at = current_timestamp()\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)})\n"
        f"VALUES ({values})"
    )


def process_batch(batch_df: DataFrame, cfg: Config) -> None:
    spark = batch_df.sparkSession
    events = batch_df.dropDuplicates(["event_id"])
    facility_ids = spark.table(cfg.table("ref", "facility")).select("facility_id")
    valid, rejected = validate_events(events, facility_ids)
    if not rejected.isEmpty():
        append_into(quarantine_rows(rejected), cfg, "silver", "ed_events_quarantine")
    view = f"silver_visits_src_{uuid.uuid4().hex[:8]}"
    fold_events(deidentify(valid, salt_expr(cfg))).createOrReplaceTempView(view)
    try:
        spark.sql(visits_merge_sql(cfg.table("silver", "ed_visits"), view))
    finally:
        spark.catalog.dropTempView(view)


def start_silver_visits(spark: SparkSession, cfg: Config, available_now: bool = False) -> StreamingQuery:
    writer = (
        spark.readStream.table(cfg.table("bronze", "ed_events"))
        .writeStream.foreachBatch(lambda df, _batch_id: process_batch(df, cfg))
        .option("checkpointLocation", cfg.checkpoint("silver_visits"))
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="30 seconds")
    return writer.start()
