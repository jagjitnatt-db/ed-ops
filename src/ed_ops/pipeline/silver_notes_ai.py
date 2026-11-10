"""bronze TRIAGE events -> silver.triage_notes (spec 9.1).

ai_mask runs first, followed by a deterministic regex post-check; classification, vitals extraction and the
suggested ESI only ever see masked text. Raw note text never reaches silver. Notes older than
ai_backfill_days are stored without text (ai_status = 'skipped'): regex alone cannot remove names.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from functools import reduce
import uuid

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ed_ops.ai import AiFunctions, DatabricksAi
from ed_ops.config import Config
from ed_ops.delta import conform, table_def
from ed_ops.pipeline.silver_visits import validate_events
from ed_ops.timeutil import utcnow

PHI_PATTERNS = (
    r"(?:\(\d{3}\)\s?|\b\d{3}[-.\s])\d{3}[-.\s]\d{4}\b",  # phone: 312-555-0101, 312.555.0101, (312) 555-0101
    r"\b\d{10}\b",  # phone without separators
    r"(?i)\bMRN[\s:#-]*\d{4,}",  # medical record number: MRN-7000001, MRN: 1234567, mrn 4321, MRN#88001
    r"(?<![/\d])\d{1,2}/\d{1,2}/\d{2,4}(?![/\d])",  # m/d/y dates (DOB), not path segments; 120/80 and 9/10 have one slash
    r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b",  # ISO dates
    r"[\w.+-]+@[\w-]+\.[A-Za-z]{2,}",  # email
)
REDACTED = "[REDACTED]"
ESI_SCHEMA = "suggested_esi INT, rationale STRING"
ESI_PROMPT = (
    "You are an emergency department triage assistant. Assign an Emergency Severity Index (ESI) level from "
    "1 (most acute) to 5 (least acute) for this triage note. Return ONLY a JSON object, no prose, no code fences, "
    'matching {"suggested_esi": <integer 1-5>, "rationale": <string, max 30 words>}. Triage note: '
)
MAX_AI_ATTEMPTS = 3
NOTE_KEYS = ["facility_id", "visit_id"]
VITAL_COLUMNS = ("heart_rate", "bp_systolic", "bp_diastolic", "respiratory_rate", "spo2", "temperature_c",
                 "pain_score", "symptom_onset")


def phi_hit(text: Column) -> Column:
    return F.coalesce(reduce(lambda a, b: a | b, [text.rlike(p) for p in PHI_PATTERNS]), F.lit(False))


def regex_redact(text: Column) -> Column:
    return reduce(lambda c, p: F.regexp_replace(c, p, REDACTED), PHI_PATTERNS, text)


def parse_json_reply(result: Column, schema: str) -> Column:
    return F.from_json(F.regexp_extract(result, r"(?s)(\{.*\})", 1), schema)


def _match(text: Column, pattern: str) -> Column:
    return F.nullif(F.regexp_extract(text, pattern, 1), F.lit(""))


def parse_vitals(v: Column) -> dict[str, Column]:
    number = r"(\d+(?:\.\d+)?)"
    temp = _match(v["temperature"], number).try_cast("double")
    return {
        "heart_rate": _match(v["heart_rate"], number).try_cast("int"),
        "bp_systolic": _match(v["blood_pressure"], r"(\d{2,3})\s*/\s*\d{2,3}").try_cast("int"),
        "bp_diastolic": _match(v["blood_pressure"], r"\d{2,3}\s*/\s*(\d{2,3})").try_cast("int"),
        "respiratory_rate": _match(v["respiratory_rate"], number).try_cast("int"),
        "spo2": _match(v["spo2"], number).try_cast("int"),
        "temperature_c": F.when(temp > 50, F.round((temp - 32) * 5 / 9, 1)).otherwise(temp),
        "pain_score": F.coalesce(_match(v["pain_score"], r"(\d+)\s*/\s*10"), _match(v["pain_score"], number)).try_cast("int"),
        "symptom_onset": v["symptom_onset"],
    }


def _typed(values: dict[str, Column]) -> list[Column]:
    """Every triage_notes column except ai_attempts/processed_at, typed; unspecified columns are typed nulls."""
    cols = [c for c in table_def("silver", "triage_notes").columns if c.name not in ("ai_attempts", "processed_at")]
    return [values.get(c.name, F.lit(None)).cast(c.type).alias(c.name) for c in cols]


def triage_events(batch_df: DataFrame, facility_ids: DataFrame) -> DataFrame:
    valid, _ = validate_events(batch_df.dropDuplicates(["event_id"]), facility_ids)
    return valid.filter(F.col("event_type") == "TRIAGE").dropDuplicates(NOTE_KEYS)


def mask_stage(events: DataFrame, ai: AiFunctions) -> DataFrame:
    df = events.select(
        *NOTE_KEYS,
        F.col("event_ts").alias("triage_ts"),
        F.col("payload.esi_level").alias("esi_level"),
        F.col("payload.triage_note").alias("_note"),
        F.col("payload.chief_complaint_text").alias("_complaint"),
    )
    df = df.withColumn("_m_note", ai.mask("_note")).withColumn("_m_complaint", ai.mask("_complaint"))
    return df.select(
        *NOTE_KEYS, "triage_ts", "esi_level",
        regex_redact(F.col("_m_note")).alias("note_masked"),
        regex_redact(F.col("_m_complaint")).alias("chief_complaint_masked"),
        (phi_hit(F.col("_m_note")) | phi_hit(F.col("_m_complaint"))).alias("residual_phi_flag"),
    )


def enrich_stage(masked: DataFrame, ai: AiFunctions) -> DataFrame:
    df = (
        masked.withColumn("complaint_category", ai.classify("note_masked"))
        .withColumn("_vitals", ai.extract("note_masked"))
        .withColumn("_prompt", F.concat(F.lit(ESI_PROMPT), F.col("note_masked")))
        .withColumn("_reply", ai.query("_prompt"))
    )
    df = df.withColumn("_esi", parse_json_reply(F.col("_reply.result"), ESI_SCHEMA))
    status = (
        F.when(F.col("_reply.errorMessage").isNotNull(), F.lit("error"))
        .when(F.col("_esi.suggested_esi").between(1, 5), F.lit("ok"))
        .otherwise(F.lit("parse_error"))
    )
    df = df.withColumn("ai_status", status)
    ok = F.col("ai_status") == "ok"
    vitals = parse_vitals(F.col("_vitals"))
    return df.select(
        *_typed(
            {
                "facility_id": F.col("facility_id"),
                "visit_id": F.col("visit_id"),
                "triage_ts": F.col("triage_ts"),
                "esi_level": F.col("esi_level"),
                "note_masked": F.col("note_masked"),
                "chief_complaint_masked": F.col("chief_complaint_masked"),
                "residual_phi_flag": F.col("residual_phi_flag"),
                "complaint_category": F.col("complaint_category"),
                **vitals,
                "suggested_esi": F.when(ok, F.col("_esi.suggested_esi")),
                "esi_rationale": F.when(ok, F.col("_esi.rationale")),
                "undertriage_flag": F.when(ok, (F.col("esi_level") - F.col("_esi.suggested_esi")) >= 2),
                "ai_enriched": F.lit(True),
                "ai_status": F.col("ai_status"),
                "ai_error": F.when(F.col("ai_status") == "error", F.col("_reply.errorMessage")).when(
                    F.col("ai_status") == "parse_error", F.substring(F.coalesce(F.col("_reply.result"), F.lit("")), 1, 500)
                ),
            }
        )
    )


def skipped_rows(events: DataFrame) -> DataFrame:
    return events.select(
        *_typed(
            {
                "facility_id": F.col("facility_id"),
                "visit_id": F.col("visit_id"),
                "triage_ts": F.col("event_ts"),
                "esi_level": F.col("payload.esi_level"),
                "residual_phi_flag": F.lit(False),
                "ai_enriched": F.lit(False),
                "ai_status": F.lit("skipped"),
            }
        )
    )


def notes_rows(events: DataFrame, ai: AiFunctions, enrich_since: datetime) -> DataFrame:
    since = F.lit(enrich_since).cast("timestamp")
    recent = enrich_stage(mask_stage(events.filter(F.col("event_ts") >= since), ai), ai)
    old = skipped_rows(events.filter(F.col("event_ts") < since))
    rows = recent.unionByName(old).withColumn("ai_attempts", F.lit(1)).withColumn("processed_at", F.current_timestamp())
    return conform(rows, table_def("silver", "triage_notes"))


def notes_merge_sql(target: str, source_view: str) -> str:
    """Insert new notes; update existing ones only while they are not 'ok' (a success is never overwritten)."""
    cols = table_def("silver", "triage_notes").column_names
    updates = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in NOTE_KEYS and c != "ai_attempts")
    return (
        f"MERGE INTO {target} AS t USING {source_view} AS s\n"
        "ON t.facility_id = s.facility_id AND t.visit_id = s.visit_id\n"
        "WHEN MATCHED AND (t.ai_status IS NULL OR t.ai_status <> 'ok') THEN UPDATE SET "
        f"{updates}, t.ai_attempts = coalesce(t.ai_attempts, 0) + 1\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
    )


def merge_notes(rows: DataFrame, cfg: Config) -> None:
    spark = rows.sparkSession
    view = f"triage_notes_src_{uuid.uuid4().hex[:8]}"
    rows.createOrReplaceTempView(view)
    try:
        spark.sql(notes_merge_sql(cfg.table("silver", "triage_notes"), view))
    finally:
        spark.catalog.dropTempView(view)


def process_notes_batch(batch_df: DataFrame, cfg: Config, ai: AiFunctions) -> None:
    spark = batch_df.sparkSession
    events = triage_events(batch_df, spark.table(cfg.table("ref", "facility")).select("facility_id"))
    if events.isEmpty():
        return
    merge_notes(notes_rows(events, ai, utcnow() - timedelta(days=cfg.ai_backfill_days)), cfg)


def start_silver_notes_ai(spark: SparkSession, cfg: Config, available_now: bool = False) -> StreamingQuery:
    endpoint = cfg.llm_endpoint
    writer = (
        spark.readStream.table(cfg.table("bronze", "ed_events"))
        .writeStream.foreachBatch(lambda df, _batch_id: process_notes_batch(df, cfg, DatabricksAi(endpoint)))
        .option("checkpointLocation", cfg.checkpoint("silver_notes_ai"))
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="60 seconds")
    return writer.start()


def retry_candidates(notes: DataFrame) -> DataFrame:
    return notes.filter(
        F.col("ai_status").isin("error", "parse_error")
        & (F.col("ai_attempts") < MAX_AI_ATTEMPTS)
        & F.col("note_masked").isNotNull()
    ).select(*NOTE_KEYS, "triage_ts", "esi_level", "note_masked", "chief_complaint_masked", "residual_phi_flag")


def retry_rows(candidates: DataFrame, ai: AiFunctions) -> DataFrame:
    rows = enrich_stage(candidates, ai).withColumn("ai_attempts", F.lit(1)).withColumn("processed_at", F.current_timestamp())
    return conform(rows, table_def("silver", "triage_notes"))


def ai_retry_tick(spark: SparkSession, cfg: Config, ai: AiFunctions) -> None:
    candidates = retry_candidates(spark.table(cfg.table("silver", "triage_notes")))
    if candidates.isEmpty():
        return
    merge_notes(retry_rows(candidates, ai), cfg)


def start_ai_retry(spark: SparkSession, cfg: Config) -> StreamingQuery:
    endpoint = cfg.llm_endpoint
    return (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .writeStream.foreachBatch(lambda df, _batch_id: ai_retry_tick(df.sparkSession, cfg, DatabricksAi(endpoint)))
        .trigger(processingTime=f"{cfg.ai_retry_minutes} minutes")
        .option("checkpointLocation", cfg.checkpoint("ai_retry_sweep"))
        .start()
    )
