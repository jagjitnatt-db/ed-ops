"""bronze TRIAGE events -> silver.triage_notes (spec 9.1).

ai_mask runs first, followed by a deterministic regex post-check; classification, vitals extraction and the
suggested ESI only ever see masked text. Raw note text never reaches silver. Notes older than
ai_backfill_days are stored without text (ai_status = 'skipped'): regex alone cannot remove names.
"""

from __future__ import annotations

from datetime import datetime
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ed_ops.ai import AiFunctions
from ed_ops.delta import conform, table_def
from ed_ops.pipeline.silver_visits import validate_events

PHI_PATTERNS = (
    r"\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b",  # phone
    r"MRN-?\d{5,}",  # medical record number
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",  # full date (DOB); vitals like 120/80 and 9/10 have one slash
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
