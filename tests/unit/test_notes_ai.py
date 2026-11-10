from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from ed_ops.delta import table_def
from ed_ops.pipeline.silver_notes_ai import (
    notes_rows, parse_json_reply, parse_vitals, regex_redact, triage_events,
)
from tests.bronze_factories import BRONZE_DDL, ev, payload, triage
from tests.fake_ai import FakeAi

NOW = datetime(2026, 11, 9, 12)
SINCE = NOW - timedelta(days=2)
NOTE = ("Pat1 Tester (68F), DOB 03/14/1958, MRN-7000001. BIBA c/o crushing substernal chest pain x 40 min, "
        "diaphoretic. HR 118, BP 92/58, RR 24, SpO2 89% RA, T 37.1C. Pain 9/10. Callback 312-555-0001.")


def _rows(spark, events, ai=None):
    df = spark.createDataFrame(events, BRONZE_DDL)
    return {r.visit_id: r for r in notes_rows(df, ai or FakeAi(), SINCE).collect()}


def test_recent_note_is_masked_then_enriched(spark):
    r = _rows(spark, [triage("a", NOW - timedelta(minutes=5), NOTE, esi=4, chief="cp - pt Pat1 Tester, cb 312-555-0001")])["FAC001-a"]
    assert "Pat1" not in r.note_masked and "MRN-7000001" not in r.note_masked and "312-555-0001" not in r.note_masked
    assert "Pat1" not in r.chief_complaint_masked
    assert r.residual_phi_flag is False
    assert r.complaint_category == "cardiac"
    assert (r.heart_rate, r.bp_systolic, r.bp_diastolic, r.respiratory_rate, r.spo2, r.temperature_c, r.pain_score) == (
        118, 92, 58, 24, 89, 37.1, 9
    )
    assert r.ai_status == "ok" and r.suggested_esi == 1 and r.esi_rationale == "fake rationale"
    assert r.undertriage_flag is True  # nurse ESI 4 vs suggested 1
    assert r.ai_enriched is True and r.ai_attempts == 1 and r.ai_error is None


def test_residual_phi_missed_by_ai_mask_is_redacted_and_flagged(spark):
    r = _rows(spark, [triage("b", NOW, NOTE)], FakeAi(miss_phone=True))["FAC001-b"]
    assert r.residual_phi_flag is True
    assert "312-555-0001" not in r.note_masked and "[REDACTED]" in r.note_masked


def test_endpoint_error_and_unparseable_reply(spark):
    rows = _rows(spark, [triage("c", NOW, NOTE + " FAILME"), triage("d", NOW, NOTE + " GARBAGE")])
    assert rows["FAC001-c"].ai_status == "error" and rows["FAC001-c"].ai_error == "fake endpoint error"
    assert rows["FAC001-c"].suggested_esi is None and rows["FAC001-c"].undertriage_flag is None
    assert rows["FAC001-d"].ai_status == "parse_error" and "cannot help" in rows["FAC001-d"].ai_error
    assert rows["FAC001-d"].note_masked is not None  # masked text kept so the retry sweep can re-run enrichment


def test_old_notes_are_skipped_and_store_no_text(spark):
    r = _rows(spark, [triage("e", SINCE - timedelta(hours=1), NOTE)])["FAC001-e"]
    assert r.ai_status == "skipped" and r.ai_enriched is False
    assert r.note_masked is None and r.chief_complaint_masked is None and r.complaint_category is None


def test_output_matches_triage_notes_table(spark):
    df = spark.createDataFrame([triage("f", NOW, NOTE), triage("g", SINCE - timedelta(days=1), NOTE)], BRONZE_DDL)
    assert notes_rows(df, FakeAi(), SINCE).columns == table_def("silver", "triage_notes").column_names


def test_parse_vitals_units(spark):
    v = spark.createDataFrame(
        [("101.3F", "120 / 80", "9/10", "93%", "HR 102", "18", "2 days")],
        "temperature STRING, blood_pressure STRING, pain_score STRING, spo2 STRING, heart_rate STRING, respiratory_rate STRING, symptom_onset STRING",
    ).select(F.struct("*").alias("v"))
    out = v.select(*[c.alias(k) for k, c in parse_vitals(F.col("v")).items()]).collect()[0]
    assert (out.temperature_c, out.bp_systolic, out.bp_diastolic, out.pain_score, out.spo2, out.heart_rate) == (38.5, 120, 80, 9, 93, 102)
    assert out.respiratory_rate == 18 and out.symptom_onset == "2 days"


@pytest.mark.parametrize(
    "reply,esi",
    [
        ('{"suggested_esi": 2, "rationale": "x"}', 2),
        ('Here you go:\n```json\n{\n  "suggested_esi": 3,\n  "rationale": "y"\n}\n```', 3),
        ("no json here", None),
    ],
)
def test_parse_json_reply_tolerates_prose_and_fences(spark, reply, esi):
    r = spark.createDataFrame([(reply,)], "r STRING").select(parse_json_reply(F.col("r"), "suggested_esi INT, rationale STRING").alias("p")).collect()[0]
    assert (r.p.suggested_esi if r.p else None) == esi


def test_regex_redact_keeps_vitals(spark):
    text = "BP 120/80, pain 9/10, DOB 03/14/1958, ph 312.555.0199, MRN-7000123, a.b@x.org"
    out = spark.createDataFrame([(text,)], "t STRING").select(regex_redact(F.col("t")).alias("o")).collect()[0].o
    assert "120/80" in out and "9/10" in out
    assert "03/14/1958" not in out and "312.555.0199" not in out and "MRN-7000123" not in out and "a.b@x.org" not in out


def test_triage_events_keeps_valid_deduplicated_triage_only(spark):
    rows = [
        triage("t1", NOW, NOTE, visit_id="FAC001-v1"),
        triage("t1", NOW, NOTE, visit_id="FAC001-v1"),  # duplicate event
        triage("t2", NOW, NOTE, esi=9, visit_id="FAC001-v2"),  # invalid ESI
        triage("t3", NOW, NOTE, facility_id="FAC999", visit_id="FAC999-v3"),  # unknown facility
        ev("a1", "ARRIVAL", NOW, visit_id="FAC001-v1", pl=payload(arrival_mode="walk_in")),
    ]
    df = triage_events(spark.createDataFrame(rows, BRONZE_DDL), spark.createDataFrame([("FAC001",)], "facility_id STRING"))
    assert [r.visit_id for r in df.collect()] == ["FAC001-v1"]


def test_notes_merge_never_overwrites_ok_and_counts_attempts():
    from ed_ops.pipeline.silver_notes_ai import notes_merge_sql

    sql = notes_merge_sql("c.ed_silver.triage_notes", "src")
    assert "WHEN MATCHED AND (t.ai_status IS NULL OR t.ai_status <> 'ok') THEN UPDATE SET" in sql
    assert "t.ai_attempts = coalesce(t.ai_attempts, 0) + 1" in sql
    assert "t.ai_attempts = s.ai_attempts" not in sql
    insert_cols = sql.split("INSERT (")[1].split(")")[0].split(", ")
    assert insert_cols == table_def("silver", "triage_notes").column_names


def test_retry_candidates_and_successful_retry(spark):
    from ed_ops.pipeline.silver_notes_ai import retry_candidates, retry_rows

    df = spark.createDataFrame([triage("ok", NOW, NOTE), triage("err", NOW, NOTE + " FAILME"),
                                triage("bad", NOW, NOTE + " GARBAGE"), triage("old", SINCE - timedelta(days=1), NOTE)], BRONZE_DDL)
    notes = notes_rows(df, FakeAi(), SINCE)
    exhausted = notes.withColumn("ai_attempts", F.when(F.col("visit_id") == "FAC001-bad", F.lit(3)).otherwise(F.col("ai_attempts")))
    cands = retry_candidates(exhausted)
    assert [r.visit_id for r in cands.collect()] == ["FAC001-err"]  # ok, skipped and exhausted rows are not retried
    # The endpoint recovers: retrying the masked text now succeeds. (The FAILME marker is in the masked note too.)
    retried = {r.visit_id: r for r in retry_rows(cands, FakeAi(fail_marker=None)).collect()}
    assert retried["FAC001-err"].ai_status == "ok" and retried["FAC001-err"].suggested_esi == 1
    assert retry_rows(cands, FakeAi()).columns == table_def("silver", "triage_notes").column_names


@pytest.mark.parametrize("phi", ["(312) 555-0101", "3125550101", "MRN: 1234567", "mrn 4321", "MRN#88001", "1958-03-14", "3/14/58"])
def test_regex_backstop_catches_common_real_world_formats(spark, phi):
    text = f"HR 118, BP 120/80, SpO2 89% RA, T 37.1C, pain 9/10, onset x 40 min; id {phi}."
    out = spark.createDataFrame([(text,)], "t STRING").select(regex_redact(F.col("t")).alias("o")).collect()[0].o
    assert phi not in out and "[REDACTED]" in out
    for vital in ("HR 118", "BP 120/80", "SpO2 89%", "T 37.1C", "pain 9/10", "x 40 min"):
        assert vital in out
