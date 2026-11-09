import hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from pyspark.sql import functions as F

from ed_ops.config import Config
from ed_ops.delta import conform, table_def
from ed_ops.events_contract import EVENT_SCHEMA_DDL
from ed_ops.generator.events import LandingWriter, events_in_window
from ed_ops.governance.deid import deidentify
from ed_ops.pipeline.bronze import add_ingest_metadata
from ed_ops.pipeline.silver_visits import (
    FOLD_COLUMNS, fold_events, quarantine_rows, validate_events, visits_merge_sql,
)
from tests.gen_factories import make_reference, plan_hours

BRONZE_DDL = EVENT_SCHEMA_DDL + ", _rescued_data STRING, _source_file STRING, _ingest_ts TIMESTAMP, ingest_date DATE"
T = datetime(2026, 11, 9, 12)
SALT = "test-salt"
VID = "FAC001-202611091100-001"


def payload(arrival_mode=None, patient_name=None, mrn=None, dob=None, sex=None, phone=None, address=None,
            chief=None, esi=None, note=None, bed=None, provider=None, disposition=None):
    return (arrival_mode, patient_name, mrn, dob, sex, phone, address, chief, esi, note, bed, provider, disposition)


def ev(event_id, etype, ts, visit_id=VID, facility_id="FAC001", pl=None, rescued=None):
    return (event_id, etype, ts, ts, facility_id, visit_id, pl or payload(), rescued, "f.json", ts, ts.date() if ts else None)


ARRIVAL_PAYLOAD = payload("ambulance", "Ava Carter", "MRN-7000042", date(1958, 3, 14), "F", "312-555-0147",
                          ("42 Elm St", "Springfield", "IL", "62704"), "chest pain")
JOURNEY = [
    ev("e1", "ARRIVAL", T - timedelta(minutes=60), pl=ARRIVAL_PAYLOAD),
    ev("e2", "TRIAGE", T - timedelta(minutes=55), pl=payload(esi=3, note="note", chief="chest pain")),
    ev("e3", "BED_ASSIGNED", T - timedelta(minutes=40), pl=payload(bed="FAC001-B03")),
    ev("e4", "PROVIDER_GREET", T - timedelta(minutes=30), pl=payload(provider="FAC001-MD2")),
    ev("e5", "DISPOSITION", T - timedelta(minutes=10), pl=payload(disposition="ADMIT")),
    ev("e6", "DEPARTURE", T - timedelta(minutes=1)),
]


def _fold(spark, rows):
    df = spark.createDataFrame(rows, BRONZE_DDL)
    return {r["visit_id"]: r.asDict() for r in fold_events(deidentify(df, SALT)).collect()}


def test_validate_assigns_reason_codes(spark):
    rows = [
        ev("ok", "ARRIVAL", T, pl=ARRIVAL_PAYLOAD),
        ev("esi", "TRIAGE", T, pl=payload(esi=9)),
        ev("novisit", "TRIAGE", T, visit_id=None, pl=payload(esi=3)),
        ev("nofac", "TRIAGE", T, facility_id=None, pl=payload(esi=3)),
        ev("unknown", "TRIAGE", T, facility_id="FAC999", pl=payload(esi=3)),
        ev("type", "FOO", T),
        ev("rescued", "TRIAGE", T, pl=payload(esi=3), rescued='{"esi_level":"high"}'),
        ev("nots", "TRIAGE", None, pl=payload(esi=3)),
    ]
    valid, rejected = validate_events(spark.createDataFrame(rows, BRONZE_DDL), spark.createDataFrame([("FAC001",)], "facility_id STRING"))
    assert [r.event_id for r in valid.collect()] == ["ok"]
    reasons = {r.event_id: r.reason_code for r in rejected.collect()}
    assert reasons == {
        "esi": "invalid_esi", "novisit": "missing_visit_id", "nofac": "missing_facility_id",
        "unknown": "unknown_facility", "type": "invalid_event_type", "rescued": "unparseable",
        "nots": "missing_event_ts",
    }
    q = quarantine_rows(rejected)
    conform(q, table_def("silver", "ed_events_quarantine"))  # raises if the shape drifts
    assert {r.reason_code for r in q.collect()} == set(reasons.values())


def test_deidentify_structured_phi(spark):
    df = deidentify(spark.createDataFrame([JOURNEY[0], JOURNEY[1]], BRONZE_DDL), SALT)
    a = df.filter("event_type = 'ARRIVAL'").collect()[0]
    assert a.patient_name_redacted == "REDACTED"
    assert a.mrn_hash == hashlib.sha256(("MRN-7000042" + SALT).encode()).hexdigest()
    assert (a.birth_year, a.age_at_arrival, a.sex, a.phone_last4, a.city, a.state) == (1958, 68, "F", "0147", "Springfield", "IL")
    assert df.schema["payload"].dataType.fieldNames() == ["arrival_mode", "esi_level", "bed_id", "provider_id", "disposition"]
    t = df.filter("event_type = 'TRIAGE'").collect()[0]
    assert t.mrn_hash is None and t.payload.esi_level == 3


def test_fold_builds_visit_row(spark):
    v = _fold(spark, JOURNEY)[VID]
    assert v["arrival_ts"] == T - timedelta(minutes=60) and v["arrival_mode"] == "ambulance"
    assert v["esi_level"] == 3 and v["disposition"] == "ADMIT"
    assert v["greet_ts"] == T - timedelta(minutes=30) and v["departure_ts"] == T - timedelta(minutes=1)
    assert v["last_event_ts"] == T - timedelta(minutes=1)
    assert v["mrn_hash"] and v["patient_name_redacted"] == "REDACTED"


def test_fold_is_order_independent(spark):
    assert _fold(spark, JOURNEY) == _fold(spark, list(reversed(JOURNEY)))


def test_fold_lwbs_sets_departure(spark):
    rows = [JOURNEY[0], JOURNEY[1], ev("e9", "LWBS", T - timedelta(minutes=5))]
    v = _fold(spark, rows)[VID]
    assert v["lwbs_ts"] == v["departure_ts"] == T - timedelta(minutes=5)


def test_merge_sql_coalesces_every_stage():
    sql = visits_merge_sql("c.ed_silver.ed_visits", "src")
    for c in FOLD_COLUMNS:
        assert f"t.{c} = coalesce(t.{c}, s.{c})" in sql
    assert "t.last_event_ts = greatest(t.last_event_ts, s.last_event_ts)" in sql
    insert_cols = sql.split("INSERT (")[1].split(")")[0].split(", ")
    assert insert_cols == table_def("silver", "ed_visits").column_names


def test_generator_to_silver_contract_end_to_end(spark, tmp_path):
    ref = make_reference()
    start = datetime(2026, 11, 2, 18)
    plan = plan_hours(ref, start, 2)
    cfg = Config(dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)
    writer = LandingWriter(str(tmp_path))
    t = start
    while t < start + timedelta(hours=72):
        events = events_in_window(plan, ref, t, t + timedelta(hours=1), cfg)
        if events:
            writer(events, t + timedelta(hours=1))
        t += timedelta(hours=1)
    raw = spark.read.schema(EVENT_SCHEMA_DDL).json(f"{tmp_path}/*/*/*/*/*.json")
    bronze = add_ingest_metadata(raw).withColumn("_rescued_data", F.lit(None).cast("string"))
    conform(bronze, table_def("bronze", "ed_events"))
    facility_ids = spark.createDataFrame([(f,) for f in ref.facilities.facility_id], "facility_id STRING")
    valid, rejected = validate_events(bronze, facility_ids)
    assert rejected.count() == 0
    visits = {r.visit_id: r for r in fold_events(deidentify(valid, SALT)).collect()}
    assert len(visits) == len(plan)
    for p in plan.itertuples(index=False):
        v = visits[p.visit_id]
        assert v.arrival_ts == p.arrival_ts.to_pydatetime()
        assert v.departure_ts == p.departure_ts.to_pydatetime()
        assert v.esi_level == p.nurse_esi
        assert (v.lwbs_ts is None) == pd.isna(p.lwbs_ts)
