"""Builders for bronze.ed_events-shaped rows (shared by silver tests)."""

from ed_ops.events_contract import EVENT_SCHEMA_DDL

BRONZE_DDL = EVENT_SCHEMA_DDL + ", _rescued_data STRING, _source_file STRING, _ingest_ts TIMESTAMP, ingest_date DATE"


def payload(arrival_mode=None, patient_name=None, mrn=None, dob=None, sex=None, phone=None, address=None,
            chief=None, esi=None, note=None, bed=None, provider=None, disposition=None):
    return (arrival_mode, patient_name, mrn, dob, sex, phone, address, chief, esi, note, bed, provider, disposition)


def ev(event_id, etype, ts, visit_id="FAC001-202611091100-001", facility_id="FAC001", pl=None, rescued=None):
    return (event_id, etype, ts, ts, facility_id, visit_id, pl or payload(), rescued, "f.json", ts, ts.date() if ts else None)


def triage(event_id, ts, note, esi=3, chief="chest pain", visit_id=None, facility_id="FAC001"):
    return ev(event_id, "TRIAGE", ts, visit_id=visit_id or f"{facility_id}-{event_id}", facility_id=facility_id,
              pl=payload(esi=esi, note=note, chief=chief))
