"""The landing-event contract shared by the generator (writer) and bronze ingestion (reader)."""

EVENT_TYPES = (
    "ARRIVAL",
    "TRIAGE",
    "BED_ASSIGNED",
    "PROVIDER_GREET",
    "VENT_START",
    "VENT_END",
    "DISPOSITION",
    "LWBS",
    "DEPARTURE",
)

PAYLOAD_DDL = (
    "STRUCT<arrival_mode: STRING, patient_name: STRING, mrn: STRING, dob: DATE, sex: STRING, phone: STRING, "
    "address: STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>, chief_complaint_text: STRING, "
    "esi_level: INT, triage_note: STRING, bed_id: STRING, provider_id: STRING, disposition: STRING>"
)

EVENT_SCHEMA_DDL = (
    "event_id STRING, event_type STRING, event_ts TIMESTAMP, emitted_ts TIMESTAMP, "
    f"facility_id STRING, visit_id STRING, payload {PAYLOAD_DDL}"
)
