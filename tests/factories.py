"""Test-data builders. All datetimes are naive UTC."""

from datetime import datetime, timedelta

T = datetime(2026, 11, 9, 12, 0)


def m(minutes: float) -> datetime:
    """T shifted by `minutes` (negative = before T)."""
    return T + timedelta(minutes=minutes)


VISIT_DDL = (
    "facility_id STRING, visit_id STRING, arrival_ts TIMESTAMP, arrival_mode STRING, esi_level INT, "
    "triage_ts TIMESTAMP, bed_assigned_ts TIMESTAMP, greet_ts TIMESTAMP, vent_start_ts TIMESTAMP, "
    "vent_end_ts TIMESTAMP, disposition STRING, disposition_ts TIMESTAMP, lwbs_ts TIMESTAMP, "
    "departure_ts TIMESTAMP, suggested_esi INT, undertriage_flag BOOLEAN, complaint_category STRING"
)


def visit(visit_id, arrival, *, facility_id="FAC001", esi=None, triage=None, bed=None, greet=None,
          vent_start=None, vent_end=None, disposition=None, disposition_ts=None, lwbs=None,
          departure=None, mode="walk_in", undertriage=None):
    return (facility_id, visit_id, arrival, mode, esi, triage, bed, greet, vent_start, vent_end,
            disposition, disposition_ts, lwbs, departure, None, undertriage, None)


def visits_df(spark, rows):
    return spark.createDataFrame(rows, VISIT_DDL)


def facilities_df(spark, rows):
    """rows: (facility_id, ed_beds, inpatient_beds)."""
    full = [(fid, f"{fid} Hospital", "community", "Central", beds, inpatient) for fid, beds, inpatient in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, facility_name STRING, facility_type STRING, region STRING, ed_beds INT, inpatient_beds INT"
    )


def roster_df(spark, rows):
    """rows: (facility_id, shift_start, shift_end, attendings_on_duty)."""
    full = [(fid, s, e, "day", n) for fid, s, e, n in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, shift_start TIMESTAMP, shift_end TIMESTAMP, shift_name STRING, attendings_on_duty INT"
    )


def as_of_df(spark, *times):
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


# Hand-built scenario for FAC001 at T (20 ED beds, 300 inpatient beds, 2 attendings 07:00-15:00 UTC).
SCENARIO_VISITS = [
    # v1: ESI1, bedded, greeted, ventilated, admitted at T-60 and still boarding.
    visit("v1", m(-120), esi=1, triage=m(-118), bed=m(-115), greet=m(-110), vent_start=m(-100),
          disposition="ADMIT", disposition_ts=m(-60)),
    # v2: ESI3 ambulance, bedded T-30, greeted T-20 (d2p 30 min), flagged under-triaged.
    visit("v2", m(-50), esi=3, triage=m(-45), bed=m(-30), greet=m(-20), mode="ambulance", undertriage=True),
    # v3: ESI4 waiting since T-40.
    visit("v3", m(-40), esi=4, triage=m(-35)),
    # v4: arrived T-10, not triaged yet.
    visit("v4", m(-10)),
    # v5: ESI5 greeted T-30 (d2p 60 min), discharged, departed T-5 (LOS 85 min).
    visit("v5", m(-90), esi=5, triage=m(-85), greet=m(-30), disposition="DISCHARGE", disposition_ts=m(-15),
          departure=m(-5)),
    # v6: ESI3 left without being seen at T-200.
    visit("v6", m(-300), esi=3, triage=m(-295), lwbs=m(-200), departure=m(-200)),
    # v7: ESI2 admitted T-150, departed T-30 (boarding 2 h); greet outside the 1 h window.
    visit("v7", m(-200), esi=2, triage=m(-195), greet=m(-190), bed=m(-180), disposition="ADMIT",
          disposition_ts=m(-150), departure=m(-30)),
    # v8: arrives in the future.
    visit("v8", m(10), esi=3, triage=m(12)),
    # v9: arrived T-3, triaged only at T+2 (unknown ESI at T), flag must not count yet.
    visit("v9", m(-3), esi=2, triage=m(2), undertriage=True),
]
