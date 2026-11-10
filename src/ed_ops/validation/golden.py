"""Hand-calculated golden scenario (FAC001 busy, FAC002 empty, at T). Unit tests use it, and ed_validate re-runs
it on the workspace runtime (serverless Photon) to prove the metric formulas give identical results there."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from ed_ops.metrics.snapshots import compute_snapshots

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
    # v9: arrived T-3, triaged only at T+2 (unknown ESI at T); its flag must not count yet.
    visit("v9", m(-3), esi=2, triage=m(2), undertriage=True),
]
GOLDEN_FACILITIES = [("FAC001", 20, 300), ("FAC002", 10, 100)]
GOLDEN_ROSTER = [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)]

_GEDWIN = math.log(0.25 / 0.75) + math.log(3.2)
_FAC001 = {
    "census": 5, "n_esi1": 1, "n_esi3": 1, "n_esi4": 1, "n_untriaged": 2, "waiting_now_n": 3, "in_bed_n": 2,
    "longest_current_wait_min": 40.0, "boarders_n": 1, "longest_boarder_h": 1.0, "vents_n": 1,
    "undertriage_flags_n": 1, "d2p_sample_n": 2, "d2p_mean_min": 45.0, "d2p_median_min": 45.0, "d2p_p90_min": 57.0,
    "arrivals_1h": 4, "ambulance_arrivals_1h": 1, "last_bedded_wait_h": 20 / 60, "lwbs_rate_24h": 1 / 3,
    "los_median_h_24h": 85 / 60, "boarding_median_h_24h": 2.0, "admit_rate_24h": 2 / 3, "attendings_on_duty": 2,
    "occupancy_ratio": 0.25, "acuity_ratio": 3.2, "edwin": 0.5 * (0.25 / 0.75) * 3.2, "edwin_band": "Active",
    "gedwin": _GEDWIN, "gedwin_band": "Active",
    "gedwin_delta_1h": _GEDWIN - (math.log(0.15 / 0.85) + math.log(10 / 3)),
    "nedocs": -20 + 85.8 * 0.25 + 600 / 300 + 13.4 + 0.93 * 1.0 + 5.64 * (20 / 60), "nedocs_level": 1,
}
_FAC002 = {
    "census": 0, "edwin": None, "gedwin": None, "gedwin_band": "Active", "nedocs": 0.0, "nedocs_level": 1,
    "dq_flags": ["no_attending_on_duty"], "longest_boarder_h": 0.0, "last_bedded_wait_h": 0.0,
}
EXPECTED: dict[tuple[str, str], object] = {
    **{("FAC001", k): v for k, v in _FAC001.items()},
    **{("FAC002", k): v for k, v in _FAC002.items()},
}


def visits_frame(spark: SparkSession, rows) -> DataFrame:
    return spark.createDataFrame(rows, VISIT_DDL)


def facilities_frame(spark: SparkSession, rows) -> DataFrame:
    """rows: (facility_id, ed_beds, inpatient_beds)."""
    full = [(fid, f"{fid} Hospital", "community", "Central", beds, inpatient) for fid, beds, inpatient in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, facility_name STRING, facility_type STRING, region STRING, ed_beds INT, inpatient_beds INT"
    )


def roster_frame(spark: SparkSession, rows) -> DataFrame:
    """rows: (facility_id, shift_start, shift_end, attendings_on_duty)."""
    full = [(fid, s, e, "day", n) for fid, s, e, n in rows]
    return spark.createDataFrame(
        full, "facility_id STRING, shift_start TIMESTAMP, shift_end TIMESTAMP, shift_name STRING, attendings_on_duty INT"
    )


def as_of_frame(spark: SparkSession, *times) -> DataFrame:
    return spark.createDataFrame([(t,) for t in times], "snapshot_ts TIMESTAMP")


def _close(actual, expected) -> bool:
    if isinstance(expected, float) and actual is not None:
        return math.isclose(float(actual), expected, rel_tol=1e-9, abs_tol=1e-9)
    return actual == expected


def check_golden(spark: SparkSession, expected: dict[tuple[str, str], object] = EXPECTED) -> list[str]:
    snaps = compute_snapshots(
        visits_frame(spark, SCENARIO_VISITS), roster_frame(spark, GOLDEN_ROSTER),
        facilities_frame(spark, GOLDEN_FACILITIES), as_of_frame(spark, T),
    )
    rows = {r["facility_id"]: r.asDict() for r in snaps.collect()}
    return [f"{fid}.{col}: expected {want}, got {rows[fid][col]}"
            for (fid, col), want in expected.items() if not _close(rows[fid][col], want)]
