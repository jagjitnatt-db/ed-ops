"""Plan each visit's full journey at arrival, with durations driven by current load (spec section 6)."""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from ed_ops.generator import curves
from ed_ops.generator.notes_templates import pick_archetype, render_note
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.scenarios import ScenarioWindow, acuity_shift_active, arrival_multiplier
from ed_ops.timeutil import to_local

PLAN_COLUMNS = (
    "facility_id", "visit_id", "pool_id", "arrival_ts", "arrival_mode", "true_esi", "nurse_esi", "archetype",
    "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition", "disposition_ts",
    "lwbs_ts", "departure_ts", "triage_note", "chief_complaint_text", "planned_at",
)
TS_COLUMNS = (
    "arrival_ts", "triage_ts", "bed_assigned_ts", "greet_ts", "vent_start_ts", "vent_end_ts", "disposition_ts",
    "lwbs_ts", "departure_ts", "planned_at",
)
INT_COLUMNS = ("pool_id", "true_esi", "nurse_esi")
UNDERTRIAGE_P = 0.03
MAX_BOARDING_MIN = 48 * 60.0


def normalize_plan(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reindex(columns=list(PLAN_COLUMNS))
    for c in TS_COLUMNS:
        df[c] = pd.to_datetime(df[c])
    for c in INT_COLUMNS:
        df[c] = df[c].astype("int64")
    return df.reset_index(drop=True)


def empty_plan() -> pd.DataFrame:
    return normalize_plan(pd.DataFrame(columns=list(PLAN_COLUMNS)))


def census_at(plan: pd.DataFrame, ts: datetime) -> dict[str, int]:
    if plan.empty:
        return {}
    mask = (plan["arrival_ts"] <= ts) & (plan["departure_ts"] > ts)
    return {k: int(v) for k, v in plan.loc[mask].groupby("facility_id").size().items()}


def load_factor(occupancy: float) -> float:
    return 1.0 + 3.0 * max(0.0, occupancy - 0.7) + 10.0 * max(0.0, occupancy - 1.0)


def staffing_factor(patients_per_attending: float) -> float:
    return max(1.0, patients_per_attending / 6.0) ** 0.7


def _lognormal(rng: np.random.Generator, median: float, sigma: float) -> float:
    return float(median * math.exp(sigma * rng.standard_normal()))


def _mins(m: float) -> timedelta:
    return timedelta(minutes=m)


def _pick_disposition(rng: np.random.Generator, facility_type: str, true_esi: int) -> str:
    p_admit = min(0.95, curves.ADMIT_P[true_esi] * curves.ADMIT_TYPE_FACTOR[facility_type])
    p_transfer = 0.10 if (facility_type == "rural_critical_access" and true_esi <= 2) else 0.01
    u = rng.random()
    if u < p_admit:
        return "ADMIT"
    if u < p_admit + p_transfer:
        return "TRANSFER"
    return "DISCHARGE"


def plan_visit(rng, fac, visit_id: str, arrival: datetime, occupancy: float, patients_per_attending: float,
               acuity_shift: bool, patient: dict, pool_id: int, planned_at: datetime) -> dict:
    true_esi = int(rng.choice(5, p=curves.esi_mix(fac.facility_type, acuity_shift))) + 1
    nurse_esi = true_esi
    if rng.random() < UNDERTRIAGE_P:
        true_esi = int(rng.choice([1, 2], p=[0.25, 0.75]))
        nurse_esi = int(rng.choice([4, 5], p=[0.7, 0.3]))
    archetype = pick_archetype(rng, patient["sex"])
    arrival_mode = "ambulance" if rng.random() < curves.AMBULANCE_P[true_esi] else "walk_in"
    note, complaint = render_note(rng, archetype, true_esi, patient, arrival)

    speed = curves.PROFILE_SPEED[fac.scenario_profile]
    lf, sf = load_factor(occupancy), staffing_factor(patients_per_attending)
    d2t = _lognormal(rng, curves.D2T_MEDIAN[nurse_esi] * (0.5 if arrival_mode == "ambulance" else 1.0), 0.5)
    triage = arrival + _mins(max(0.5, d2t))
    d2p = _lognormal(rng, curves.D2P_MEDIAN[nurse_esi] * lf * sf * speed, 0.6)
    greet = max(triage + _mins(1), arrival + _mins(d2p))
    patience = _lognormal(rng, curves.PATIENCE_MEDIAN, 0.6)

    row = {c: None for c in PLAN_COLUMNS}
    row.update(
        facility_id=fac.facility_id, visit_id=visit_id, pool_id=int(pool_id), arrival_ts=arrival,
        arrival_mode=arrival_mode, true_esi=true_esi, nurse_esi=nurse_esi, archetype=archetype,
        triage_ts=triage, triage_note=note, chief_complaint_text=complaint, planned_at=planned_at,
    )
    if nurse_esi >= 3 and (greet - arrival).total_seconds() / 60 > patience:
        lwbs = max(triage + _mins(5), arrival + _mins(patience))
        row.update(lwbs_ts=lwbs, departure_ts=lwbs)
        return _whole_seconds(row)

    bed = triage + _mins(_lognormal(rng, curves.BED_WAIT_MEDIAN * lf * speed, 0.7))
    treat = _lognormal(rng, curves.TREAT_MEDIAN[true_esi] * (1.0 + 0.3 * max(0.0, occupancy - 0.8)), 0.5)
    disposition_ts = max(greet, bed) + _mins(treat)
    disposition = _pick_disposition(rng, fac.facility_type, true_esi)
    if disposition == "DISCHARGE":
        after = _lognormal(rng, curves.DISCHARGE_TO_DEPART_MEDIAN, 0.6)
    elif disposition == "TRANSFER":
        after = _lognormal(rng, curves.TRANSFER_TO_DEPART_MEDIAN, 0.5)
    else:
        after = min(_lognormal(rng, curves.boarding_median(fac.scenario_profile), 0.7), MAX_BOARDING_MIN)
    departure = disposition_ts + _mins(max(1.0, after))
    vent_start = vent_end = None
    if rng.random() < curves.VENT_P.get(true_esi, 0.0):
        start = bed + _mins(float(rng.uniform(5, 30)))
        end = departure - _mins(5)
        if end > start:
            vent_start, vent_end = start, end
    row.update(
        bed_assigned_ts=bed, greet_ts=greet, disposition=disposition, disposition_ts=disposition_ts,
        departure_ts=departure, vent_start_ts=vent_start, vent_end_ts=vent_end,
    )
    return _whole_seconds(row)


def _whole_seconds(row: dict) -> dict:
    for c in TS_COLUMNS:
        if row[c] is not None:
            row[c] = row[c].replace(microsecond=0)
    return row


def plan_arrivals(rng: np.random.Generator, ref: ReferenceData, plan: pd.DataFrame, start: datetime, end: datetime,
                  scenarios: list[ScenarioWindow], tz_name: str) -> pd.DataFrame:
    seconds = int((end - start).total_seconds())
    if seconds < 2:
        return empty_plan()
    mid = start + (end - start) / 2
    local = to_local(mid, tz_name)
    census = census_at(plan, start)
    rows = []
    for fac in ref.facilities.itertuples(index=False):
        lam = (
            curves.arrivals_per_minute(fac.baseline_daily_visits, local.hour, local.weekday())
            * arrival_multiplier(scenarios, fac.facility_id, mid)
            * seconds / 60.0
        )
        n = int(rng.poisson(lam))
        if n == 0:
            continue
        in_ed = census.get(fac.facility_id, 0)
        occupancy = in_ed / fac.ed_beds
        ppa = in_ed / max(1, ref.attendings_at(fac.facility_id, start))
        shift = acuity_shift_active(scenarios, fac.facility_id, mid)
        seq: Counter[str] = Counter()
        for offset in np.sort(rng.integers(1, seconds, n)):
            arrival = start + timedelta(seconds=int(offset))
            key = arrival.strftime("%Y%m%d%H%M")
            seq[key] += 1
            pool_id = int(rng.integers(0, ref.pool_size))
            patient = {**ref.patient(pool_id), "city": fac.city, "state": fac.state}
            rows.append(
                plan_visit(rng, fac, f"{fac.facility_id}-{key}-{seq[key]:03d}", arrival, occupancy, ppa, shift,
                           patient, pool_id, end)
            )
    if not rows:
        return empty_plan()
    return normalize_plan(pd.DataFrame(rows, columns=list(PLAN_COLUMNS)))
