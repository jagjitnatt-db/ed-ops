"""Turn planned journeys into landing events. Chaos (late, duplicate, malformed) is a deterministic
function of (seed, visit_id, event_type), so replays emit identical files."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta

import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.reference import ReferenceData
from ed_ops.timeutil import epoch_seconds

NAMESPACE = uuid.UUID("6f1c2b8e-4d0a-4f7e-9a51-3e1d5c7b2a90")
STAGES = (
    ("ARRIVAL", "arrival_ts"),
    ("TRIAGE", "triage_ts"),
    ("BED_ASSIGNED", "bed_assigned_ts"),
    ("PROVIDER_GREET", "greet_ts"),
    ("VENT_START", "vent_start_ts"),
    ("VENT_END", "vent_end_ts"),
    ("DISPOSITION", "disposition_ts"),
    ("LWBS", "lwbs_ts"),
    ("DEPARTURE", "departure_ts"),
)
MAX_EMIT_DELAY = timedelta(minutes=60)  # late (<= 20 min) + duplicate (<= 30 min) re-emission


def _u(seed: int, *parts: object) -> float:
    digest = hashlib.blake2b("|".join(str(p) for p in (seed, *parts)).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def event_id(visit_id: str, event_type: str, variant: str = "") -> str:
    return str(uuid.uuid5(NAMESPACE, f"{visit_id}|{event_type}|{variant}"))


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _opt(value):
    if value is None or pd.isna(value):
        return None
    return value.to_pydatetime() if isinstance(value, pd.Timestamp) else value


def _payload(event_type: str, row, ref: ReferenceData) -> dict:
    fac = ref.facility(row.facility_id)
    if event_type == "ARRIVAL":
        p = ref.patient(row.pool_id)
        return {
            "arrival_mode": row.arrival_mode,
            "patient_name": f"{p['first_name']} {p['last_name']}",
            "mrn": p["mrn"],
            "dob": p["dob"].isoformat(),
            "sex": p["sex"],
            "phone": p["phone"],
            "address": {"street": p["street"], "city": fac.city, "state": fac.state, "zip": p["zip"]},
            "chief_complaint_text": row.chief_complaint_text,
        }
    if event_type == "TRIAGE":
        return {"esi_level": int(row.nurse_esi), "triage_note": row.triage_note, "chief_complaint_text": row.chief_complaint_text}
    if event_type == "BED_ASSIGNED":
        return {"bed_id": f"{row.facility_id}-B{1 + int(_u(0, row.visit_id, 'bed') * fac.ed_beds):02d}"}
    if event_type == "PROVIDER_GREET":
        return {"provider_id": f"{row.facility_id}-MD{1 + int(_u(0, row.visit_id, 'md') * 9)}"}
    if event_type == "DISPOSITION":
        return {"disposition": row.disposition}
    return {}


def _corrupt(event: dict, kind: int) -> dict:
    bad = copy.deepcopy(event)
    bad["event_id"] = event_id(str(event["visit_id"]), event["event_type"], "malformed")
    if kind == 1:
        bad["facility_id"] = "FAC999"
    elif kind == 2 and bad["event_type"] == "TRIAGE":
        bad["payload"]["esi_level"] = 9
    else:
        bad["visit_id"] = None
    return bad


def schedule(row, ref: ReferenceData, cfg: Config) -> list[tuple[datetime, dict]]:
    lwbs = _opt(row.lwbs_ts)
    out: list[tuple[datetime, dict]] = []
    for etype, column in STAGES:
        ts = _opt(getattr(row, column))
        if ts is None or (etype == "DEPARTURE" and lwbs is not None):
            continue
        event = {
            "event_id": event_id(row.visit_id, etype),
            "event_type": etype,
            "event_ts": _iso(ts),
            "facility_id": row.facility_id,
            "visit_id": row.visit_id,
            "payload": _payload(etype, row, ref),
        }
        emit_at = ts
        if _u(cfg.seed, row.visit_id, etype, "late") < cfg.late_rate:
            emit_at = ts + timedelta(minutes=5 + int(_u(cfg.seed, row.visit_id, etype, "late_min") * 16))
        out.append((emit_at, event))
        if _u(cfg.seed, row.visit_id, etype, "dup") < cfg.dup_rate:
            out.append((emit_at + timedelta(minutes=1 + int(_u(cfg.seed, row.visit_id, etype, "dup_min") * 29)), event))
        if _u(cfg.seed, row.visit_id, etype, "bad") < cfg.malformed_rate:
            out.append((emit_at, _corrupt(event, int(_u(cfg.seed, row.visit_id, etype, "bad_kind") * 3))))
    return out


def events_in_window(plan: pd.DataFrame, ref: ReferenceData, start_exclusive: datetime, end_inclusive: datetime,
                     cfg: Config) -> list[dict]:
    if plan.empty:
        return []
    candidates = plan[(plan["arrival_ts"] <= end_inclusive) & (plan["departure_ts"] >= start_exclusive - MAX_EMIT_DELAY)]
    emitted = _iso(end_inclusive)
    events = []
    for row in candidates.itertuples(index=False):
        for emit_at, event in schedule(row, ref, cfg):
            if start_exclusive < emit_at <= end_inclusive:
                events.append({**event, "emitted_ts": emitted})
    events.sort(key=lambda e: (e["event_ts"], e["event_type"], e["event_id"]))
    return events


class LandingWriter:
    """Writes one JSON-lines file per window. Files on UC Volumes become visible only once closed."""

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")

    def __call__(self, events: list[dict], window_end: datetime) -> str:
        directory = f"{self.root}/{window_end:%Y/%m/%d/%H}"
        os.makedirs(directory, exist_ok=True)
        path = f"{directory}/tick_{epoch_seconds(window_end)}.json"
        with open(path, "w") as fh:
            for e in events:
                fh.write(json.dumps(e, separators=(",", ":")) + "\n")
        return path
