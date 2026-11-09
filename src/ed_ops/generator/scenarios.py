"""Scenario windows that raise arrivals (and acuity) for chosen facilities (spec section 6)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from ed_ops.config import Config
from ed_ops.generator.state import GeneratorState

FLU_START_DAY, FLU_END_DAY, FLU_MULTIPLIER = 8, 11, 1.4  # backfill days 9-11
LIVE_SURGE_MULTIPLIER = 2.0


@dataclass(frozen=True)
class ScenarioWindow:
    name: str
    start: datetime
    end: datetime
    facility_ids: frozenset[str]
    multiplier: float
    acuity_shift: bool

    def active(self, facility_id: str, ts: datetime) -> bool:
        return facility_id in self.facility_ids and self.start <= ts < self.end


def build_scenarios(facilities: pd.DataFrame, state: GeneratorState, cfg: Config) -> list[ScenarioWindow]:
    surge_prone = sorted(facilities.loc[facilities["scenario_profile"] == "surge_prone", "facility_id"])
    if not surge_prone:
        return []
    flu_region = facilities.loc[facilities["facility_id"] == surge_prone[0], "region"].iloc[0]
    flu_ids = frozenset(surge_prone) | frozenset(facilities.loc[facilities["region"] == flu_region, "facility_id"])
    windows = [
        ScenarioWindow(
            "flu_surge",
            state.backfill_start_ts + timedelta(days=FLU_START_DAY),
            state.backfill_start_ts + timedelta(days=FLU_END_DAY),
            flu_ids,
            FLU_MULTIPLIER,
            True,
        )
    ]
    if state.live_started_ts is not None:
        start = state.live_started_ts + timedelta(minutes=cfg.live_surge_offset_min)
        windows.append(
            ScenarioWindow(
                "live_surge",
                start,
                start + timedelta(minutes=cfg.live_surge_duration_min),
                frozenset(surge_prone[:2]),
                LIVE_SURGE_MULTIPLIER,
                False,
            )
        )
    return windows


def arrival_multiplier(windows: list[ScenarioWindow], facility_id: str, ts: datetime) -> float:
    result = 1.0
    for w in windows:
        if w.active(facility_id, ts):
            result *= w.multiplier
    return result


def acuity_shift_active(windows: list[ScenarioWindow], facility_id: str, ts: datetime) -> bool:
    return any(w.acuity_shift and w.active(facility_id, ts) for w in windows)
