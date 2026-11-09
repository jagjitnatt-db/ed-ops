"""Arrival curves, acuity mixes and journey duration medians (minutes) for the simulator."""

from __future__ import annotations

import numpy as np

_HOURLY_RAW = (0.55, 0.45, 0.38, 0.33, 0.30, 0.32, 0.45, 0.70, 0.95, 1.15, 1.30, 1.38,
               1.40, 1.38, 1.35, 1.33, 1.32, 1.30, 1.25, 1.15, 1.02, 0.90, 0.78, 0.65)
HOURLY = tuple(h / (sum(_HOURLY_RAW) / 24) for h in _HOURLY_RAW)

_DOW_RAW = (1.12, 1.03, 1.00, 0.98, 1.00, 0.95, 0.92)  # Monday = 0
DOW = tuple(d / (sum(_DOW_RAW) / 7) for d in _DOW_RAW)

ESI_MIX = {
    "level_1_trauma": (0.03, 0.18, 0.47, 0.25, 0.07),
    "community": (0.01, 0.12, 0.45, 0.32, 0.10),
    "rural_critical_access": (0.005, 0.08, 0.38, 0.40, 0.135),
    "pediatric": (0.005, 0.07, 0.35, 0.43, 0.145),
}
FLU_SHIFT = np.array((0.0, 0.04, 0.06, -0.07, -0.03))

AMBULANCE_P = {1: 0.85, 2: 0.45, 3: 0.20, 4: 0.06, 5: 0.03}
ADMIT_P = {1: 0.80, 2: 0.55, 3: 0.30, 4: 0.06, 5: 0.01}
ADMIT_TYPE_FACTOR = {"level_1_trauma": 1.1, "community": 1.0, "rural_critical_access": 0.8, "pediatric": 0.6}
VENT_P = {1: 0.35, 2: 0.04}

D2T_MEDIAN = {1: 1.0, 2: 4.0, 3: 8.0, 4: 10.0, 5: 10.0}
D2P_MEDIAN = {1: 2.0, 2: 8.0, 3: 22.0, 4: 32.0, 5: 38.0}
TREAT_MEDIAN = {1: 120.0, 2: 180.0, 3: 200.0, 4: 110.0, 5: 70.0}
BED_WAIT_MEDIAN = 10.0
PATIENCE_MEDIAN = 160.0
DISCHARGE_TO_DEPART_MEDIAN = 20.0
TRANSFER_TO_DEPART_MEDIAN = 150.0
PROFILE_SPEED = {
    "normal": 1.0,
    "chronic_boarding": 1.0,
    "understaffed": 1.25,
    "surge_prone": 1.0,
    "high_performer": 0.6,
}


def boarding_median(profile: str) -> float:
    return 540.0 if profile == "chronic_boarding" else 150.0


def esi_mix(facility_type: str, acuity_shift: bool) -> np.ndarray:
    p = np.array(ESI_MIX[facility_type], dtype=float)
    if acuity_shift:
        p = p + FLU_SHIFT
    p = np.clip(p, 0.001, None)
    return p / p.sum()


def arrivals_per_minute(baseline_daily: float, local_hour: int, local_weekday: int) -> float:
    return baseline_daily / 1440.0 * HOURLY[local_hour] * DOW[local_weekday]
