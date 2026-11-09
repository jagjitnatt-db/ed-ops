"""Pure-pandas reference data for generator tests (no Spark needed)."""

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from ed_ops.generator.journey import empty_plan, plan_arrivals
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.roster import build_roster

FACILITIES = pd.DataFrame(
    [
        ("FAC001", "Springfield Memorial Hospital", "Central", "Springfield", "IL", 39.8, -89.6, "community", 30, 200, 72, "normal"),
        ("FAC002", "Peoria Regional Medical Center", "Central", "Peoria", "IL", 40.7, -89.6, "level_1_trauma", 60, 450, 144, "chronic_boarding"),
        ("FAC003", "Madison Community Hospital", "North", "Madison", "WI", 43.1, -89.4, "community", 25, 150, 60, "surge_prone"),
    ],
    columns=["facility_id", "facility_name", "region", "city", "state", "latitude", "longitude", "facility_type",
             "ed_beds", "inpatient_beds", "baseline_daily_visits", "scenario_profile"],
)


def make_pool(size: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pool_id": np.arange(size, dtype="int64"),
            "first_name": [f"Pat{i}" for i in range(size)],
            "last_name": ["Tester"] * size,
            "dob": [date(1950 + i % 50, 1 + i % 12, 1 + i % 28) for i in range(size)],
            "sex": ["F" if i % 2 == 0 else "M" for i in range(size)],
            "phone": [f"312-555-{i:04d}" for i in range(size)],
            "street": [f"{i + 10} Main St" for i in range(size)],
            "zip": ["60601"] * size,
            "mrn": [f"MRN-{7000000 + i}" for i in range(size)],
        }
    )


def make_reference(facilities: pd.DataFrame | None = None, pool_size: int = 100) -> ReferenceData:
    fac = FACILITIES if facilities is None else facilities
    roster = build_roster(fac, date(2026, 10, 20), 40, "America/Chicago")
    return ReferenceData(fac, roster, make_pool(pool_size))


def plan_hours(ref: ReferenceData, start: datetime, hours: int, seed: int = 42) -> pd.DataFrame:
    plan = empty_plan()
    for h in range(hours):
        s, e = start + timedelta(hours=h), start + timedelta(hours=h + 1)
        rng = np.random.default_rng([seed, h])
        new = plan_arrivals(rng, ref, plan, s, e, [], "America/Chicago")
        plan = pd.concat([plan, new], ignore_index=True) if not new.empty else plan
    return plan
