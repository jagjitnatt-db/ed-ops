"""The simulated ED network: dbldatagen draws the random attributes, fixed rules make sizes coherent."""

from __future__ import annotations

import math
from collections import Counter

import dbldatagen as dg
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from ed_ops.config import Config

FACILITY_COLUMNS = (
    "facility_id", "facility_name", "region", "city", "state", "latitude", "longitude", "facility_type",
    "ed_beds", "inpatient_beds", "baseline_daily_visits", "scenario_profile",
)

CITIES = (
    ("Springfield", "IL", 39.80, -89.64, "Central"), ("Peoria", "IL", 40.69, -89.59, "Central"),
    ("Champaign", "IL", 40.12, -88.24, "Central"), ("Bloomington", "IL", 40.48, -88.99, "Central"),
    ("Rockford", "IL", 42.27, -89.09, "North"), ("Madison", "WI", 43.07, -89.40, "North"),
    ("Milwaukee", "WI", 43.04, -87.91, "North"), ("Green Bay", "WI", 44.51, -88.01, "North"),
    ("Naperville", "IL", 41.75, -88.15, "Metro"), ("Joliet", "IL", 41.53, -88.08, "Metro"),
    ("Evanston", "IL", 42.05, -87.69, "Metro"), ("Aurora", "IL", 41.76, -88.32, "Metro"),
    ("Kankakee", "IL", 41.12, -87.86, "Metro"), ("St. Louis", "MO", 38.63, -90.20, "South"),
    ("Columbia", "MO", 38.95, -92.33, "South"), ("Carbondale", "IL", 37.73, -89.22, "South"),
    ("Davenport", "IA", 41.52, -90.58, "West"), ("Cedar Rapids", "IA", 41.98, -91.67, "West"),
    ("Des Moines", "IA", 41.59, -93.62, "West"), ("Quincy", "IL", 39.94, -91.41, "West"),
)
TYPE_SUFFIXES = {
    "level_1_trauma": ("Regional Medical Center", "University Hospital"),
    "community": ("Memorial Hospital", "Community Hospital", "General Hospital"),
    "rural_critical_access": ("Area Hospital", "Critical Access Hospital"),
    "pediatric": ("Children's Hospital",),
}
TYPE_SHARES = {"community": 0.56, "rural_critical_access": 0.24, "level_1_trauma": 0.12, "pediatric": 0.08}
PROFILE_SHARES = {"normal": 0.6, "chronic_boarding": 0.1, "understaffed": 0.1, "surge_prone": 0.1, "high_performer": 0.1}
SIZE = {
    "level_1_trauma": ((50, 80), (300, 600)),
    "community": ((18, 45), (100, 300)),
    "rural_critical_access": ((8, 15), (25, 60)),
    "pediatric": ((20, 40), (80, 200)),
}
VISITS_PER_BED = {"level_1_trauma": 2.4, "community": 2.4, "rural_critical_access": 1.8, "pediatric": 2.2}


def allocate(shares: dict[str, float], n: int) -> list[str]:
    """Deterministic largest-remainder allocation; every label gets >= 1 when n >= len(shares)."""
    keys = list(shares)
    minimum = 1 if n >= len(keys) else 0
    counts = {k: max(minimum, math.floor(shares[k] * n)) for k in keys}
    while sum(counts.values()) > n:
        largest = max(keys, key=lambda k: (counts[k], -keys.index(k)))
        counts[largest] -= 1
    while sum(counts.values()) < n:
        best = max(keys, key=lambda k: (shares[k] * n - counts[k], -keys.index(k)))
        counts[best] += 1
    return [k for k in keys for _ in range(counts[k])]


def build_facilities(spark: SparkSession, cfg: Config) -> pd.DataFrame:
    n = cfg.facility_count
    spec = (
        dg.DataGenerator(spark, name="facilities", rows=n, partitions=1, randomSeedMethod="hash_fieldname", randomSeed=cfg.seed)
        .withIdOutput()
        .withColumn("city_idx", "int", minValue=0, maxValue=len(CITIES) - 1, random=True)
        .withColumn("suffix_idx", "int", minValue=0, maxValue=11, random=True)
        .withColumn("lat_jitter_milli", "int", minValue=-60, maxValue=60, random=True)
        .withColumn("lon_jitter_milli", "int", minValue=-60, maxValue=60, random=True)
        .withColumn("size_pct", "int", minValue=0, maxValue=100, random=True)
        .withColumn("inpatient_pct", "int", minValue=0, maxValue=100, random=True)
        .withColumn("visit_jitter_pct", "int", minValue=-10, maxValue=10, random=True)
    )
    raw = spec.build().orderBy("id").toPandas()
    rng = np.random.default_rng(cfg.seed)
    types = [str(t) for t in rng.permutation(allocate(TYPE_SHARES, n))]
    profiles = [str(p) for p in rng.permutation(allocate(PROFILE_SHARES, n))]
    seen: Counter[str] = Counter()
    rows = []
    for i, r in enumerate(raw.itertuples(index=False)):
        ftype = types[i]
        city, state, lat, lon, region = CITIES[int(r.city_idx)]
        suffixes = TYPE_SUFFIXES[ftype]
        base = f"{city} {suffixes[int(r.suffix_idx) % len(suffixes)]}"
        seen[base] += 1
        name = base if seen[base] == 1 else f"{base} - Campus {seen[base]}"
        (bed_lo, bed_hi), (inp_lo, inp_hi) = SIZE[ftype]
        ed_beds = bed_lo + round((bed_hi - bed_lo) * int(r.size_pct) / 100)
        inpatient = inp_lo + round((inp_hi - inp_lo) * int(r.inpatient_pct) / 100)
        visits = round(ed_beds * VISITS_PER_BED[ftype] * (1 + int(r.visit_jitter_pct) / 100))
        rows.append(
            {
                "facility_id": f"FAC{i + 1:03d}",
                "facility_name": name,
                "region": region,
                "city": city,
                "state": state,
                "latitude": round(lat + int(r.lat_jitter_milli) / 1000, 4),
                "longitude": round(lon + int(r.lon_jitter_milli) / 1000, 4),
                "facility_type": ftype,
                "ed_beds": int(ed_beds),
                "inpatient_beds": int(inpatient),
                "baseline_daily_visits": int(visits),
                "scenario_profile": profiles[i],
            }
        )
    return pd.DataFrame(rows, columns=list(FACILITY_COLUMNS))
