"""In-memory reference data for the simulator (pandas; small enough for the driver)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from ed_ops.config import Config


class ReferenceData:
    def __init__(self, facilities: pd.DataFrame, roster: pd.DataFrame, pool: pd.DataFrame) -> None:
        self.facilities = facilities.reset_index(drop=True)
        self.roster = roster.reset_index(drop=True)
        self.pool = pool.sort_values("pool_id").reset_index(drop=True)
        if not np.array_equal(self.pool["pool_id"].to_numpy(), np.arange(len(self.pool))):
            raise ValueError("patient pool ids must be contiguous 0..N-1")
        self._patients = self.pool.to_dict("records")
        self._facilities = {f.facility_id: f for f in self.facilities.itertuples(index=False)}
        self._shifts: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
        for fid, group in self.roster.groupby("facility_id"):
            self._shifts[fid] = (
                group["shift_start"].to_numpy(dtype="datetime64[ns]"),
                group["shift_end"].to_numpy(dtype="datetime64[ns]"),
                group["attendings_on_duty"].to_numpy(dtype=int),
            )

    @property
    def pool_size(self) -> int:
        return len(self._patients)

    def patient(self, pool_id: int) -> dict:
        return self._patients[int(pool_id)]

    def facility(self, facility_id: str):
        return self._facilities[facility_id]

    def attendings_at(self, facility_id: str, ts: datetime) -> int:
        if facility_id not in self._shifts:
            return 0
        starts, ends, counts = self._shifts[facility_id]
        t = np.datetime64(ts, "ns")
        return int(counts[(starts <= t) & (ends > t)].sum())


def load_reference(spark: SparkSession, cfg: Config) -> ReferenceData:
    return ReferenceData(
        spark.table(cfg.table("ref", "facility")).toPandas(),
        spark.table(cfg.table("ref", "physician_roster")).toPandas(),
        spark.table(cfg.table("sim", "patient_pool")).toPandas(),
    )
