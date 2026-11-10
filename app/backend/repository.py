"""Read-only queries against the Lakebase synced tables (Postgres schema = the UC serving schema)."""

from __future__ import annotations

from typing import Protocol

FACILITY_COLUMNS = (
    "facility_id, facility_name, facility_type, region, snapshot_ts, census, gedwin, gedwin_band, gedwin_delta_1h, "
    "edwin, edwin_band, nedocs, nedocs_level, occupancy_ratio, d2p_mean_min, d2p_median_min, d2p_p90_min, d2p_sample_n, "
    "waiting_now_n, longest_current_wait_min, boarders_n, longest_boarder_h, lwbs_rate_24h, los_median_h_24h, "
    "attendings_on_duty, patients_per_attending, arrival_surge_ratio, undertriage_flags_n, "
    "to_jsonb(top_drivers) AS top_drivers, computed_at"
)


class Repository(Protocol):
    def facilities(self) -> list[dict]: ...
    def facility(self, facility_id: str) -> dict | None: ...
    def narrative(self, facility_id: str) -> dict | None: ...
    def network(self) -> dict | None: ...
    def history(self, facility_id: str, hours: int) -> list[dict]: ...
    def board(self, facility_id: str) -> list[dict]: ...
    def sparklines(self, hours: int) -> dict[str, list]: ...
    def heatmap(self, hours: int) -> list[dict]: ...
    def network_history(self, hours: int, bucket_minutes: int) -> list[dict]: ...
    def data_age_seconds(self) -> float | None: ...


class LakebaseRepository:
    def __init__(self, pool, schema: str) -> None:
        self.pool = pool
        self.s = '"' + schema.replace('"', '""') + '"'

    def _all(self, query: str, params: tuple = ()) -> list[dict]:
        from psycopg.rows import dict_row

        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return list(cur.fetchall())

    def _one(self, query: str, params: tuple = ()) -> dict | None:
        rows = self._all(query, params)
        return rows[0] if rows else None

    def facilities(self) -> list[dict]:
        return self._all(f"SELECT {FACILITY_COLUMNS} FROM {self.s}.facility_current ORDER BY gedwin DESC NULLS LAST")

    def facility(self, facility_id: str) -> dict | None:
        return self._one(
            # Subquery first so FACILITY_COLUMNS stays unqualified without clashing with ref columns of the same name.
            f"SELECT c.*, f.ed_beds, f.inpatient_beds, f.city, f.state "
            f"FROM (SELECT {FACILITY_COLUMNS} FROM {self.s}.facility_current WHERE facility_id = %s) c "
            f"JOIN {self.s}.facility f ON f.facility_id = c.facility_id",
            (facility_id,),
        )

    def narrative(self, facility_id: str) -> dict | None:
        return self._one(
            f"SELECT facility_id, snapshot_ts, gedwin_band, headline, explanation, to_jsonb(recommended_actions) AS recommended_actions, "
            f"to_jsonb(top_drivers) AS top_drivers, generated_at FROM {self.s}.facility_narrative WHERE facility_id = %s",
            (facility_id,),
        )

    def network(self) -> dict | None:
        return self._one(
            "SELECT snapshot_ts, crowded_n, busy_n, active_n, headline, explanation, to_jsonb(crowded_facilities) AS crowded_facilities, "
            "to_jsonb(doing_well_facilities) AS doing_well_facilities, to_jsonb(recommended_actions) AS recommended_actions, "
            f"generated_at FROM {self.s}.network_narrative WHERE network_id = 'NETWORK'"
        )

    def history(self, facility_id: str, hours: int) -> list[dict]:
        return self._all(
            "SELECT snapshot_ts, gedwin, edwin, nedocs, d2p_median_min, d2p_p90_min, d2p_sample_n, census, n_esi1, n_esi2, "
            "n_esi3, n_esi4, n_esi5, n_untriaged, boarders_n, occupancy_ratio, waiting_now_n, longest_current_wait_min, lwbs_rate_24h "
            f"FROM {self.s}.facility_snapshot_5min WHERE facility_id = %s AND snapshot_ts > now() - make_interval(hours => %s) "
            "ORDER BY snapshot_ts",
            (facility_id, hours),
        )

    def board(self, facility_id: str) -> list[dict]:
        return self._all(
            "SELECT visit_id, status, esi_level, suggested_esi, undertriage_flag, complaint_category, arrival_ts, minutes_in_ed, "
            f"minutes_waiting, arrival_mode, on_vent FROM {self.s}.ed_board_current WHERE facility_id = %s AND in_ed "
            "ORDER BY esi_level NULLS LAST, minutes_waiting DESC",
            (facility_id,),
        )

    def sparklines(self, hours: int) -> dict[str, list]:
        out: dict[str, list] = {}
        for r in self._all(
            f"SELECT facility_id, snapshot_ts, gedwin FROM {self.s}.facility_snapshot_5min "
            "WHERE snapshot_ts > now() - make_interval(hours => %s) ORDER BY facility_id, snapshot_ts",
            (hours,),
        ):
            out.setdefault(r["facility_id"], []).append([r["snapshot_ts"], r["gedwin"]])
        return out

    def heatmap(self, hours: int) -> list[dict]:
        return self._all(
            "SELECT s.facility_id, f.facility_name, date_trunc('hour', s.snapshot_ts) AS hour, max(s.gedwin) AS gedwin "
            f"FROM {self.s}.facility_snapshot_5min s JOIN {self.s}.facility f USING (facility_id) "
            "WHERE s.snapshot_ts > now() - make_interval(hours => %s) GROUP BY 1, 2, 3 ORDER BY 2, 3",
            (hours,),
        )

    def network_history(self, hours: int, bucket_minutes: int) -> list[dict]:
        """Network totals per 5-minute snapshot, then averaged per bucket (longest wait: the bucket maximum)."""
        return self._all(
            "WITH snap AS ("
            "  SELECT snapshot_ts, sum(census) AS census, percentile_cont(0.5) WITHIN GROUP (ORDER BY d2p_median_min) AS d2p_median_min, "
            "         sum(waiting_now_n) AS waiting_now_n, max(longest_current_wait_min) AS longest_current_wait_min, "
            "         sum(boarders_n) AS boarders_n, avg(lwbs_rate_24h) AS lwbs_rate_24h, "
            "         count(*) FILTER (WHERE gedwin_band = 'Crowded') AS crowded_n "
            f"  FROM {self.s}.facility_snapshot_5min WHERE snapshot_ts > now() - make_interval(hours => %s) GROUP BY snapshot_ts) "
            "SELECT date_bin(make_interval(mins => %s), snapshot_ts, TIMESTAMP '2000-01-01') AS bucket, "
            "       round(avg(census))::int AS census, avg(d2p_median_min) AS d2p_median_min, round(avg(waiting_now_n))::int AS waiting_now_n, "
            "       max(longest_current_wait_min) AS longest_current_wait_min, round(avg(boarders_n))::int AS boarders_n, "
            "       avg(lwbs_rate_24h) AS lwbs_rate_24h, round(avg(crowded_n))::int AS crowded_n "
            "FROM snap GROUP BY 1 ORDER BY 1",
            (hours, bucket_minutes),
        )

    def data_age_seconds(self) -> float | None:
        row = self._one(f"SELECT EXTRACT(EPOCH FROM now() - max(computed_at))::float AS age FROM {self.s}.facility_current")
        return row["age"] if row else None
