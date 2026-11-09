import math
from datetime import datetime

import pytest

from ed_ops.metrics.snapshots import METRIC_COLUMNS, compute_snapshots
from tests.factories import (
    SCENARIO_VISITS, T, as_of_df, facilities_df, m, roster_df, visit, visits_df,
)


def _rows(df):
    return {(r["facility_id"], r["snapshot_ts"]): r.asDict() for r in df.collect()}


@pytest.fixture(scope="module")
def scenario(spark):
    visits = visits_df(spark, SCENARIO_VISITS)
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    return _rows(compute_snapshots(visits, roster, facilities, as_of_df(spark, T)))


def test_columns(spark, scenario):
    assert set(next(iter(scenario.values())).keys()) == set(METRIC_COLUMNS)


def test_census_is_point_in_time(scenario):
    r = scenario[("FAC001", T)]
    assert r["census"] == 5  # v1, v2, v3, v4, v9
    assert (r["n_esi1"], r["n_esi2"], r["n_esi3"], r["n_esi4"], r["n_esi5"]) == (1, 0, 1, 1, 0)
    assert r["n_untriaged"] == 2  # v4 and v9 (triaged after T)
    assert r["waiting_now_n"] == 3  # v3, v4, v9
    assert r["in_bed_n"] == 2
    assert r["longest_current_wait_min"] == pytest.approx(40.0)
    assert r["boarders_n"] == 1 and r["longest_boarder_h"] == pytest.approx(1.0)
    assert r["vents_n"] == 1
    assert r["undertriage_flags_n"] == 1  # v2 only; v9 not yet triaged


def test_door_to_provider_window(scenario):
    r = scenario[("FAC001", T)]
    assert r["d2p_sample_n"] == 2  # v2 (30 min) and v5 (60 min)
    assert r["d2p_mean_min"] == pytest.approx(45.0)
    assert r["d2p_median_min"] == pytest.approx(45.0)
    assert r["d2p_p90_min"] == pytest.approx(57.0)


def test_flow_windows(scenario):
    r = scenario[("FAC001", T)]
    assert r["arrivals_1h"] == 4 and r["ambulance_arrivals_1h"] == 1
    assert r["last_bedded_wait_h"] == pytest.approx(20 / 60)  # v2: arrival T-50, bed T-30
    assert r["lwbs_rate_24h"] == pytest.approx(1 / 3)
    assert r["los_median_h_24h"] == pytest.approx(85 / 60)
    assert r["boarding_median_h_24h"] == pytest.approx(2.0)
    assert r["admit_rate_24h"] == pytest.approx(2 / 3)


def test_indices(scenario):
    r = scenario[("FAC001", T)]
    assert r["attendings_on_duty"] == 2
    assert r["occupancy_ratio"] == pytest.approx(0.25)
    assert r["acuity_ratio"] == pytest.approx(16 / 5)  # 5 + 3 + 2 + 3 + 3
    assert r["edwin"] == pytest.approx(0.5 * (0.25 / 0.75) * 3.2)
    assert r["edwin_band"] == "Active"
    assert r["gedwin"] == pytest.approx(math.log(0.25 / 0.75) + math.log(3.2))
    assert r["gedwin_band"] == "Active"
    assert r["nedocs"] == pytest.approx(-20 + 85.8 * 0.25 + 600 / 300 + 13.4 + 0.93 * 1.0 + 5.64 * (20 / 60))  # 19.66
    assert r["nedocs_level"] == 1
    assert r["patients_per_attending"] == pytest.approx(2.5)
    assert r["boarder_share"] == pytest.approx(0.2)
    assert r["high_acuity_share"] == pytest.approx(0.2)
    assert r["dq_flags"] == []


def test_gedwin_delta_one_hour(scenario):
    # At T-60: v1 (ESI1, boarder), v5 (ESI5), v7 (ESI2) -> census 3, AR = 10/3, OR = 0.15
    prior = math.log(0.15 / 0.85) + math.log(10 / 3)
    r = scenario[("FAC001", T)]
    assert r["gedwin_delta_1h"] == pytest.approx(r["gedwin"] - prior)


def test_facility_without_visits_or_roster_still_reported(scenario):
    r = scenario[("FAC002", T)]
    assert r["census"] == 0
    assert r["edwin"] is None and r["edwin_band"] is None
    assert r["gedwin"] is None and r["gedwin_band"] == "Active"
    assert r["nedocs"] == 0.0 and r["nedocs_level"] == 1
    assert r["dq_flags"] == ["no_attending_on_duty"]
    assert r["d2p_sample_n"] == 0 and r["d2p_mean_min"] is None


def test_over_capacity_is_capped_and_flagged(spark):
    visits = visits_df(spark, [visit(f"c{i}", m(-30), facility_id="FAC003", esi=3, triage=m(-25)) for i in range(6)])
    roster = roster_df(spark, [("FAC003", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 1)])
    r = _rows(compute_snapshots(visits, roster, facilities_df(spark, [("FAC003", 4, 50)]), as_of_df(spark, T)))[("FAC003", T)]
    assert r["occupancy_ratio"] == pytest.approx(1.5)
    assert r["edwin"] == pytest.approx(99 * 3)
    assert r["gedwin"] == pytest.approx(math.log(99) + math.log(3))
    assert r["gedwin_band"] == "Crowded"
    assert r["dq_flags"] == ["census_exceeds_beds"]


def test_visit_crossing_midnight_counts_after_midnight(spark):
    late = datetime(2026, 11, 9, 23, 30)
    snap = datetime(2026, 11, 10, 0, 5)
    visits = visits_df(spark, [visit("mid", late, esi=3, triage=datetime(2026, 11, 9, 23, 40))])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 23), datetime(2026, 11, 10, 7), 1)])
    r = _rows(compute_snapshots(visits, roster, facilities_df(spark, [("FAC001", 20, 300)]), as_of_df(spark, snap)))[("FAC001", snap)]
    assert r["census"] == 1
    assert r["arrivals_1h"] == 1


def test_negative_door_to_provider_ignored(spark):
    bad = visit("neg", m(-20), esi=3, triage=m(-19), greet=m(-25), disposition="DISCHARGE", disposition_ts=m(-10), departure=m(-2))
    r = _rows(compute_snapshots(visits_df(spark, [bad]), roster_df(spark, []), facilities_df(spark, [("FAC001", 20, 300)]), as_of_df(spark, T)))[("FAC001", T)]
    assert r["d2p_sample_n"] == 0
