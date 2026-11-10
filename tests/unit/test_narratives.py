import json
from datetime import datetime

import pytest

from ed_ops.delta import conform, table_def
from ed_ops.metrics.drivers import compute_baselines
from ed_ops.pipeline.gold_metrics import compute_tick_frames
from ed_ops.pipeline.gold_narratives import facility_narratives, network_narrative
from tests.factories import SCENARIO_VISITS, T, facilities_df, roster_df, visits_df
from tests.fake_ai import FakeAi

BASELINE_DDL = ("facility_id STRING, snapshot_ts TIMESTAMP, occupancy_ratio DOUBLE, boarder_share DOUBLE, "
                "patients_per_attending DOUBLE, arrivals_1h INT, high_acuity_share DOUBLE")


@pytest.fixture(scope="module")
def current(spark):
    facilities = facilities_df(spark, [("FAC001", 20, 300), ("FAC002", 10, 100)])
    roster = roster_df(spark, [("FAC001", datetime(2026, 11, 9, 7), datetime(2026, 11, 9, 15), 2)])
    baselines = compute_baselines(spark.createDataFrame([], BASELINE_DDL), "America/Chicago")
    return compute_tick_frames(visits_df(spark, SCENARIO_VISITS), roster, facilities, baselines, T, "America/Chicago").current


def test_facility_narratives_keep_only_parsed_rows(spark, current):
    out = facility_narratives(current, FakeAi(garbage_marker="FAC002"))
    rows = {r.facility_id: r for r in conform(out, table_def("gold", "facility_narrative")).collect()}
    assert set(rows) == {"FAC001"}  # FAC002's reply did not parse -> previous narrative is kept (no row to merge)
    r = rows["FAC001"]
    assert r.headline == "Fake facility status" and r.recommended_actions == ["a", "b", "c"]  # capped at 3
    assert r.model == "fake-endpoint" and r.gedwin_band == "Active" and r.snapshot_ts == T
    payload = json.loads(r.input_json)
    assert payload["facility_id"] == "FAC001" and payload["census"] == 5 and "top_drivers" in payload


def test_network_narrative_single_row(spark, current):
    rows = conform(network_narrative(current, FakeAi()), table_def("gold", "network_narrative")).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.network_id == "NETWORK" and (r.crowded_n, r.busy_n, r.active_n) == (0, 0, 2)
    assert r.crowded_facilities == ["X"] and r.doing_well_facilities == ["Y"]
    assert len(json.loads(r.input_json)["facilities"]) == 2


def test_network_narrative_unparseable_yields_no_row(spark, current):
    assert network_narrative(current, FakeAi(garbage_marker="FAC001")).count() == 0
