from ed_ops.config import Config
from ed_ops.validation.golden import EXPECTED, check_golden
from ed_ops.validation.pipeline import evaluate_pipeline


def test_golden_scenario_matches_hand_calculation(spark):
    assert check_golden(spark) == []


def test_golden_reports_mismatches(spark):
    wrong = dict(EXPECTED)
    wrong[("FAC001", "census")] = 99
    assert check_golden(spark, wrong) == ["FAC001.census: expected 99, got 5"]


GOOD = {
    "bronze_events": 2600, "silver_visits": 480, "quarantine": 10, "facility_current": 5, "snapshot_facilities": 5,
    "snapshot_rows": 1465, "board_census_mismatches": 0, "notes_enriched": 120, "notes_ok_share": 0.97,
    "facility_narratives": 5, "network_narratives": 1, "legacy_driver_entries": 0,
}
SMALL = Config(facility_count=5, backfill_days=1)


def test_evaluate_pipeline_passes_on_healthy_metrics():
    assert evaluate_pipeline(GOOD, SMALL) == []


def test_evaluate_pipeline_names_each_failure():
    bad = dict(GOOD, quarantine=0, snapshot_rows=100, board_census_mismatches=2, notes_ok_share=0.5,
               network_narratives=0, legacy_driver_entries=3)
    failures = evaluate_pipeline(bad, SMALL)
    assert len(failures) == 6
    assert any("quarantine" in f for f in failures) and any("arrivals_1h" in f for f in failures)
