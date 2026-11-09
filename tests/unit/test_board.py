from ed_ops.metrics.board import board_rows
from tests.factories import SCENARIO_VISITS, T, visits_df


def test_board_statuses_and_minutes(spark):
    rows = {r["visit_id"]: r.asDict() for r in board_rows(visits_df(spark, SCENARIO_VISITS), T).collect()}
    assert set(rows) == {"v1", "v2", "v3", "v4", "v9", "v5"}  # v6/v7 departed > 30 min ago, v8 future
    assert rows["v1"]["status"] == "boarding" and rows["v1"]["on_vent"] is True
    assert rows["v2"]["status"] == "in_bed" and rows["v2"]["minutes_waiting"] == 30
    assert rows["v3"]["status"] == "waiting" and rows["v3"]["minutes_waiting"] == 40
    assert rows["v5"]["status"] == "departed" and rows["v5"]["in_ed"] is False
    assert rows["v5"]["minutes_in_ed"] == 85 and rows["v5"]["departed_ts"] is not None
    assert rows["v1"]["minutes_in_ed"] == 120 and rows["v1"]["in_ed"] is True
    assert rows["v9"]["esi_level"] is None  # triaged after T
    assert rows["v1"]["as_of_ts"] == T


def test_board_columns_match_table(spark):
    from ed_ops.ddl.metadata import load_catalog_def

    expected = load_catalog_def().table("gold", "ed_board_current").column_names
    assert board_rows(visits_df(spark, SCENARIO_VISITS), T).columns == expected
