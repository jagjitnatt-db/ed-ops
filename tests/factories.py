"""Test-data builders. All datetimes are naive UTC. The scenario lives in ed_ops.validation.golden, so unit
tests and the ed_validate job use exactly the same hand-calculated dataset."""

from ed_ops.validation.golden import (  # noqa: F401 (re-exported for tests)
    SCENARIO_VISITS, T, VISIT_DDL, as_of_frame, facilities_frame, m, roster_frame, visit, visits_frame,
)

visits_df = visits_frame
facilities_df = facilities_frame
roster_df = roster_frame
as_of_df = as_of_frame
