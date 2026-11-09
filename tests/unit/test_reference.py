from collections import Counter
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from ed_ops.config import Config
from ed_ops.generator.facilities import FACILITY_COLUMNS, allocate, build_facilities
from ed_ops.generator.population import build_patient_pool
from ed_ops.generator.reference import ReferenceData
from ed_ops.generator.roster import attendings_for, build_roster


def test_allocate_proportional_with_minimum_one():
    shares = {"normal": 0.6, "chronic_boarding": 0.1, "understaffed": 0.1, "surge_prone": 0.1, "high_performer": 0.1}
    assert Counter(allocate(shares, 50)) == {"normal": 30, "chronic_boarding": 5, "understaffed": 5, "surge_prone": 5, "high_performer": 5}
    assert Counter(allocate(shares, 5)) == {k: 1 for k in shares}
    assert len(allocate(shares, 3)) == 3


@pytest.fixture(scope="module")
def facilities(spark):
    return build_facilities(spark, Config())


def test_facilities_shape_and_mix(facilities):
    assert list(facilities.columns) == list(FACILITY_COLUMNS)
    assert list(facilities.facility_id) == [f"FAC{i:03d}" for i in range(1, 51)]
    assert facilities.facility_name.is_unique
    assert Counter(facilities.scenario_profile) == {"normal": 30, "chronic_boarding": 5, "understaffed": 5, "surge_prone": 5, "high_performer": 5}
    assert Counter(facilities.facility_type) == {"community": 28, "rural_critical_access": 12, "level_1_trauma": 6, "pediatric": 4}


def test_facility_sizes_follow_type(facilities):
    trauma = facilities[facilities.facility_type == "level_1_trauma"]
    rural = facilities[facilities.facility_type == "rural_critical_access"]
    assert trauma.ed_beds.between(50, 80).all() and trauma.inpatient_beds.between(300, 600).all()
    assert rural.ed_beds.between(8, 15).all() and rural.inpatient_beds.between(25, 60).all()
    assert (facilities.baseline_daily_visits > 0).all()


def test_facilities_deterministic(spark, facilities):
    pd.testing.assert_frame_equal(facilities, build_facilities(spark, Config()))


def test_small_network_has_every_profile(spark):
    small = build_facilities(spark, Config(facility_count=5))
    assert set(small.scenario_profile) == {"normal", "chronic_boarding", "understaffed", "surge_prone", "high_performer"}


def test_roster_covers_every_hour_once_across_dst(facilities):
    one = facilities.head(1)
    roster = build_roster(one, date(2026, 10, 30), 5, "America/Chicago").sort_values("shift_start")
    starts, ends = list(roster.shift_start), list(roster.shift_end)
    assert all(ends[i] == starts[i + 1] for i in range(len(starts) - 1))  # contiguous, no gaps/overlaps
    night = roster[(roster.shift_name == "night") & (roster.shift_start == datetime(2026, 11, 1, 4))]
    assert len(night) == 1 and night.iloc[0].shift_end == datetime(2026, 11, 1, 13)  # 23:00 CDT -> 07:00 CST = 9 h
    assert len(roster) == 15


def test_attendings_rules():
    assert attendings_for(24, "normal", "night", 0.6) == 1  # minimum one
    assert attendings_for(200, "understaffed", "night", 0.6) == attendings_for(200, "normal", "night", 0.6) - 1
    assert attendings_for(200, "high_performer", "day", 1.0) == attendings_for(200, "normal", "day", 1.0) + 1


def test_patient_pool(spark):
    pool = build_patient_pool(spark, 200, 42).toPandas()
    assert list(pool.columns) == ["pool_id", "first_name", "last_name", "dob", "sex", "phone", "street", "zip", "mrn"]
    assert sorted(pool.pool_id) == list(range(200))
    assert pool.mrn.str.fullmatch(r"MRN-\d{7}").all() and pool.mrn.is_unique
    assert set(pool.sex) <= {"F", "M"}
    assert pool.phone.str.fullmatch(r"\d{3}-555-\d{4}").all()


def test_reference_data_lookups(spark, facilities):
    pool = build_patient_pool(spark, 10, 42).toPandas()
    roster = build_roster(facilities.head(2), date(2026, 11, 9), 1, "America/Chicago")
    ref = ReferenceData(facilities.head(2), roster, pool)
    assert ref.pool_size == 10
    assert ref.patient(3)["mrn"] == "MRN-7000003"
    assert ref.facility("FAC001").facility_id == "FAC001"
    noon_local = datetime(2026, 11, 9, 18)  # 12:00 CST -> day shift
    assert ref.attendings_at("FAC001", noon_local) == int(
        roster[(roster.facility_id == "FAC001") & (roster.shift_name == "day")].attendings_on_duty.iloc[0]
    )
    assert ref.attendings_at("FAC001", noon_local + timedelta(days=5)) == 0
