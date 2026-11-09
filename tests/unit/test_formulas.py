import math

import pytest
from pyspark.sql import functions as F

from ed_ops.metrics import formulas as fx

DDL = "census INT, ed_beds INT, attendings INT, weight_sum INT, boarders INT, inpatient INT, vents INT, lb DOUBLE, lbw DOUBLE"


def _eval(spark, row):
    df = spark.createDataFrame([row], DDL)
    c = F.col
    ar = fx.acuity_ratio(c("weight_sum"), c("census"))
    df = df.withColumn("ar", ar)
    df = df.withColumn("edwin", fx.edwin(c("census"), c("ed_beds"), c("attendings"), c("ar")))
    df = df.withColumn("gedwin", fx.gedwin(c("census"), c("ed_beds"), c("ar")))
    df = df.withColumn("edwin_band", fx.edwin_band(c("edwin")))
    df = df.withColumn("gedwin_band", fx.gedwin_band(c("gedwin"), c("census")))
    df = df.withColumn("nedocs", fx.nedocs(c("census"), c("ed_beds"), c("boarders"), c("inpatient"), c("vents"), c("lb"), c("lbw")))
    df = df.withColumn("nedocs_level", fx.nedocs_level(c("nedocs")))
    return df.collect()[0].asDict()


def test_typical_case(spark):
    # 20 beds, 15 patients: ESI1 x1, ESI2 x3, ESI3 x6, ESI4 x4, ESI5 x1 -> sum(n*t) = 5+12+18+8+1 = 44
    r = _eval(spark, (15, 20, 2, 44, 0, 300, 0, None, None))
    assert r["ar"] == pytest.approx(44 / 15)
    assert r["edwin"] == pytest.approx((1 / 2) * (0.75 / 0.25) * (44 / 15))  # 4.4
    assert r["edwin_band"] == "Crowded"
    assert r["gedwin"] == pytest.approx(math.log(0.75 / 0.25) + math.log(44 / 15))  # 2.1748
    assert r["gedwin_band"] == "Busy"


def test_full_ed_caps_occupancy_at_099(spark):
    r = _eval(spark, (12, 10, 1, 36, 0, 300, 0, None, None))  # OR 1.2, all ESI 3 -> AR 3
    assert r["edwin"] == pytest.approx(99 * 3)
    assert r["gedwin"] == pytest.approx(math.log(99) + math.log(3))
    assert r["gedwin_band"] == "Crowded"


def test_empty_ed(spark):
    r = _eval(spark, (0, 10, 2, 0, 0, 300, 0, None, None))
    assert r["ar"] is None
    assert r["edwin"] == 0.0
    assert r["edwin_band"] == "Active"
    assert r["gedwin"] is None
    assert r["gedwin_band"] == "Active"


def test_no_attending_makes_edwin_null_but_not_gedwin(spark):
    r = _eval(spark, (5, 10, 0, 15, 0, 300, 0, None, None))
    assert r["edwin"] is None and r["edwin_band"] is None
    assert r["gedwin"] == pytest.approx(math.log(0.5 / 0.5) + math.log(3))


def test_null_attending_treated_like_zero(spark):
    r = _eval(spark, (5, 10, None, 15, 0, 300, 0, None, None))
    assert r["edwin"] is None


def test_low_occupancy_floor(spark):
    r = _eval(spark, (1, 200, 1, 3, 0, 300, 0, None, None))  # OR 0.005 -> clamped to 0.01
    assert r["gedwin"] == pytest.approx(math.log(0.01 / 0.99) + math.log(3))


@pytest.mark.parametrize(
    "value,band",
    [(1.49, "Active"), (1.5, "Busy"), (2.0, "Busy"), (2.01, "Crowded")],
)
def test_edwin_band_boundaries(spark, value, band):
    df = spark.createDataFrame([(value,)], "e DOUBLE").select(fx.edwin_band(F.col("e")).alias("b"))
    assert df.collect()[0]["b"] == band


@pytest.mark.parametrize(
    "value,band",
    [(2.05, "Active"), (2.06, "Busy"), (2.34, "Busy"), (2.35, "Crowded")],
)
def test_gedwin_band_boundaries(spark, value, band):
    df = spark.createDataFrame([(value, 5)], "g DOUBLE, census INT").select(fx.gedwin_band(F.col("g"), F.col("census")).alias("b"))
    assert df.collect()[0]["b"] == band


def test_nedocs_worked_example(spark):
    # -20 + 85.8*(30/25) + 600*(6/300) + 13.4*1 + 0.93*5 + 5.64*1.5 = 121.47 -> level 4
    r = _eval(spark, (30, 25, 3, 90, 6, 300, 1, 5.0, 1.5))
    assert r["nedocs"] == pytest.approx(121.47)
    assert r["nedocs_level"] == 4


def test_nedocs_clamps(spark):
    assert _eval(spark, (0, 25, 3, 0, 0, 300, 0, None, None))["nedocs"] == 0.0
    huge = _eval(spark, (100, 10, 3, 300, 90, 100, 5, 40.0, 10.0))
    assert huge["nedocs"] == 200.0 and huge["nedocs_level"] == 6


@pytest.mark.parametrize("score,level", [(20, 1), (20.5, 2), (60, 2), (100, 3), (140, 4), (180, 5), (180.1, 6)])
def test_nedocs_level_cuts(spark, score, level):
    df = spark.createDataFrame([(float(score),)], "n DOUBLE").select(fx.nedocs_level(F.col("n")).alias("l"))
    assert df.collect()[0]["l"] == level


def test_triage_weight(spark):
    df = spark.createDataFrame([(1,), (5,), (None,)], "esi INT").select(fx.triage_weight(F.col("esi")).alias("w"))
    assert [r["w"] for r in df.collect()] == [5, 1, 3]
