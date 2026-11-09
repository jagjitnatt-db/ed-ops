"""Crowding index formulas as Spark Column expressions.

Source: gEDWIN: a simple and practical index for real-time monitoring of ED crowding,
BMC Emergency Medicine 2025 (PMC12648796); NEDOCS: Weiss et al., Acad Emerg Med 2004.
Assumptions (spec section 7): natural log for gEDWIN; untriaged patients weighted t = 3;
occupancy clamped to [0.01, 0.99] (0.99 cap from the paper, 0.01 floor ours).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

OR_FLOOR, OR_CAP = 0.01, 0.99
EDWIN_BUSY, EDWIN_CROWDED = 1.5, 2.0
GEDWIN_BUSY, GEDWIN_CROWDED = 2.06, 2.34


def triage_weight(esi: Column) -> Column:
    return F.when(esi.isNull(), F.lit(3)).otherwise(F.lit(6) - esi)


def clamp(x: Column, lo: float, hi: float) -> Column:
    return F.least(F.greatest(x, F.lit(lo)), F.lit(hi))


def occupancy_clamped(census: Column, ed_beds: Column) -> Column:
    return clamp(F.try_divide(census.cast("double"), ed_beds.cast("double")), OR_FLOOR, OR_CAP)


def acuity_ratio(weight_sum: Column, census: Column) -> Column:
    return F.when(census > 0, F.try_divide(weight_sum.cast("double"), census.cast("double")))


def edwin(census: Column, ed_beds: Column, attendings: Column, ar: Column) -> Column:
    orc = occupancy_clamped(census, ed_beds)
    return (
        F.when(attendings.isNull() | (attendings <= 0), F.lit(None).cast("double"))
        .when(census == 0, F.lit(0.0))
        .otherwise(F.try_divide(orc / (F.lit(1.0) - orc) * ar, attendings.cast("double")))
    )


def gedwin(census: Column, ed_beds: Column, ar: Column) -> Column:
    orc = occupancy_clamped(census, ed_beds)
    return F.when(census > 0, F.log(orc / (F.lit(1.0) - orc)) + F.log(ar))


def edwin_band(e: Column) -> Column:
    return (
        F.when(e.isNull(), F.lit(None).cast("string"))
        .when(e < EDWIN_BUSY, F.lit("Active"))
        .when(e <= EDWIN_CROWDED, F.lit("Busy"))
        .otherwise(F.lit("Crowded"))
    )


def gedwin_band(g: Column, census: Column) -> Column:
    return (
        F.when(census == 0, F.lit("Active"))
        .when(g.isNull(), F.lit(None).cast("string"))
        .when(g < GEDWIN_BUSY, F.lit("Active"))
        .when(g <= GEDWIN_CROWDED, F.lit("Busy"))
        .otherwise(F.lit("Crowded"))
    )


def nedocs(
    census: Column,
    ed_beds: Column,
    boarders: Column,
    inpatient_beds: Column,
    vents: Column,
    longest_boarder_h: Column,
    last_bedded_wait_h: Column,
) -> Column:
    raw = (
        F.lit(-20.0)
        + F.lit(85.8) * F.try_divide(census.cast("double"), ed_beds.cast("double"))
        + F.lit(600.0) * F.try_divide(boarders.cast("double"), inpatient_beds.cast("double"))
        + F.lit(13.4) * vents.cast("double")
        + F.lit(0.93) * F.coalesce(longest_boarder_h, F.lit(0.0))
        + F.lit(5.64) * F.coalesce(last_bedded_wait_h, F.lit(0.0))
    )
    return clamp(raw, 0.0, 200.0)


def nedocs_level(n: Column) -> Column:
    return (
        F.when(n <= 20, F.lit(1))
        .when(n <= 60, F.lit(2))
        .when(n <= 100, F.lit(3))
        .when(n <= 140, F.lit(4))
        .when(n <= 180, F.lit(5))
        .otherwise(F.lit(6))
    )
