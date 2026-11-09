"""Synthetic patient population built with dbldatagen; arrivals sample patients from it."""

from __future__ import annotations

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FIRST_NAMES = (
    "Ava", "Liam", "Mia", "Noah", "Emma", "Oliver", "Sophia", "Elijah", "Isabella", "James", "Amelia", "Lucas",
    "Harper", "Mason", "Evelyn", "Ethan", "Abigail", "Logan", "Emily", "Aiden", "Ella", "Jacob", "Grace", "Wyatt",
    "Chloe", "Henry", "Nora", "Daniel", "Lily", "Samuel", "Zoe", "Carter", "Layla", "Owen", "Riley", "Julian",
    "Aria", "Levi", "Maya", "Isaac",
)
LAST_NAMES = (
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
)
STREETS = (
    "Elm St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Washington Ave", "Lincoln Rd", "Lake St",
    "Hill Rd", "Park Ave", "Main St", "Church St", "River Rd", "Prairie Ln", "Sunset Blvd", "Highland Ave",
    "Grand Ave", "Jefferson St", "Madison Ave", "Walnut St",
)


def build_patient_pool(spark: SparkSession, size: int, seed: int) -> DataFrame:
    spec = (
        dg.DataGenerator(spark, name="patient_pool", rows=size, partitions=4, randomSeedMethod="hash_fieldname", randomSeed=seed)
        .withIdOutput()
        .withColumn("first_name", "string", values=list(FIRST_NAMES), random=True)
        .withColumn("last_name", "string", values=list(LAST_NAMES), random=True)
        .withColumn("dob", "date", data_range=dg.DateRange("1930-01-01 00:00:00", "2025-12-31 00:00:00", "days=1"), random=True)
        .withColumn("sex", "string", values=["F", "M"], weights=[52, 48], random=True)
        .withColumn("phone", "string", template=r"ddd-555-dddd")
        .withColumn("street_no", "int", minValue=10, maxValue=9999, random=True)
        .withColumn("street_name", "string", values=list(STREETS), random=True)
        .withColumn("zip", "string", template=r"6dddd")
    )
    return (
        spec.build()
        .withColumnRenamed("id", "pool_id")
        .withColumn("street", F.concat_ws(" ", F.col("street_no").cast("string"), F.col("street_name")))
        .withColumn("mrn", F.concat(F.lit("MRN-"), (F.col("pool_id") + F.lit(7000000)).cast("string")))
        .select("pool_id", "first_name", "last_name", "dob", "sex", "phone", "street", "zip", "mrn")
    )
