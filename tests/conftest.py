import os
import time

import pytest
from pyspark.sql import SparkSession


# PySpark converts timestamps to/from Python datetimes in the OS time zone; the project convention is naive UTC.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ed_ops_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
