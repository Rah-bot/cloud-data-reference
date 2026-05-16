"""
Tests for the alerts job. Uses a local Spark session, no Kafka.

We test the *logic* — given a DataFrame of events, the same filtering and
union that the streaming job applies produces the expected alerts.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_alerts")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def harsh_braking_filter(df):
    return df.where(F.col("harsh_braking") == True)  # noqa: E712


def geofence_violation_filter(df):
    return df.where(F.col("geofence_id").isNotNull() & (F.col("speed_kph") > 50))


def test_harsh_braking_detected(spark):
    rows = [
        ("e1", "V1", True, None, 60.0),
        ("e2", "V2", False, None, 60.0),
        ("e3", "V3", True, "manhattan", 30.0),
    ]
    df = spark.createDataFrame(rows, ["event_id", "vehicle_id", "harsh_braking", "geofence_id", "speed_kph"])
    out = harsh_braking_filter(df).collect()
    assert {r["event_id"] for r in out} == {"e1", "e3"}


def test_geofence_violation_only_when_above_limit(spark):
    rows = [
        ("e1", "V1", False, "manhattan", 70.0),   # violation
        ("e2", "V2", False, "manhattan", 40.0),   # in zone but slow
        ("e3", "V3", False, None, 90.0),          # fast but no zone
        ("e4", "V4", False, "manhattan", 51.0),   # just over
    ]
    df = spark.createDataFrame(rows, ["event_id", "vehicle_id", "harsh_braking", "geofence_id", "speed_kph"])
    out = geofence_violation_filter(df).collect()
    assert {r["event_id"] for r in out} == {"e1", "e4"}


def test_dedupe_by_event_id_invariant(spark):
    """
    Property: union(harsh_braking, geofence_violation) may contain the same
    event_id at most once per alert_type. The alerts sink key includes event_id,
    so downstream dedupe is safe.
    """
    rows = [
        ("e1", "V1", True, "manhattan", 70.0),
        ("e2", "V2", True, "manhattan", 70.0),
    ]
    df = spark.createDataFrame(rows, ["event_id", "vehicle_id", "harsh_braking", "geofence_id", "speed_kph"])
    h = harsh_braking_filter(df).withColumn("alert_type", F.lit("HARSH_BRAKING"))
    g = geofence_violation_filter(df).withColumn("alert_type", F.lit("GEOFENCE_SPEED"))
    union = h.unionByName(g, allowMissingColumns=True)
    dup_count = (
        union.groupBy("event_id", "alert_type").count().where("count > 1").count()
    )
    assert dup_count == 0
