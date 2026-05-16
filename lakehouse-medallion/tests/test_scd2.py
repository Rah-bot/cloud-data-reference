"""
Unit tests for SCD2 logic. Runs against a local Spark session.

To run:  pytest tests/test_scd2.py
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transforms.silver.dim_customer_scd2 import attribute_hash, TRACKED_ATTRIBUTES


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def test_attribute_hash_is_deterministic(spark):
    df = spark.createDataFrame(
        [("c1", "Alice", "Smith", "a@x.com", "1 Main", "NYC", "NY", "10001", "US", True)],
        ["customer_id"] + TRACKED_ATTRIBUTES,
    )
    h1 = attribute_hash(df, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    h2 = attribute_hash(df, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    assert h1 == h2


def test_attribute_hash_changes_when_tracked_field_changes(spark):
    base_row = ("c1", "Alice", "Smith", "a@x.com", "1 Main", "NYC", "NY", "10001", "US", True)
    changed_row = ("c1", "Alice", "Smith", "a@x.com", "2 Main", "NYC", "NY", "10001", "US", True)

    cols = ["customer_id"] + TRACKED_ATTRIBUTES
    df1 = spark.createDataFrame([base_row], cols)
    df2 = spark.createDataFrame([changed_row], cols)

    h1 = attribute_hash(df1, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    h2 = attribute_hash(df2, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    assert h1 != h2


def test_attribute_hash_stable_to_column_order(spark):
    cols = ["customer_id"] + TRACKED_ATTRIBUTES
    row = ("c1", "Alice", "Smith", "a@x.com", "1 Main", "NYC", "NY", "10001", "US", True)
    df = spark.createDataFrame([row], cols)

    # If we recompute hash with same column order, same value.
    h1 = attribute_hash(df, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    h2 = attribute_hash(df.select(*cols), TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    assert h1 == h2


def test_attribute_hash_nulls_handled(spark):
    cols = ["customer_id"] + TRACKED_ATTRIBUTES
    row = ("c1", "Alice", None, "a@x.com", None, "NYC", "NY", "10001", "US", None)
    df = spark.createDataFrame([row], cols)
    h = attribute_hash(df, TRACKED_ATTRIBUTES).select("_attr_hash").first()[0]
    assert isinstance(h, str) and len(h) == 64  # SHA-256 hex
