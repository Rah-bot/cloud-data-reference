"""
Spark Structured Streaming — fleet KPI aggregations to Snowflake.

Computes 1-minute tumbling-window KPIs per geofence and channel and writes
them to Snowflake via Snowpipe Streaming for near-real-time dashboards.

Aggregates per (geofence, minute):
    - active_vehicles      (distinct vehicle_id count)
    - avg_speed_kph
    - max_speed_kph
    - harsh_braking_events
    - total_events

Watermark: 30 seconds. Late events are dropped from this aggregation but
captured in the cold-archive job for offline reconciliation.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pathlib import Path


SCHEMA_PATH = Path(__file__).parents[2] / "producer" / "schemas" / "vehicle.avsc"


def build_stream(spark: SparkSession):
    avro_schema = SCHEMA_PATH.read_text()

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "vehicle.events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 500_000)
        .load()
    )

    payload = F.expr("substring(value, 6, length(value)-5)")

    events = (
        raw.select(from_avro(payload, avro_schema).alias("evt"))
        .select("evt.*")
        .withColumn("event_ts", (F.col("event_ts_ms") / 1000).cast("timestamp"))
        .withWatermark("event_ts", "30 seconds")
    )

    kpis = (
        events
        .withColumn("geofence_id", F.coalesce(F.col("geofence_id"), F.lit("__none__")))
        .groupBy(
            F.window(F.col("event_ts"), "1 minute").alias("window"),
            F.col("geofence_id"),
        )
        .agg(
            F.approx_count_distinct("vehicle_id").alias("active_vehicles"),
            F.avg("speed_kph").alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.sum(F.col("harsh_braking").cast("int")).alias("harsh_braking_events"),
            F.count("*").alias("total_events"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("geofence_id"),
            F.col("active_vehicles"),
            F.col("avg_speed_kph"),
            F.col("max_speed_kph"),
            F.col("harsh_braking_events"),
            F.col("total_events"),
        )
    )

    # Snowflake sink via Snowpipe Streaming.
    # The Snowflake Kafka Connector (Streaming mode) or the snowflake-ingest-java
    # library handle this in production; here we show the Spark-side write.
    return (
        kpis.writeStream
        .format("snowflake")
        .option("checkpointLocation", "/tmp/checkpoints/kpi/")
        .option("sfURL",       "${SNOWFLAKE_URL}")
        .option("sfUser",      "${SNOWFLAKE_USER}")
        .option("sfPassword",  "${SNOWFLAKE_PASSWORD}")
        .option("sfDatabase",  "FLEET")
        .option("sfSchema",    "ANALYTICS")
        .option("dbtable",     "FLEET_KPI_MIN")
        .option("streaming_stage", "fleet_stream_stage")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("vehicle-kpi-snowflake")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    query = build_stream(spark)
    query.awaitTermination()
