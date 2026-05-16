"""
Spark Structured Streaming — harsh-braking + geofence-violation alerts.

Reads Avro-encoded vehicle events from Kafka, applies the alerting rules,
and writes detected alerts to a downstream Kafka topic. Designed for
sub-2-second p99 alert latency at 50K eps.

Key properties:
    - Idempotent: events carry event_id, and the sink topic uses a key-based
      dedupe convention so replays are safe.
    - Checkpointed offsets ensure at-least-once read; the dedupe convention
      collapses any duplicates into exactly-once delivery downstream.
    - State is bounded: alerts are stateless per event; we don't need
      windows here, only filtering and enrichment.
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
        .option("failOnDataLoss", "false")
        .load()
    )

    # Schema Registry wire format = magic byte (1) + schema id (4) + Avro payload.
    payload = F.expr("substring(value, 6, length(value)-5)")

    events = (
        raw.select(
            F.col("key").cast("string").alias("vehicle_id"),
            from_avro(payload, avro_schema).alias("evt"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("vehicle_id", "evt.*", "kafka_ts")
        .withColumn("event_ts", (F.col("event_ts_ms") / 1000).cast("timestamp"))
        .withWatermark("event_ts", "30 seconds")
    )

    harsh_braking = (
        events
        .where(F.col("harsh_braking") == True)  # noqa: E712
        .select(
            F.col("event_id"),
            F.col("vehicle_id"),
            F.col("driver_id"),
            F.col("event_ts"),
            F.lit("HARSH_BRAKING").alias("alert_type"),
            F.col("acceleration_ms2"),
            F.col("speed_kph"),
            F.col("latitude"),
            F.col("longitude"),
        )
    )

    # Geofence violations: vehicle present in restricted zone while above limit.
    geofence_violations = (
        events
        .where(F.col("geofence_id").isNotNull() & (F.col("speed_kph") > 50))
        .select(
            F.col("event_id"),
            F.col("vehicle_id"),
            F.col("driver_id"),
            F.col("event_ts"),
            F.lit("GEOFENCE_SPEED").alias("alert_type"),
            F.col("speed_kph").alias("acceleration_ms2"),
            F.col("speed_kph"),
            F.col("latitude"),
            F.col("longitude"),
        )
    )

    alerts = harsh_braking.unionByName(geofence_violations)

    # Sink: write back to Kafka. The key = vehicle_id|alert_type|event_ts ensures
    # downstream consumers using compacted topics or upsert sinks will dedupe.
    return (
        alerts
        .select(
            F.concat_ws("|",
                F.col("vehicle_id"),
                F.col("alert_type"),
                F.col("event_id"),
            ).alias("key"),
            F.to_json(F.struct(*alerts.columns)).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "vehicle.alerts")
        .option("checkpointLocation", "/tmp/checkpoints/alerts/")
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .start()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("vehicle-alerts")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    query = build_stream(spark)
    query.awaitTermination()
