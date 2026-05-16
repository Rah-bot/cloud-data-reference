"""
Spark Structured Streaming — raw event archive to Delta Lake (cold path).

Sinks every event into a partitioned Delta table on S3 for:
    - 90-day retention to support ML training and incident replay
    - Full backfill if downstream warm/hot paths need to be rebuilt

Idempotent: Spark checkpointing + Delta's transactional log guarantee
exactly-once writes. The cold archive is the source of truth.
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
        .option("startingOffsets", "earliest")    # cold path reads from start
        .option("maxOffsetsPerTrigger", 1_000_000)
        .load()
    )

    payload = F.expr("substring(value, 6, length(value)-5)")

    decoded = (
        raw.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            from_avro(payload, avro_schema).alias("evt"),
        )
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_ts"),
            "evt.*",
        )
        .withColumn("event_date", F.to_date((F.col("event_ts_ms") / 1000).cast("timestamp")))
        .withColumn("event_hour", F.hour((F.col("event_ts_ms") / 1000).cast("timestamp")))
    )

    return (
        decoded.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3://fleet-stream-checkpoints/cold/")
        .option("path", "s3://fleet-stream-cold/vehicle_events/")
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime="60 seconds")
        .start()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("vehicle-cold-archive")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    query = build_stream(spark)
    query.awaitTermination()
