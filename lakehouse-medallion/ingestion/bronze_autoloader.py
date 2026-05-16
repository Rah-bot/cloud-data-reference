"""
Bronze layer ingestion via Databricks Auto Loader.

Reads incoming files from cloud storage as a streaming source, infers schema,
and writes append-only Delta tables with audit columns. Idempotent via
checkpointing — re-running picks up where the previous run left off.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def ingest_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    checkpoint_path: str,
    file_format: str = "json",
    schema_location: str | None = None,
    schema: StructType | None = None,
    trigger_interval: str = "1 minute",
) -> None:
    """Stream files from `source_path` into the Bronze Delta table `target_table`.

    Audit columns added:
        _ingest_ts       - timestamp when row was read by Spark
        _source_file     - originating file path
        _ingest_batch_id - microbatch id (set by foreachBatch wrapper if used)

    Args:
        spark: active SparkSession
        source_path: cloud URI (s3://, abfss://, gs://) containing source files
        target_table: 3-part name e.g. `bronze.retail.orders_raw`
        checkpoint_path: dedicated checkpoint dir for this stream
        file_format: json | csv | parquet | avro
        schema_location: path for Auto Loader's inferred schema (required if
            schema is None and format is json/csv)
        schema: optional explicit StructType, skips inference
        trigger_interval: processingTime trigger string
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )

    if schema is not None:
        reader = reader.schema(schema)
    elif schema_location is not None:
        reader = reader.option("cloudFiles.schemaLocation", schema_location)
    else:
        raise ValueError("Provide either `schema` or `schema_location`.")

    df = (
        reader.load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger_interval)
        .toTable(target_table)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("bronze_autoloader").getOrCreate()

    ingest_to_bronze(
        spark,
        source_path="s3://example-landing/retail/orders/",
        target_table="bronze.retail.orders_raw",
        checkpoint_path="s3://example-checkpoints/bronze/orders_raw/",
        schema_location="s3://example-checkpoints/bronze/_schemas/orders_raw/",
        file_format="json",
    )
