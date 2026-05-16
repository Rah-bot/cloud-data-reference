"""
Silver layer — fact_sales build.

Explodes order lines from Bronze, joins SCD2 dims to resolve surrogate keys
at order_ts, and writes a deduplicated, deterministic fact table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def build_fact_sales(spark: SparkSession) -> DataFrame:
    orders = spark.table("bronze.retail.orders_raw")
    customers = spark.table("silver.retail.dim_customer")
    products = spark.table("silver.retail.dim_product")

    lines = (
        orders.select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_ts"),
            F.col("store_id"),
            F.col("channel"),
            F.col("currency"),
            F.explode("lines").alias("line"),
        )
        .select(
            "order_id",
            "customer_id",
            "order_ts",
            "store_id",
            "channel",
            "currency",
            F.col("line.line_id").alias("line_id"),
            F.col("line.sku").alias("sku"),
            F.col("line.qty").alias("qty"),
            F.col("line.unit_price").alias("unit_price"),
            (F.col("line.qty") * F.col("line.unit_price")).alias("line_amount"),
        )
    )

    # Point-in-time join to SCD2 dimensions: pick the dim row whose
    # [valid_from, valid_to) window contains order_ts.
    customer_sk = (
        customers.select(
            F.col("customer_id"),
            F.col("dim_customer_sk"),
            F.col("valid_from"),
            F.col("valid_to"),
        )
    )

    product_sk = (
        products.select(
            F.col("sku"),
            F.col("dim_product_sk"),
            F.col("valid_from"),
            F.col("valid_to"),
        )
    )

    fact = (
        lines.alias("l")
        .join(
            customer_sk.alias("c"),
            (F.col("l.customer_id") == F.col("c.customer_id"))
            & (F.col("l.order_ts") >= F.col("c.valid_from"))
            & (F.col("l.order_ts") < F.coalesce(F.col("c.valid_to"), F.lit("9999-12-31").cast("timestamp"))),
            "left",
        )
        .join(
            product_sk.alias("p"),
            (F.col("l.sku") == F.col("p.sku"))
            & (F.col("l.order_ts") >= F.col("p.valid_from"))
            & (F.col("l.order_ts") < F.coalesce(F.col("p.valid_to"), F.lit("9999-12-31").cast("timestamp"))),
            "left",
        )
        .select(
            F.col("l.order_id"),
            F.col("l.line_id"),
            F.col("c.dim_customer_sk"),
            F.col("p.dim_product_sk"),
            F.col("l.store_id"),
            F.col("l.channel"),
            F.col("l.currency"),
            F.col("l.qty"),
            F.col("l.unit_price"),
            F.col("l.line_amount"),
            F.col("l.order_ts"),
            F.to_date("l.order_ts").alias("order_date"),
        )
        .dropDuplicates(["order_id", "line_id"])
    )

    return fact


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_fact_sales").getOrCreate()
    fact = build_fact_sales(spark)

    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_date")
        .saveAsTable("silver.retail.fact_sales")
    )

    spark.sql("OPTIMIZE silver.retail.fact_sales ZORDER BY (dim_customer_sk, dim_product_sk)")
