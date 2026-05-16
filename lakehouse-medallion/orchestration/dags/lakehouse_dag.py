"""
Lakehouse orchestration DAG.

Daily flow:
    1. Bronze ingest (continuous, validated here)
    2. Quality gate on Bronze
    3. Silver builds (dim + fact)
    4. Quality gate on Silver
    5. Gold (dbt build)
    6. Publish freshness + lineage to OpenMetadata
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.providers.great_expectations.operators.great_expectations import (
    GreatExpectationsOperator,
)


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CLUSTER = {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 4,
    "data_security_mode": "USER_ISOLATION",
}


with DAG(
    dag_id="lakehouse_daily",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "retail"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_quality = GreatExpectationsOperator(
        task_id="quality_gate_bronze_orders",
        data_context_root_dir="governance/",
        checkpoint_name="orders_bronze_checkpoint",
        fail_task_on_validation_failure=True,
    )

    silver_dim_customer = DatabricksSubmitRunOperator(
        task_id="silver_dim_customer",
        new_cluster=CLUSTER,
        spark_python_task={
            "python_file": "dbfs:/repos/lakehouse/transforms/silver/dim_customer_scd2.py"
        },
    )

    silver_fact_sales = DatabricksSubmitRunOperator(
        task_id="silver_fact_sales",
        new_cluster=CLUSTER,
        spark_python_task={
            "python_file": "dbfs:/repos/lakehouse/transforms/silver/fact_sales.py"
        },
    )

    silver_quality = GreatExpectationsOperator(
        task_id="quality_gate_silver_fact_sales",
        data_context_root_dir="governance/",
        checkpoint_name="fact_sales_silver_checkpoint",
        fail_task_on_validation_failure=True,
    )

    gold_dbt_build = DatabricksSubmitRunOperator(
        task_id="gold_dbt_build",
        new_cluster=CLUSTER,
        spark_python_task={
            "python_file": "dbfs:/repos/lakehouse/transforms/gold/run_dbt.py"
        },
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> bronze_quality
        >> silver_dim_customer
        >> silver_fact_sales
        >> silver_quality
        >> gold_dbt_build
        >> end
    )
