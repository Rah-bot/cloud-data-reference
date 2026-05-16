"""
Quality-gate DAG.

Runs Great Expectations suites against curated datasets. A failure here
prevents the dataset from being marked 'certified' in OpenMetadata and
blocks downstream consumers that gate on certification.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


SUITES_DIR = Path(__file__).resolve().parents[1] / "quality" / "suites"


def run_suite(suite_name: str) -> None:
    """Stub that would call Great Expectations against the named suite."""
    import great_expectations as ge

    context = ge.get_context()
    checkpoint = context.get_checkpoint(suite_name)
    result = checkpoint.run()
    if not result.success:
        raise RuntimeError(f"Suite {suite_name} failed validations")


def certify(dataset_fqn: str) -> None:
    """Mark dataset as certified in OpenMetadata."""
    # In production: PATCH the dataset entity with tier=Tier.Gold + certification tag.
    print(f"Certified {dataset_fqn}")


DEFAULT_ARGS = {
    "owner": "data-governance",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="quality_gate",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["governance", "quality"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for suite_path in SUITES_DIR.glob("*_suite.json"):
        name = suite_path.stem.replace("_suite", "")
        run_t = PythonOperator(
            task_id=f"run_{name}",
            python_callable=run_suite,
            op_kwargs={"suite_name": suite_path.stem},
        )
        certify_t = PythonOperator(
            task_id=f"certify_{name}",
            python_callable=certify,
            op_kwargs={"dataset_fqn": f"snowflake.prod.{name}"},
        )
        start >> run_t >> certify_t >> end
