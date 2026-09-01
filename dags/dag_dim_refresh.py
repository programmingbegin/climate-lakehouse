"""
## dag_dim_refresh

Weekly GeoNames pull + SCD2 merge into silver.dim_city. Single task:
DatabricksRunNowOperator against the `climate_dim_refresh` job (ingest ->
SCD2 merge, chained as two notebook tasks in Databricks -- see
notebooks/05_ingest_city_reference.py and 06_merge_dim_city_scd2.py).
"""

from __future__ import annotations

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag
from pendulum import datetime

DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_JOB_ID = 477982203180726


@dag(
    dag_id="dag_dim_refresh",
    schedule="@weekly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={"owner": "climate-lakehouse", "retries": 2},
    tags=["climate-lakehouse", "phase2"],
)
def dag_dim_refresh():
    DatabricksRunNowOperator(
        task_id="trigger_dim_refresh",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
    )


dag_dim_refresh()
