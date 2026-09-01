"""
## dag_realtime_ingest

Phase 1 MVP spine: Open-Meteo -> Bronze -> Silver -> Gold, end to end.

Two tasks, matching the build guide's Phase 1 design:

1. `preflight_check_open_meteo`: a lightweight TaskFlow task that calls
   Open-Meteo directly (no landing) so a broken API fails fast, before
   spending Databricks compute. Deliberately self-contained (plain
   `requests`, no import of climate-lakehouse's `src/`) since the Airflow
   image and the Databricks job are separate deployment targets.
2. `trigger_bronze_silver_gold`: DatabricksRunNowOperator against the
   `climate_bronze_silver_gold` job, which does the real Bronze landing
   (Spark/Delta), the Silver MERGE, and the Gold aggregation as three
   chained notebook tasks. Landing logic lives in exactly one place (the
   Databricks job) rather than being duplicated between Airflow and
   Databricks.
"""

from __future__ import annotations

import requests
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag, task
from pendulum import datetime

DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_JOB_ID = 41207891199807

PREFLIGHT_CITY = {"city_name": "New York City", "latitude": 40.71427, "longitude": -74.00597}


@dag(
    dag_id="dag_realtime_ingest",
    schedule="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={"owner": "climate-lakehouse", "retries": 2},
    tags=["climate-lakehouse", "phase1"],
)
def dag_realtime_ingest():
    @task
    def preflight_check_open_meteo() -> dict:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": PREFLIGHT_CITY["latitude"],
                "longitude": PREFLIGHT_CITY["longitude"],
                "current": "temperature_2m",
                "timezone": "UTC",
            },
            timeout=10,
        )
        response.raise_for_status()
        current = response.json().get("current", {})
        print(f"Open-Meteo reachable, sample reading for {PREFLIGHT_CITY['city_name']}: {current}")
        return current

    trigger_bronze_silver_gold = DatabricksRunNowOperator(
        task_id="trigger_bronze_silver_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
    )

    preflight_check_open_meteo() >> trigger_bronze_silver_gold


dag_realtime_ingest()
