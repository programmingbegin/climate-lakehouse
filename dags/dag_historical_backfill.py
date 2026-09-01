"""
## dag_historical_backfill

Parameterized by date range (`start_date`/`end_date` DAG params, YYYY-MM-DD),
dynamically mapped over the pilot GHCN stations via Airflow's `.expand()` --
one DatabricksRunNowOperator task instance per station instead of a
hardcoded loop, so adding a station is a one-line config change.

Each mapped task triggers the `climate_historical_backfill` job
(notebooks/08_ingest_ghcn_bronze.py -> 09_merge_silver_historical_weather.py)
with that station's id and the requested date range passed as notebook
widget values.

Safe to re-run for any date range or logical date: Bronze is append-only and
Silver dedupes on (station_id, obs_date, element) before the MERGE (see
src/transformations/silver_historical_weather.py) -- verified live by
running this same job twice back to back and confirming Silver's row count
held steady while Bronze grew.
"""

from __future__ import annotations

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag
from pendulum import datetime

DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_JOB_ID = 371319992269926

PILOT_STATION_IDS = [
    "USW00094728",  # New York City -- Central Park
    "UKM00003772",  # London -- Heathrow
    "JA000047662",  # Tokyo
    "IN012070800",  # Mumbai -- Santacruz
    "KEM00063741",  # Nairobi -- Dagoretti
]


@dag(
    dag_id="dag_historical_backfill",
    schedule=None,  # triggered manually / on demand with a date range, not on a fixed cadence
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={"owner": "climate-lakehouse", "retries": 2},
    params={"start_date": "2020-01-01", "end_date": "2020-12-31"},
    tags=["climate-lakehouse", "phase2"],
)
def dag_historical_backfill():
    DatabricksRunNowOperator.partial(
        task_id="backfill_station",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
    ).expand(
        notebook_params=[
            {
                "station_id": station_id,
                "start_date": "{{ params.start_date }}",
                "end_date": "{{ params.end_date }}",
            }
            for station_id in PILOT_STATION_IDS
        ]
    )


dag_historical_backfill()
