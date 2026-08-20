# Climate & Air Quality Intelligence Lakehouse

A Databricks + Airflow lakehouse pipeline that ingests weather (Open-Meteo, NOAA GHCN-Daily) and air-quality (OpenAQ) data through Bronze/Silver/Gold layers on Delta Lake, with SCD2 dimensional modeling, data-quality gating, and a Databricks SQL dashboard.

Repo scaffolded per the build guide/project plan; implementation in progress.

## Layout

```
/dags/                    Airflow DAGs
/src/transformations/     PySpark Silver/Gold logic (importable, unit-testable)
/tests/unit/              pytest + chispa tests for transformation functions
/tests/dag_validation/    DAG import/cycle/schedule tests
/notebooks/               Databricks notebooks (thin wrappers calling /src)
/bundle/                  Databricks Asset Bundle config (databricks.yml)
/dashboards/               exported Databricks SQL dashboard definitions + SQL
/docs/adr/                Architecture Decision Records
```
