# Databricks notebook source
# Thin wrapper: fetch Open-Meteo current weather for the Phase 1 pilot cities
# and land it into bronze.weather_raw. All real logic lives in
# src/ingestion/open_meteo.py (fetch/parse) and src/ingestion/land_bronze.py (Spark write)
# so it stays unit-testable per /tests/unit.

# COMMAND ----------

import sys, os

# When this repo is synced as a Databricks Repo/Git folder, the repo root is the
# notebook's parent-of-parent dir — add it so `import src...` resolves.
repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.ingestion.land_bronze import land_weather_bronze

# COMMAND ----------

rows_written = land_weather_bronze(spark)
print(f"Landed {rows_written} rows into bronze.weather_raw")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.bronze.weather_raw ORDER BY _ingested_at DESC LIMIT 10"))
