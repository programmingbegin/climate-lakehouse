# Databricks notebook source
# Thin wrapper: MERGE bronze.ghcn_daily_raw into
# silver.historical_daily_weather. Real logic lives in
# src/transformations/silver_historical_weather.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.transformations.silver_historical_weather import merge_silver_historical_weather

# COMMAND ----------

rows = merge_silver_historical_weather(spark)
print(f"Merged {rows} rows into silver.historical_daily_weather")
