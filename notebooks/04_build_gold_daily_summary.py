# Databricks notebook source
# Thin wrapper: aggregate silver.weather_observations into
# gold.mart_daily_city_climate_summary. Real logic lives in
# src/transformations/gold_climate_summary.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.transformations.gold_climate_summary import merge_daily_city_climate_summary

# COMMAND ----------

rows = merge_daily_city_climate_summary(spark)
print(f"Merged {rows} rows into gold.mart_daily_city_climate_summary")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.gold.mart_daily_city_climate_summary ORDER BY obs_date DESC, city_name"))
