# Databricks notebook source
# Thin wrapper: MERGE bronze.weather_raw into silver.weather_observations.
# Real logic lives in src/transformations/silver_weather.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.transformations.silver_weather import merge_silver_weather_observations

# COMMAND ----------

result = merge_silver_weather_observations(spark)
print(f"Merged {result['merged_rows']} rows, quarantined {result['quarantined_rows']} rows")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.silver.weather_observations ORDER BY observation_timestamp DESC LIMIT 10"))
