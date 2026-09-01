# Databricks notebook source
# Thin wrapper: build gold.fact_weather_observation from
# silver.weather_observations, FK'd against gold.dim_city_current.
# Real logic lives in src/transformations/gold_facts.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.transformations.gold_facts import merge_fact_weather_observation

# COMMAND ----------

rows = merge_fact_weather_observation(spark)
print(f"Merged {rows} rows into gold.fact_weather_observation")
