# Databricks notebook source
# Thin wrapper: fetch the GeoNames cities15000 snapshot and land it into
# bronze.city_reference_raw. Real logic lives in
# src/ingestion/land_city_reference.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.ingestion.land_city_reference import land_city_reference_bronze

# COMMAND ----------

rows_written = land_city_reference_bronze(spark)
print(f"Landed {rows_written} rows into bronze.city_reference_raw")
