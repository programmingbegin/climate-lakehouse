# Databricks notebook source
# Thin wrapper: pull GHCN-Daily .dly files for the pilot stations and land
# into bronze.ghcn_daily_raw. Real logic lives in src/ingestion/land_ghcn.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.ingestion.land_ghcn import land_ghcn_daily_bronze

# COMMAND ----------

rows_written = land_ghcn_daily_bronze(spark)
print(f"Landed {rows_written} rows into bronze.ghcn_daily_raw")
