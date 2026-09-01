# Databricks notebook source
# Thin wrapper: SCD2 MERGE of bronze.city_reference_raw into silver.dim_city.
# Real logic lives in src/transformations/silver_dim_city.py.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.transformations.silver_dim_city import merge_dim_city_scd2

# COMMAND ----------

result = merge_dim_city_scd2(spark)
print(f"SCD2 merge: {result['changed_or_new_cities']} cities changed or newly inserted")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.silver.dim_city ORDER BY city_id, effective_start_ts LIMIT 20"))
