# Databricks notebook source
# Thin wrapper: pull GHCN-Daily .dly data and land into bronze.ghcn_daily_raw.
# Real logic lives in src/ingestion/ghcn.py and src/ingestion/land_ghcn.py.
#
# Widgets (all optional -- defaults reproduce the old "all pilot stations,
# full history" behavior used by the climate_historical_backfill job):
#   station_id: a single GHCN station id (for dag_historical_backfill's
#               per-station dynamic task mapping). Blank = all PILOT_STATIONS.
#   start_date / end_date: YYYY-MM-DD, inclusive. Blank = no filter.

# COMMAND ----------

import sys, os

repo_root = os.path.abspath(os.path.join(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()), ".."))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.ingestion.ghcn import PILOT_STATIONS, fetch_pilot_stations_rows
from src.ingestion.land_ghcn import land_ghcn_daily_bronze

# COMMAND ----------

dbutils.widgets.text("station_id", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

station_id = dbutils.widgets.get("station_id").strip()
start_date = dbutils.widgets.get("start_date").strip() or None
end_date = dbutils.widgets.get("end_date").strip() or None

stations = [s for s in PILOT_STATIONS if s["station_id"] == station_id] if station_id else PILOT_STATIONS
if station_id and not stations:
    raise ValueError(f"Unknown station_id: {station_id}")

# COMMAND ----------

rows = fetch_pilot_stations_rows(stations=stations, start_date=start_date, end_date=end_date)
rows_written = land_ghcn_daily_bronze(spark, rows=rows)
print(f"Landed {rows_written} rows into bronze.ghcn_daily_raw "
      f"(stations={[s['station_id'] for s in stations]}, start_date={start_date}, end_date={end_date})")
