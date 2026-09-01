"""Spark/Delta landing logic for bronze.ghcn_daily_raw."""

import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.ingestion.ghcn import PILOT_STATIONS, fetch_pilot_stations_rows

BRONZE_TABLE = "workspace.bronze.ghcn_daily_raw"

_RAW_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("obs_date", StringType()),
    StructField("element", StringType()),
    StructField("data_value", IntegerType()),
    StructField("mflag", StringType()),
    StructField("qflag", StringType()),
    StructField("sflag", StringType()),
])


def land_ghcn_daily_bronze(spark: SparkSession, rows: list[dict] | None = None, run_id: str | None = None) -> int:
    run_id = run_id or str(uuid.uuid4())
    rows = rows if rows is not None else fetch_pilot_stations_rows()

    df = (
        spark.createDataFrame(rows, schema=_RAW_SCHEMA)
        .withColumn("obs_date", F.to_date("obs_date"))
        .withColumn("_source", F.lit("noaa-ghcn"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_source_file", F.concat(F.col("station_id"), F.lit(".dly")))
        .withColumn("_ingest_date", F.current_date())
        .withColumn("obs_time", F.lit(None).cast("string"))
        .select(
            "_source", "_ingested_at", "_ingestion_run_id", "_source_file", "_ingest_date",
            "station_id", "obs_date", "element", "data_value", "mflag", "qflag", "sflag", "obs_time",
        )
    )
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    return df.count()
