"""Bronze -> Silver transform for NOAA GHCN-Daily historical observations."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "workspace.bronze.ghcn_daily_raw"
SILVER_TABLE = "workspace.silver.historical_daily_weather"

MERGE_KEY_COLUMNS = ["station_id", "obs_date", "element"]

# GHCN stores TMAX/TMIN/PRCP in tenths of their natural unit (degC, mm);
# everything else (SNOW, SNWD, ...) is already in its natural unit.
_TENTHS_ELEMENTS = ("TMAX", "TMIN", "PRCP")


def build_silver_historical_weather(bronze_df: DataFrame) -> DataFrame:
    """Dedup is required here (unlike a pure delta feed): each .dly pull
    re-lands a station's FULL history, not just new days, so re-running the
    backfill creates multiple bronze rows per (station_id, obs_date, element).
    Without this, MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
    -- caught via a real re-run test, not spotted in review."""
    shaped = bronze_df.select(
        "station_id",
        "obs_date",
        F.year("obs_date").alias("obs_year"),
        "element",
        F.when(F.col("element").isin(*_TENTHS_ELEMENTS), F.col("data_value") / 10.0)
        .otherwise(F.col("data_value").cast("double"))
        .alias("value"),
        "mflag", "qflag", "sflag",
        "_source", "_ingested_at",
    )
    window = Window.partitionBy(*MERGE_KEY_COLUMNS).orderBy(F.col("_ingested_at").desc())
    return (
        shaped.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_merged_at", F.current_timestamp())
    )


def merge_silver_historical_weather(spark: SparkSession, bronze_df: DataFrame | None = None) -> int:
    bronze_df = bronze_df if bronze_df is not None else spark.table(BRONZE_TABLE)
    source = build_silver_historical_weather(bronze_df)

    source.createOrReplaceTempView("_silver_historical_weather_source")
    merge_predicate = " AND ".join(f"target.{c} = source.{c}" for c in MERGE_KEY_COLUMNS)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING _silver_historical_weather_source AS source
        ON {merge_predicate}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return source.count()
