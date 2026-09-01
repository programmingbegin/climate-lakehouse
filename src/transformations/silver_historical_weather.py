"""Bronze -> Silver transform for NOAA GHCN-Daily historical observations."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.ghcn_daily_raw"
SILVER_TABLE = "workspace.silver.historical_daily_weather"

MERGE_KEY_COLUMNS = ["station_id", "obs_date", "element"]

# GHCN stores TMAX/TMIN/PRCP in tenths of their natural unit (degC, mm);
# everything else (SNOW, SNWD, ...) is already in its natural unit.
_TENTHS_ELEMENTS = ("TMAX", "TMIN", "PRCP")


def build_silver_historical_weather(bronze_df: DataFrame) -> DataFrame:
    return bronze_df.select(
        "station_id",
        "obs_date",
        F.year("obs_date").alias("obs_year"),
        "element",
        F.when(F.col("element").isin(*_TENTHS_ELEMENTS), F.col("data_value") / 10.0)
        .otherwise(F.col("data_value").cast("double"))
        .alias("value"),
        "mflag", "qflag", "sflag",
        "_source", "_ingested_at",
    ).withColumn("_merged_at", F.current_timestamp())


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
