"""Silver -> Gold transform: daily per-city climate summary."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_TABLE = "workspace.silver.weather_observations"
DIM_CITY_CURRENT_TABLE = "workspace.gold.dim_city_current"
GOLD_TABLE = "workspace.gold.mart_daily_city_climate_summary"

MERGE_KEY_COLUMNS = ["city_id", "obs_date"]


def build_daily_city_climate_summary(silver_df: DataFrame, city_lookup: DataFrame) -> DataFrame:
    """Aggregate silver.weather_observations to one row per city per day.
    total_precip_mm sums across every poll that landed that day (this table
    is fed by repeated intraday polls, not a single daily reading).
    city_lookup is expected to be gold.dim_city_current (city_id, city_name)
    in production; a small synthetic DataFrame works fine in tests."""
    aggregated = silver_df.withColumn("obs_date", F.to_date("observation_timestamp")).groupBy(
        "city_id", "obs_date"
    ).agg(
        F.avg("temperature_c").alias("avg_temp_c"),
        F.min("temperature_c").alias("min_temp_c"),
        F.max("temperature_c").alias("max_temp_c"),
        F.sum("precipitation_mm").alias("total_precip_mm"),
        F.avg("wind_speed_10m_kmh").alias("avg_wind_speed_kmh"),
        F.lit(None).cast("string").alias("dominant_aqi_category"),  # populated once OpenAQ lands in Phase 2
        F.count(F.lit(1)).alias("row_count"),
    )
    return aggregated.join(city_lookup, "city_id", "left").select(
        "city_id", "city_name", "obs_date", "avg_temp_c", "min_temp_c", "max_temp_c",
        "total_precip_mm", "avg_wind_speed_kmh", "dominant_aqi_category", "row_count",
    )


def merge_daily_city_climate_summary(spark: SparkSession, silver_df: DataFrame | None = None) -> int:
    """MERGE the daily aggregate into gold.mart_daily_city_climate_summary,
    keyed on (city_id, obs_date) -- re-aggregating a day updates that day's
    row instead of duplicating it."""
    silver_df = silver_df if silver_df is not None else spark.table(SILVER_TABLE)
    city_lookup = spark.table(DIM_CITY_CURRENT_TABLE).select("city_id", "city_name")
    source = build_daily_city_climate_summary(silver_df, city_lookup)

    source.createOrReplaceTempView("_gold_daily_summary_source")
    merge_predicate = " AND ".join(f"target.{c} = source.{c}" for c in MERGE_KEY_COLUMNS)
    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} AS target
        USING _gold_daily_summary_source AS source
        ON {merge_predicate}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return source.count()
