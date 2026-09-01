"""Silver -> Gold fact table builds. Facts foreign-key against
gold.dim_city_current (the current-state view over silver.dim_city's SCD2
rows) rather than a hardcoded city list."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_WEATHER_TABLE = "workspace.silver.weather_observations"
DIM_CITY_CURRENT_TABLE = "workspace.gold.dim_city_current"
FACT_WEATHER_TABLE = "workspace.gold.fact_weather_observation"

MERGE_KEY_COLUMNS = ["city_id", "observation_timestamp", "_source"]


def build_fact_weather_observation(silver_df: DataFrame, dim_city_current_df: DataFrame) -> DataFrame:
    """Grain: one row per city_id x observation_timestamp x _source. Inner-joins
    dim_city_current so a reading for a city not yet in the dimension is
    dropped rather than landing with a dangling FK -- surfaces the ordering
    dependency (dim_city must be refreshed before facts) instead of hiding it."""
    return silver_df.join(
        dim_city_current_df.select("city_id"),
        on="city_id",
        how="inner",
    ).withColumn("date_sk", F.date_format("observation_timestamp", "yyyyMMdd").cast("int")).select(
        "city_id", "date_sk", "observation_timestamp", "temperature_c", "apparent_temperature_c",
        "relative_humidity_pct", "precipitation_mm", "wind_speed_10m_kmh", "weather_code", "_source",
    )


def merge_fact_weather_observation(spark: SparkSession, silver_df: DataFrame | None = None) -> int:
    silver_df = silver_df if silver_df is not None else spark.table(SILVER_WEATHER_TABLE)
    dim_city_current = spark.table(DIM_CITY_CURRENT_TABLE)
    source = build_fact_weather_observation(silver_df, dim_city_current)

    source.createOrReplaceTempView("_fact_weather_source")
    merge_predicate = " AND ".join(f"target.{c} = source.{c}" for c in MERGE_KEY_COLUMNS)
    # Both UPDATE SET * and INSERT * fail here: they require the source to
    # cover every target column including the generated identity column
    # weather_observation_sk, which by definition it never will. Explicit
    # column lists sidestep it -- the identity column is simply never
    # mentioned, so Delta auto-generates it on insert.
    update_columns = [c for c in source.columns if c not in ("city_id", "observation_timestamp", "_source")]
    update_set = ", ".join(f"target.{c} = source.{c}" for c in update_columns)
    insert_columns = ", ".join(source.columns)
    insert_values = ", ".join(f"source.{c}" for c in source.columns)
    spark.sql(f"""
        MERGE INTO {FACT_WEATHER_TABLE} AS target
        USING _fact_weather_source AS source
        ON {merge_predicate}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """)
    return source.count()
