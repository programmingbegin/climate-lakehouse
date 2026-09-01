"""Bronze -> Silver transform for weather observations.

Split into a pure shaping function (build_silver_weather_observations /
split_valid_and_quarantine — take a DataFrame, return a DataFrame, no Spark
session state) and a thin execution function (merge_silver_weather_observations)
that does the actual MERGE INTO, per the /tests/unit split described in the
build guide's Phase 3.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "workspace.bronze.weather_raw"
SILVER_TABLE = "workspace.silver.weather_observations"
QUARANTINE_TABLE = "workspace.silver.dq_quarantine"

MERGE_KEY_COLUMNS = ["city_id", "observation_timestamp", "_source"]

_RENAME_MAP = {
    "observation_time": "observation_timestamp",
    "temperature_2m": "temperature_c",
    "apparent_temperature": "apparent_temperature_c",
    "relative_humidity_2m": "relative_humidity_pct",
    "precipitation": "precipitation_mm",
    "rain": "rain_mm",
    "showers": "showers_mm",
    "snowfall": "snowfall_cm",
    "cloud_cover": "cloud_cover_pct",
    "pressure_msl": "pressure_msl_hpa",
    "surface_pressure": "surface_pressure_hpa",
    "wind_speed_10m": "wind_speed_10m_kmh",
    "wind_direction_10m": "wind_direction_10m_deg",
    "wind_gusts_10m": "wind_gusts_10m_kmh",
}


def _shape(bronze_df: DataFrame) -> DataFrame:
    """Rename bronze columns to their silver names. No unit conversion needed --
    Open-Meteo's defaults (Celsius, km/h, mm, hPa) already match the silver schema."""
    df = bronze_df
    for source_col, target_col in _RENAME_MAP.items():
        df = df.withColumnRenamed(source_col, target_col)
    return df.select(
        "city_id", "observation_timestamp", "temperature_c", "apparent_temperature_c",
        "relative_humidity_pct", "precipitation_mm", "rain_mm", "showers_mm", "snowfall_cm",
        "weather_code", "cloud_cover_pct", "pressure_msl_hpa", "surface_pressure_hpa",
        "wind_speed_10m_kmh", "wind_direction_10m_deg", "wind_gusts_10m_kmh", "is_day",
        "_source", "_ingested_at", "_ingestion_run_id",
    )


def split_valid_and_quarantine(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Inline DQ gate for Phase 1: null check on city_id, range check on
    temperature_c (-90C to 60C covers every recorded surface temperature).
    Real Great Expectations suites replace this in Phase 2."""
    shaped = _shape(bronze_df).withColumn(
        "_failure_reason",
        F.when(F.col("city_id").isNull(), F.lit("null city_id"))
        .when(F.col("temperature_c").isNull(), F.lit("null temperature_c"))
        .when(~F.col("temperature_c").between(-90, 60), F.lit("temperature_c out of range [-90, 60]"))
        .otherwise(F.lit(None)),
    )
    valid = shaped.filter(F.col("_failure_reason").isNull()).drop("_failure_reason")
    quarantine = shaped.filter(F.col("_failure_reason").isNotNull())
    return valid, quarantine


def build_silver_weather_observations(bronze_df: DataFrame) -> DataFrame:
    """Shape + dedupe valid bronze rows into the silver.weather_observations
    contract. Dedup keeps the most-recently-ingested row per
    (city_id, observation_timestamp, _source), so replaying/re-running the
    same ingest batch is safe."""
    valid, _ = split_valid_and_quarantine(bronze_df)
    window = Window.partitionBy(*MERGE_KEY_COLUMNS).orderBy(F.col("_ingested_at").desc())
    return (
        valid.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_ingestion_run_id")
        .withColumn("_merged_at", F.current_timestamp())
    )


def _quarantine_rows(quarantine_df: DataFrame, source_table: str) -> DataFrame:
    key_expr = F.concat_ws("|", F.col("city_id").cast("string"), F.col("observation_timestamp").cast("string"), F.col("_source"))
    return quarantine_df.select(
        F.lit(source_table).alias("source_table"),
        key_expr.alias("natural_key"),
        F.col("_failure_reason").alias("failure_reason"),
        F.to_json(F.struct(*[c for c in quarantine_df.columns if c != "_failure_reason"])).alias("raw_record"),
        F.col("_ingestion_run_id"),
        F.current_timestamp().alias("quarantined_at"),
    )


def merge_silver_weather_observations(spark: SparkSession, bronze_df: DataFrame | None = None) -> dict:
    """MERGE the deduped, quality-passing bronze rows into
    silver.weather_observations keyed on (city_id, observation_timestamp,
    _source) -- upsert, never a blind append or overwrite, so re-running this
    for the same data is safe. Failing rows land in silver.dq_quarantine
    instead of being silently dropped."""
    bronze_df = bronze_df if bronze_df is not None else spark.table(BRONZE_TABLE)

    _, quarantine_raw = split_valid_and_quarantine(bronze_df)
    merged_source = build_silver_weather_observations(bronze_df)

    merged_source.createOrReplaceTempView("_silver_weather_source")
    merge_predicate = " AND ".join(f"target.{c} = source.{c}" for c in MERGE_KEY_COLUMNS)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING _silver_weather_source AS source
        ON {merge_predicate}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    quarantine_count = quarantine_raw.count()
    if quarantine_count > 0:
        _quarantine_rows(quarantine_raw, "silver.weather_observations").write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

    return {"merged_rows": merged_source.count(), "quarantined_rows": quarantine_count}
