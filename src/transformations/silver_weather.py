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

from src.quality.dq_checks import DQCheck, apply_checks, enforce_dq_gate

BRONZE_TABLE = "workspace.bronze.weather_raw"
SILVER_TABLE = "workspace.silver.weather_observations"
DIM_CITY_CURRENT_TABLE = "workspace.gold.dim_city_current"
QUARANTINE_TABLE = "workspace.silver.dq_quarantine"

MERGE_KEY_COLUMNS = ["city_id", "observation_timestamp", "_source"]
DQ_GATE_THRESHOLD = 0.02  # fail the run (block Gold promotion) if >2% of rows are quarantined

WEATHER_CHECKS = [
    DQCheck("not_null_city_id", "city_id IS NOT NULL"),
    DQCheck("not_null_observation_timestamp", "observation_timestamp IS NOT NULL"),
    DQCheck("temperature_in_range", "temperature_c IS NULL OR temperature_c BETWEEN -90 AND 60"),
    DQCheck("freshness_48h", "observation_timestamp IS NULL OR observation_timestamp >= current_timestamp() - INTERVAL 48 HOURS"),
    DQCheck("city_exists_in_dim_city", "_city_exists = true"),
]

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


def split_valid_and_quarantine(bronze_df: DataFrame, dim_city_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Named DQ checks (see WEATHER_CHECKS): not-null keys, a temperature
    range check, a freshness check, and referential integrity against
    dim_city_current -- a reading for a city not in the dimension gets
    quarantined with a named reason instead of silently landing with a
    dangling FK."""
    shaped = (
        _shape(bronze_df)
        .join(dim_city_ids.withColumnRenamed("city_id", "_dim_city_id"),
              F.col("city_id") == F.col("_dim_city_id"), "left")
        .withColumn("_city_exists", F.col("_dim_city_id").isNotNull())
        .drop("_dim_city_id")
    )
    valid, invalid = apply_checks(shaped, WEATHER_CHECKS)
    return valid.drop("_city_exists"), invalid.drop("_city_exists")


def build_silver_weather_observations(bronze_df: DataFrame, dim_city_ids: DataFrame) -> DataFrame:
    """Shape + dedupe valid bronze rows into the silver.weather_observations
    contract. Dedup keeps the most-recently-ingested row per
    (city_id, observation_timestamp, _source), so replaying/re-running the
    same ingest batch is safe."""
    valid, _ = split_valid_and_quarantine(bronze_df, dim_city_ids)
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
        F.col("failure_reason"),
        F.to_json(F.struct(*[c for c in quarantine_df.columns if c != "failure_reason"])).alias("raw_record"),
        F.col("_ingestion_run_id"),
        F.current_timestamp().alias("quarantined_at"),
    )


def merge_silver_weather_observations(spark: SparkSession, bronze_df: DataFrame | None = None) -> dict:
    """MERGE the deduped, quality-passing bronze rows into
    silver.weather_observations keyed on (city_id, observation_timestamp,
    _source) -- upsert, never a blind append or overwrite, so re-running this
    for the same data is safe. Failing rows land in silver.dq_quarantine
    with a named reason instead of being silently dropped. The Silver MERGE
    still runs for whatever passed; the DQ gate fires *after*, so a bad batch
    fails this task (blocking the downstream Gold tasks) rather than
    silently promoting partial/bad data."""
    bronze_df = bronze_df if bronze_df is not None else spark.table(BRONZE_TABLE)
    dim_city_ids = spark.table(DIM_CITY_CURRENT_TABLE).select("city_id")

    _, quarantine_raw = split_valid_and_quarantine(bronze_df, dim_city_ids)
    merged_source = build_silver_weather_observations(bronze_df, dim_city_ids)

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

    merged_count = merged_source.count()
    enforce_dq_gate(merged_count + quarantine_count, quarantine_count, DQ_GATE_THRESHOLD, "silver.weather_observations")

    return {"merged_rows": merged_count, "quarantined_rows": quarantine_count}
