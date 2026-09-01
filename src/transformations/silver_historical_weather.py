"""Bronze -> Silver transform for NOAA GHCN-Daily historical observations."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.quality.dq_checks import DQCheck, apply_checks, enforce_dq_gate

BRONZE_TABLE = "workspace.bronze.ghcn_daily_raw"
SILVER_TABLE = "workspace.silver.historical_daily_weather"
QUARANTINE_TABLE = "workspace.silver.dq_quarantine"

MERGE_KEY_COLUMNS = ["station_id", "obs_date", "element"]
DQ_GATE_THRESHOLD = 0.02  # fail the run (block downstream use) if >2% of rows are quarantined

# GHCN stores TMAX/TMIN/PRCP in tenths of their natural unit (degC, mm);
# everything else (SNOW, SNWD, ...) is already in its natural unit.
_TENTHS_ELEMENTS = ("TMAX", "TMIN", "PRCP")

# No referential-integrity check against a station dimension here -- there's
# no dim_station table yet (a real gap, noted in the Gold-facts design), so
# this stays scoped to what's actually checkable: not-null keys and
# per-element range sanity.
HISTORICAL_WEATHER_CHECKS = [
    DQCheck("not_null_station_id", "station_id IS NOT NULL"),
    DQCheck("not_null_obs_date", "obs_date IS NOT NULL"),
    DQCheck("not_null_value", "value IS NOT NULL"),
    DQCheck("temp_in_range", "element NOT IN ('TMAX', 'TMIN') OR value BETWEEN -90 AND 60"),
    DQCheck("precip_non_negative", "element != 'PRCP' OR value >= 0"),
]


def _shape(bronze_df: DataFrame) -> DataFrame:
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
    )


def split_valid_and_quarantine(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    return apply_checks(_shape(bronze_df), HISTORICAL_WEATHER_CHECKS)


def build_silver_historical_weather(bronze_df: DataFrame) -> DataFrame:
    """Dedup is required here (unlike a pure delta feed): each .dly pull
    re-lands a station's FULL history, not just new days, so re-running the
    backfill creates multiple bronze rows per (station_id, obs_date, element).
    Without this, MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
    -- caught via a real re-run test, not spotted in review."""
    valid, _ = split_valid_and_quarantine(bronze_df)
    window = Window.partitionBy(*MERGE_KEY_COLUMNS).orderBy(F.col("_ingested_at").desc())
    return (
        valid.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_merged_at", F.current_timestamp())
    )


def _quarantine_rows(quarantine_df: DataFrame, source_table: str) -> DataFrame:
    key_expr = F.concat_ws("|", F.col("station_id"), F.col("obs_date").cast("string"), F.col("element"))
    return quarantine_df.select(
        F.lit(source_table).alias("source_table"),
        key_expr.alias("natural_key"),
        F.col("failure_reason"),
        F.to_json(F.struct(*[c for c in quarantine_df.columns if c != "failure_reason"])).alias("raw_record"),
        F.lit(None).cast("string").alias("_ingestion_run_id"),
        F.current_timestamp().alias("quarantined_at"),
    )


def merge_silver_historical_weather(spark: SparkSession, bronze_df: DataFrame | None = None) -> dict:
    bronze_df = bronze_df if bronze_df is not None else spark.table(BRONZE_TABLE)

    _, quarantine_raw = split_valid_and_quarantine(bronze_df)
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

    quarantine_count = quarantine_raw.count()
    if quarantine_count > 0:
        _quarantine_rows(quarantine_raw, "silver.historical_daily_weather").write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

    merged_count = source.count()
    enforce_dq_gate(merged_count + quarantine_count, quarantine_count, DQ_GATE_THRESHOLD, "silver.historical_daily_weather")

    return {"merged_rows": merged_count, "quarantined_rows": quarantine_count}
