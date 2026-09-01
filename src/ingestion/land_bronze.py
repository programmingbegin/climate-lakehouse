"""Spark/Delta landing logic for bronze.weather_raw. Keeps the Spark-specific code
separate from open_meteo.py so the fetch/parse logic stays unit-testable without a
cluster or Spark session."""

import json
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType, BooleanType,
)

from src.ingestion.open_meteo import PILOT_CITIES, fetch_current_weather_batch, parse_current_weather

BRONZE_TABLE = "workspace.bronze.weather_raw"

_RAW_ROW_SCHEMA = StructType([
    StructField("_ingestion_run_id", StringType()),
    StructField("city_id", LongType()),
    StructField("raw_json", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("elevation", DoubleType()),
    StructField("observation_time_raw", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("apparent_temperature", DoubleType()),
    StructField("relative_humidity_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("rain", DoubleType()),
    StructField("showers", DoubleType()),
    StructField("snowfall", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("cloud_cover", DoubleType()),
    StructField("pressure_msl", DoubleType()),
    StructField("surface_pressure", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("wind_direction_10m", DoubleType()),
    StructField("wind_gusts_10m", DoubleType()),
    StructField("is_day", BooleanType()),
])


def _row_from_payload(payload: dict, run_id: str) -> dict:
    parsed = parse_current_weather(payload)
    return {
        "_ingestion_run_id": run_id,
        "city_id": parsed["city_id"],
        "raw_json": json.dumps(payload),
        "latitude": parsed["latitude"],
        "longitude": parsed["longitude"],
        "elevation": parsed["elevation"],
        "observation_time_raw": parsed["observation_time"],
        "temperature_2m": parsed["temperature_2m"],
        "apparent_temperature": parsed["apparent_temperature"],
        "relative_humidity_2m": parsed["relative_humidity_2m"],
        "precipitation": parsed["precipitation"],
        "rain": parsed["rain"],
        "showers": parsed["showers"],
        "snowfall": parsed["snowfall"],
        "weather_code": parsed["weather_code"],
        "cloud_cover": parsed["cloud_cover"],
        "pressure_msl": parsed["pressure_msl"],
        "surface_pressure": parsed["surface_pressure"],
        "wind_speed_10m": parsed["wind_speed_10m"],
        "wind_direction_10m": parsed["wind_direction_10m"],
        "wind_gusts_10m": parsed["wind_gusts_10m"],
        "is_day": parsed["is_day"],
    }


def land_weather_bronze(spark: SparkSession, cities: list[dict] = PILOT_CITIES, run_id: str | None = None) -> int:
    """Fetch current weather for `cities` and append the raw payloads to
    bronze.weather_raw. Append-only, matching the Bronze contract — never
    overwrites. Returns the number of rows written."""
    run_id = run_id or str(uuid.uuid4())
    payloads = fetch_current_weather_batch(cities)
    rows = [_row_from_payload(p, run_id) for p in payloads]

    df = (
        spark.createDataFrame(rows, schema=_RAW_ROW_SCHEMA)
        .withColumn("_source", F.lit("open-meteo"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("observation_time", F.to_timestamp("observation_time_raw"))
        .select(
            "_source", "_ingested_at", "_ingestion_run_id", "_ingest_date", "city_id", "raw_json",
            "latitude", "longitude", "elevation", "observation_time",
            "temperature_2m", "apparent_temperature", "relative_humidity_2m",
            "precipitation", "rain", "showers", "snowfall", "weather_code",
            "cloud_cover", "pressure_msl", "surface_pressure",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "is_day",
        )
    )
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    return df.count()
