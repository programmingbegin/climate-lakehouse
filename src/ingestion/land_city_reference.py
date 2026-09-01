"""Spark/Delta landing logic for bronze.city_reference_raw."""

import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.ingestion.geonames import COLUMNS, fetch_cities15000_rows

BRONZE_TABLE = "workspace.bronze.city_reference_raw"

_RAW_SCHEMA = StructType([StructField(c, StringType()) for c in COLUMNS])


def land_city_reference_bronze(spark: SparkSession, rows: list[dict] | None = None, run_id: str | None = None) -> int:
    """Land one GeoNames snapshot into bronze.city_reference_raw. Every field
    lands as a string in this raw layer (GeoNames' native format) and gets
    cast in the Silver transform -- keeps Bronze schema-on-read per the
    Bronze contract."""
    run_id = run_id or str(uuid.uuid4())
    rows = rows if rows is not None else fetch_cities15000_rows()

    df = (
        spark.createDataFrame(rows, schema=_RAW_SCHEMA)
        .withColumn("_source", F.lit("geonames"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_ingest_date", F.current_date())
        # try_cast, not cast -- GeoNames leaves elevation/population/dates blank
        # for plenty of rows, and a blank shouldn't fail the whole landing job.
        .withColumn("geonameid", F.expr("try_cast(geonameid AS BIGINT)"))
        .withColumn("latitude", F.expr("try_cast(latitude AS DOUBLE)"))
        .withColumn("longitude", F.expr("try_cast(longitude AS DOUBLE)"))
        .withColumn("population", F.expr("try_cast(population AS BIGINT)"))
        .withColumn("elevation", F.expr("try_cast(elevation AS INT)"))
        .withColumn("dem", F.expr("try_cast(dem AS INT)"))
        .withColumn("source_modified_date", F.expr("try_cast(source_modified_date AS DATE)"))
        .select(
            "_source", "_ingested_at", "_ingestion_run_id", "_ingest_date",
            "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
            "feature_class", "feature_code", "country_code", "cc2", "admin1_code",
            "admin2_code", "admin3_code", "admin4_code", "population", "elevation",
            "dem", "timezone", "source_modified_date",
        )
    )
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)
    return df.count()
