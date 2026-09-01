"""Bronze -> Silver SCD Type 2 merge for dim_city.

Single atomic MERGE using the standard Delta "mergeKey" technique: each
changed/new source row is staged twice -- once with mergeKey = city_id (to
match and close the existing current row) and once with mergeKey = NULL (so
it never matches an existing row and always falls through to the INSERT
branch, opening the new current version). This is the documented Databricks
SCD2 pattern (see Delta Lake MERGE INTO docs) rather than two separate
statements, so the close-old/open-new happens as one transaction.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.city_reference_raw"
SILVER_TABLE = "workspace.silver.dim_city"

# Attributes GeoNames snapshots legitimately drift on -- population estimates
# update, timezone/UTC-offset corrections happen, elevation gets corrected.
TRACKED_ATTRIBUTES = ["population", "elevation_m", "timezone"]

_SOURCE_COLUMNS = [
    "city_id", "city_name", "asciiname", "country_code", "admin1_code",
    "latitude", "longitude", "population", "elevation_m", "timezone",
]


def _latest_snapshot(bronze_df: DataFrame) -> DataFrame:
    """Isolate exactly one landed snapshot (by _ingestion_run_id) -- SCD2
    compares the current dimension against one incoming batch, not every
    historical bronze row ever landed."""
    latest_run_id = (
        bronze_df.orderBy(F.col("_ingested_at").desc()).select("_ingestion_run_id").first()[0]
    )
    return bronze_df.filter(F.col("_ingestion_run_id") == latest_run_id)


def build_source_rows(bronze_df: DataFrame) -> DataFrame:
    """Shape the latest GeoNames snapshot into dim_city's attribute columns."""
    latest = _latest_snapshot(bronze_df)
    return (
        latest.select(
            F.col("geonameid").alias("city_id"),
            F.col("name").alias("city_name"),
            "asciiname",
            "country_code",
            "admin1_code",
            "latitude",
            "longitude",
            "population",
            F.coalesce(F.col("dem"), F.col("elevation")).alias("elevation_m"),
            "timezone",
        )
        .filter(F.col("city_id").isNotNull())
        .dropDuplicates(["city_id"])
    )


def merge_dim_city_scd2(spark: SparkSession, bronze_df: DataFrame | None = None) -> dict:
    """MERGE the latest GeoNames snapshot into silver.dim_city (SCD2).
    New cities get a fresh current row; cities whose tracked attributes
    changed get their old row closed (is_current=false, effective_end_ts set)
    and a new current row opened; unchanged cities are left untouched."""
    bronze_df = bronze_df if bronze_df is not None else spark.table(BRONZE_TABLE)
    source = build_source_rows(bronze_df)
    current = spark.table(SILVER_TABLE).filter("is_current = true")

    changed_predicate = " OR ".join(f"NOT (s.{c} <=> c.{c})" for c in TRACKED_ATTRIBUTES)
    changes = (
        source.alias("s")
        .join(current.alias("c"), on="city_id", how="left")
        .where(f"c.city_id IS NULL OR ({changed_predicate})")
        .select([F.col(f"s.{col}").alias(col) for col in _SOURCE_COLUMNS])
    )
    changes_count = changes.count()  # evaluate *before* the MERGE below --
    # mutates silver.dim_city -- current/changes are lazy and would otherwise
    # silently re-read the post-merge table if counted afterwards.

    # Only cities that already have a current row need closing. Cities with no
    # current row (brand new) must NOT get a mergeKey=city_id copy -- there's
    # nothing for it to match, so it would fall through to another INSERT and
    # duplicate the row (this bit; caught it landing 2x the expected row count
    # on the very first, empty-table run).
    close_rows = (
        changes.join(current.select("city_id"), on="city_id", how="inner")
        .withColumn("mergeKey", F.col("city_id"))
    )
    insert_rows = changes.withColumn("mergeKey", F.lit(None).cast("bigint"))
    staged = close_rows.unionByName(insert_rows)
    staged.createOrReplaceTempView("_dim_city_staged")

    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING _dim_city_staged AS staged
        ON target.city_id = staged.mergeKey AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.is_current = false,
            target.effective_end_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            city_id, city_name, asciiname, country_code, admin1_code, latitude, longitude,
            population, elevation_m, timezone, effective_start_ts, effective_end_ts, is_current
        ) VALUES (
            staged.city_id, staged.city_name, staged.asciiname, staged.country_code, staged.admin1_code,
            staged.latitude, staged.longitude, staged.population, staged.elevation_m, staged.timezone,
            current_timestamp(), NULL, true
        )
    """)
    return {"changed_or_new_cities": changes_count}
