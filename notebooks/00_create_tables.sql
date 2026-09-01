-- Databricks notebook source
-- Climate & Air Quality Intelligence Lakehouse — table creation
-- Run top to bottom in a Databricks SQL/notebook cell. Safe to re-run (CREATE ... IF NOT EXISTS).
-- Phase 1 tables are marked -- the rest (OpenAQ, GHCN, SCD2, gold) can be run later in Phase 2+
-- without touching what Phase 1 already created.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------
-- =====================================================================
-- BRONZE — raw, append-only, schema-on-read
-- =====================================================================

-- Phase 1
CREATE TABLE IF NOT EXISTS bronze.weather_raw (
  _source              STRING      COMMENT 'e.g. open-meteo',
  _ingested_at          TIMESTAMP,
  _ingestion_run_id      STRING      COMMENT 'Airflow run_id',
  _ingest_date          DATE        COMMENT 'partition column',
  city_id               BIGINT      COMMENT 'GeoNames geonameid used for the API call',
  raw_json              STRING      COMMENT 'full literal API response',
  latitude               DOUBLE,
  longitude              DOUBLE,
  elevation              DOUBLE,
  observation_time      TIMESTAMP  COMMENT 'parsed from current.time',
  temperature_2m         DOUBLE,
  apparent_temperature   DOUBLE,
  relative_humidity_2m   DOUBLE,
  precipitation          DOUBLE,
  rain                   DOUBLE,
  showers                DOUBLE,
  snowfall               DOUBLE,
  weather_code           INT,
  cloud_cover             DOUBLE,
  pressure_msl            DOUBLE,
  surface_pressure        DOUBLE,
  wind_speed_10m           DOUBLE,
  wind_direction_10m       DOUBLE,
  wind_gusts_10m           DOUBLE,
  is_day                   BOOLEAN
)
USING DELTA
PARTITIONED BY (_ingest_date)
COMMENT 'Raw Open-Meteo current-weather landing. Append-only, never overwritten.';

-- Phase 1 (row-per-source-file snapshot -- SCD2 handling happens in silver)
CREATE TABLE IF NOT EXISTS bronze.city_reference_raw (
  _source              STRING      COMMENT 'geonames',
  _ingested_at          TIMESTAMP,
  _ingestion_run_id      STRING,
  _ingest_date          DATE        COMMENT 'partition column — one partition per snapshot pull',
  geonameid             BIGINT,
  name                  STRING,
  asciiname             STRING,
  alternatenames        STRING,
  latitude               DOUBLE,
  longitude              DOUBLE,
  feature_class          STRING,
  feature_code           STRING,
  country_code            STRING,
  cc2                     STRING,
  admin1_code             STRING,
  admin2_code             STRING,
  admin3_code             STRING,
  admin4_code             STRING,
  population              BIGINT,
  elevation               INT,
  dem                     INT,
  timezone                STRING,
  source_modified_date    DATE
)
USING DELTA
PARTITIONED BY (_ingest_date)
COMMENT 'Raw GeoNames cities15000.txt snapshots. Re-pull weekly for dag_dim_refresh / SCD2 source.';

-- Phase 2
CREATE TABLE IF NOT EXISTS bronze.air_quality_raw (
  _source              STRING      COMMENT 'openaq',
  _ingested_at          TIMESTAMP,
  _ingestion_run_id      STRING,
  _ingest_date          DATE        COMMENT 'partition column',
  location_id            BIGINT      COMMENT 'OpenAQ locations.id',
  sensor_id              BIGINT      COMMENT 'OpenAQ sensors[].id',
  raw_json              STRING      COMMENT 'full literal API response (locations or measurements call)',
  parameter_name          STRING      COMMENT 'e.g. pm25, pm10, o3, no2, so2, co',
  parameter_units          STRING,
  value                    DOUBLE,
  period_datetime_from      TIMESTAMP,
  period_datetime_to        TIMESTAMP,
  location_latitude        DOUBLE,
  location_longitude       DOUBLE
)
USING DELTA
PARTITIONED BY (_ingest_date)
COMMENT 'Raw OpenAQ v3 landing (locations metadata + sensor measurements). Append-only.';

-- Phase 2
CREATE TABLE IF NOT EXISTS bronze.ghcn_daily_raw (
  _source              STRING      COMMENT 'noaa-ghcn',
  _ingested_at          TIMESTAMP,
  _ingestion_run_id      STRING,
  _source_file          STRING      COMMENT 'originating .csv.gz / .dly filename',
  _ingest_date          DATE,
  station_id             STRING,
  obs_date               DATE,
  element                 STRING      COMMENT 'TMAX, TMIN, PRCP, SNOW, etc.',
  data_value               INT         COMMENT 'raw GHCN units, e.g. tenths of degC — convert in silver',
  mflag                    STRING,
  qflag                    STRING,
  sflag                    STRING,
  obs_time                 STRING
)
USING DELTA
PARTITIONED BY (_ingest_date)
COMMENT 'Raw NOAA GHCN-Daily bulk file landing, scoped to ~20-50 stations matching the city list.';

-- COMMAND ----------
-- =====================================================================
-- SILVER — conformed, deduped, MERGE-upserted, quality-gated
-- =====================================================================

-- Phase 1
CREATE TABLE IF NOT EXISTS silver.weather_observations (
  city_id                   BIGINT,
  observation_timestamp      TIMESTAMP,
  temperature_c               DOUBLE,
  apparent_temperature_c       DOUBLE,
  relative_humidity_pct        DOUBLE,
  precipitation_mm              DOUBLE,
  rain_mm                       DOUBLE,
  showers_mm                    DOUBLE,
  snowfall_cm                   DOUBLE,
  weather_code                  INT,
  cloud_cover_pct                DOUBLE,
  pressure_msl_hpa                DOUBLE,
  surface_pressure_hpa            DOUBLE,
  wind_speed_10m_kmh              DOUBLE,
  wind_direction_10m_deg          DOUBLE,
  wind_gusts_10m_kmh              DOUBLE,
  is_day                           BOOLEAN,
  _source                         STRING,
  _ingested_at                     TIMESTAMP,
  _merged_at                       TIMESTAMP
)
USING DELTA
PARTITIONED BY (_source)
COMMENT 'MERGE key: (city_id, observation_timestamp, _source). Never blind-appended or overwritten.';

-- Phase 2
CREATE TABLE IF NOT EXISTS silver.air_quality_readings (
  location_id                BIGINT,
  city_id                    BIGINT      COMMENT 'nearest-city join from dim_city via haversine on location lat/lon',
  parameter_name              STRING      COMMENT 'pm25, pm10, o3, no2, so2, co',
  parameter_units              STRING,
  value                        DOUBLE,
  observation_timestamp        TIMESTAMP  COMMENT 'period.datetimeFrom.utc',
  _source                      STRING,
  _ingested_at                  TIMESTAMP,
  _merged_at                    TIMESTAMP
)
USING DELTA
PARTITIONED BY (_source)
COMMENT 'MERGE key: (location_id, parameter_name, observation_timestamp, _source) — parameter is part of the key because one station reports several parameters per timestamp.';

-- Phase 2
CREATE TABLE IF NOT EXISTS silver.historical_daily_weather (
  station_id                STRING,
  obs_date                   DATE,
  obs_year                   INT        COMMENT 'partition column',
  element                     STRING,
  value                       DOUBLE     COMMENT 'converted to standard units (e.g. degC, mm) from raw GHCN tenths',
  mflag                       STRING,
  qflag                       STRING,
  sflag                       STRING,
  _source                     STRING,
  _ingested_at                 TIMESTAMP,
  _merged_at                   TIMESTAMP
)
USING DELTA
PARTITIONED BY (obs_year)
COMMENT 'MERGE key: (station_id, obs_date, element).';

-- Phase 2 — SCD Type 2
CREATE TABLE IF NOT EXISTS silver.dim_city (
  city_sk                BIGINT GENERATED ALWAYS AS IDENTITY,
  city_id                BIGINT      COMMENT 'GeoNames geonameid, natural key',
  city_name              STRING,
  asciiname              STRING,
  country_code            STRING,
  admin1_code             STRING,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  population               BIGINT      COMMENT 'tracked attribute',
  elevation_m              INT         COMMENT 'tracked attribute',
  timezone                 STRING      COMMENT 'tracked attribute',
  effective_start_ts        TIMESTAMP,
  effective_end_ts          TIMESTAMP,
  is_current                 BOOLEAN
)
USING DELTA
COMMENT 'SCD2 city dimension sourced from GeoNames snapshots. Close/open rows via MERGE, never overwrite history.';

-- Phase 2
CREATE TABLE IF NOT EXISTS silver.dq_quarantine (
  source_table            STRING      COMMENT 'e.g. silver.weather_observations',
  natural_key              STRING      COMMENT 'serialized key of the failing row, e.g. city_id|observation_timestamp|_source',
  failure_reason            STRING,
  raw_record                STRING      COMMENT 'the failing record as JSON, for replay/debugging',
  _ingestion_run_id          STRING,
  quarantined_at             TIMESTAMP
)
USING DELTA
PARTITIONED BY (source_table)
COMMENT 'Rows that fail a Great Expectations check land here with a reason instead of being dropped or breaking the run.';

-- COMMAND ----------
-- =====================================================================
-- GOLD — star schema, dashboard-facing
-- =====================================================================

-- Phase 2
CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_sk           INT       COMMENT 'yyyymmdd',
  full_date         DATE,
  year              INT,
  quarter           INT,
  month             INT,
  month_name        STRING,
  day               INT,
  day_of_week        INT,
  day_name           STRING,
  is_weekend          BOOLEAN
)
USING DELTA
COMMENT 'Standard date dimension, generate once via a date range and populate ad hoc.';

-- Phase 2 — view over the current SCD2 rows
CREATE OR REPLACE VIEW gold.dim_city_current AS
SELECT city_sk, city_id, city_name, asciiname, country_code, admin1_code,
       latitude, longitude, population, elevation_m, timezone
FROM silver.dim_city
WHERE is_current = true;

-- Phase 1 (FK to dim_city_current can point at city_id until SCD2 lands in Phase 2)
CREATE TABLE IF NOT EXISTS gold.fact_weather_observation (
  weather_observation_sk    BIGINT GENERATED ALWAYS AS IDENTITY,
  city_id                    BIGINT,
  date_sk                     INT,
  observation_timestamp        TIMESTAMP,
  temperature_c                 DOUBLE,
  apparent_temperature_c         DOUBLE,
  relative_humidity_pct           DOUBLE,
  precipitation_mm                 DOUBLE,
  wind_speed_10m_kmh                DOUBLE,
  weather_code                       INT,
  _source                            STRING
)
USING DELTA
PARTITIONED BY (date_sk)
COMMENT 'Grain: one row per city_id x observation_timestamp.';

-- Phase 2
CREATE TABLE IF NOT EXISTS gold.fact_air_quality_reading (
  air_quality_reading_sk    BIGINT GENERATED ALWAYS AS IDENTITY,
  city_id                    BIGINT,
  location_id                 BIGINT,
  date_sk                      INT,
  observation_timestamp         TIMESTAMP,
  parameter_name                  STRING,
  parameter_units                  STRING,
  value                             DOUBLE,
  _source                           STRING
)
USING DELTA
PARTITIONED BY (date_sk)
COMMENT 'Grain: one row per city_id x parameter_name x observation_timestamp.';

-- Phase 1
CREATE TABLE IF NOT EXISTS gold.mart_daily_city_climate_summary (
  city_id                BIGINT,
  city_name              STRING,
  obs_date                DATE,
  avg_temp_c               DOUBLE,
  min_temp_c               DOUBLE,
  max_temp_c               DOUBLE,
  total_precip_mm            DOUBLE,
  avg_wind_speed_kmh          DOUBLE,
  dominant_aqi_category         STRING     COMMENT 'null until Phase 2 air-quality join exists',
  row_count                      BIGINT
)
USING DELTA
PARTITIONED BY (obs_date)
COMMENT 'Daily aggregate per city. Phase 1 - weather columns only, dominant_aqi_category populated once OpenAQ lands.';

-- Phase 2
CREATE TABLE IF NOT EXISTS gold.mart_weather_air_quality_correlation (
  city_id            BIGINT,
  city_name          STRING,
  obs_date            DATE,
  avg_temp_c            DOUBLE,
  avg_humidity_pct        DOUBLE,
  avg_pm25                 DOUBLE,
  avg_aqi                   DOUBLE
)
USING DELTA
PARTITIONED BY (obs_date)
COMMENT 'Joined weather + air-quality by city/day, for correlation analysis.';

-- Phase 4
CREATE TABLE IF NOT EXISTS gold.mart_pipeline_health (
  dag_id                STRING,
  run_id                 STRING,
  run_date                 DATE,
  rows_processed             BIGINT,
  rows_quarantined            BIGINT,
  run_duration_sec              DOUBLE,
  freshness_lag_minutes           DOUBLE
)
USING DELTA
PARTITIONED BY (run_date)
COMMENT 'Fed by Airflow/Databricks run metadata via dag_maintenance. Meta-monitoring mart.';
