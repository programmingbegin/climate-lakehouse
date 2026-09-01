"""GeoNames cities15000 ingestion. Pure Python, no Spark -- downloads the bulk
export directly from GeoNames (no auth, no documented rate limit; be a good
citizen and only call this at the dag_dim_refresh cadence, i.e. weekly)."""

import csv
import io
import zipfile

import requests

GEONAMES_CITIES15000_URL = "https://download.geonames.org/export/dump/cities15000.zip"

COLUMNS = [
    "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
    "feature_class", "feature_code", "country_code", "cc2", "admin1_code",
    "admin2_code", "admin3_code", "admin4_code", "population", "elevation",
    "dem", "timezone", "source_modified_date",
]


def fetch_cities15000_rows(timeout: int = 60) -> list[dict]:
    """Download and parse the GeoNames cities15000 bulk export into rows
    matching bronze.city_reference_raw's columns."""
    response = requests.get(GEONAMES_CITIES15000_URL, timeout=timeout)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        with zf.open("cities15000.txt") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8")
            reader = csv.reader(text, delimiter="\t")
            return [dict(zip(COLUMNS, row)) for row in reader]
