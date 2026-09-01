"""NOAA GHCN-Daily bulk ingestion. Pure Python, no Spark, no auth. Uses the
per-station .dly files (https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/)
rather than the global by_year files, since we're scoped to a handful of
stations matching the pilot city list -- no point pulling every station on
Earth for five cities."""

import datetime

import requests

GHCN_STATION_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"

# Nearest reliable long-record station per pilot city, found by haversine
# distance against the real ghcnd-stations.txt list (not guessed).
PILOT_STATIONS = [
    {"station_id": "USW00094728", "city_id": 5128581, "station_name": "NY CITY CENTRAL PARK"},
    {"station_id": "UKM00003772", "city_id": 2643743, "station_name": "HEATHROW"},
    {"station_id": "JA000047662", "city_id": 1850147, "station_name": "TOKYO"},
    {"station_id": "IN012070800", "city_id": 1275339, "station_name": "BOMBAY/SANTACRUZ"},
    {"station_id": "KEM00063741", "city_id": 184745, "station_name": "NAIROBI/DAGORETTI"},
]

_LINE_WIDTH = 269
_DAYS_PER_LINE = 31
_MISSING = -9999


def fetch_station_dly(station_id: str, timeout: int = 30) -> str:
    response = requests.get(GHCN_STATION_URL.format(station_id=station_id), timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_dly(text: str, station_id: str) -> list[dict]:
    """Unpack GHCN's fixed-width per-station-month-element format into one
    row per (station_id, obs_date, element). Skips missing values (-9999)
    and invalid calendar days (e.g. day 31 in a 30-day month) rather than
    raising -- a malformed/missing day is routine in this format, not an
    error worth failing the whole station's landing over."""
    rows = []
    for line in text.splitlines():
        if len(line) < _LINE_WIDTH:
            continue
        year = int(line[11:15])
        month = int(line[15:17])
        element = line[17:21]
        for day in range(1, _DAYS_PER_LINE + 1):
            base = 21 + (day - 1) * 8
            value_raw = line[base:base + 5].strip()
            if not value_raw:
                continue
            value = int(value_raw)
            if value == _MISSING:
                continue
            try:
                datetime.date(year, month, day)  # validates the calendar day
            except ValueError:
                continue
            obs_date = f"{year:04d}-{month:02d}-{day:02d}"
            rows.append({
                "station_id": station_id,
                "obs_date": obs_date,
                "element": element,
                "data_value": value,
                "mflag": line[base + 5:base + 6].strip() or None,
                "qflag": line[base + 6:base + 7].strip() or None,
                "sflag": line[base + 7:base + 8].strip() or None,
            })
    return rows


def fetch_pilot_stations_rows(
    stations: list[dict] = PILOT_STATIONS,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    """Fetch+parse one or more stations. start_date/end_date (YYYY-MM-DD,
    inclusive) filter the parsed rows -- the .dly file itself always holds a
    station's full history (there's no ranged-download endpoint), so
    "backfill for a date range" means "download full history, keep only the
    range you asked for", not a smaller download."""
    all_rows = []
    for station in stations:
        text = fetch_station_dly(station["station_id"])
        rows = parse_dly(text, station["station_id"])
        if start_date:
            rows = [r for r in rows if r["obs_date"] >= start_date]
        if end_date:
            rows = [r for r in rows if r["obs_date"] <= end_date]
        all_rows.extend(rows)
    return all_rows
