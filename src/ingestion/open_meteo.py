"""Open-Meteo current-weather ingestion. Pure Python, no Spark — safe to unit test
without a cluster. Bronze landing (Spark/Delta) lives in land_bronze.py."""

import requests

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

CURRENT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
]

# Phase 1 pilot list, sourced from cities15000.txt (geonameid, name, country_code, lat, lon)
PILOT_CITIES = [
    {"city_id": 5128581, "city_name": "New York City", "country_code": "US", "latitude": 40.71427, "longitude": -74.00597},
    {"city_id": 2643743, "city_name": "London", "country_code": "GB", "latitude": 51.50853, "longitude": -0.12574},
    {"city_id": 1850147, "city_name": "Tokyo", "country_code": "JP", "latitude": 35.6895, "longitude": 139.69171},
    {"city_id": 1275339, "city_name": "Mumbai", "country_code": "IN", "latitude": 19.07283, "longitude": 72.88261},
    {"city_id": 184745, "city_name": "Nairobi", "country_code": "KE", "latitude": -1.28333, "longitude": 36.81667},
]


def fetch_current_weather(city: dict, session: requests.Session | None = None, timeout: int = 10) -> dict:
    """Call Open-Meteo's current-weather endpoint for one city and return the raw
    parsed JSON response, tagged with the city_id that produced it."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": ",".join(CURRENT_VARIABLES),
        "timezone": "UTC",
    }
    http = session or requests
    response = http.get(OPEN_METEO_BASE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    payload["_city_id"] = city["city_id"]
    return payload


def fetch_current_weather_batch(cities: list[dict] = PILOT_CITIES) -> list[dict]:
    """Sequential fetch for the pilot city list — ~5 cities, no need for
    concurrency/backoff yet (that's the Phase 2 OpenAQ story)."""
    with requests.Session() as session:
        return [fetch_current_weather(city, session=session) for city in cities]


def parse_current_weather(payload: dict) -> dict:
    """Flatten one Open-Meteo response into the columns bronze.weather_raw expects.
    Best-effort: missing keys become None rather than raising, so a schema change
    upstream degrades gracefully instead of breaking the landing job."""
    current = payload.get("current", {})
    return {
        "city_id": payload.get("_city_id"),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "elevation": payload.get("elevation"),
        "observation_time": current.get("time"),
        "temperature_2m": current.get("temperature_2m"),
        "apparent_temperature": current.get("apparent_temperature"),
        "relative_humidity_2m": current.get("relative_humidity_2m"),
        "precipitation": current.get("precipitation"),
        "rain": current.get("rain"),
        "showers": current.get("showers"),
        "snowfall": current.get("snowfall"),
        "weather_code": current.get("weather_code"),
        "cloud_cover": current.get("cloud_cover"),
        "pressure_msl": current.get("pressure_msl"),
        "surface_pressure": current.get("surface_pressure"),
        "wind_speed_10m": current.get("wind_speed_10m"),
        "wind_direction_10m": current.get("wind_direction_10m"),
        "wind_gusts_10m": current.get("wind_gusts_10m"),
        "is_day": bool(current.get("is_day")) if current.get("is_day") is not None else None,
    }
