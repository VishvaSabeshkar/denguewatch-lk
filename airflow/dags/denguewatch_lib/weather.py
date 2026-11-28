from datetime import date

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dengue_db"
TZ = "Asia/Colombo"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"        # recent days
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"  # history back to 1940
DAILY_VARS = [
    "precipitation_sum",
    "rain_sum",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
]

CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.weather_daily (
        district          TEXT        NOT NULL,
        day               DATE        NOT NULL,
        precipitation_mm  NUMERIC(6,1),
        rain_mm           NUMERIC(6,1),
        temp_mean_c       NUMERIC(4,1),
        temp_max_c        NUMERIC(4,1),
        temp_min_c        NUMERIC(4,1),
        source            TEXT        NOT NULL DEFAULT 'open-meteo',
        loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (district, day)
    );
"""

UPSERT_SQL = """
    INSERT INTO raw.weather_daily
        (district, day, precipitation_mm, rain_mm, temp_mean_c, temp_max_c, temp_min_c)
    VALUES
        (%(district)s, %(day)s, %(precipitation_mm)s, %(rain_mm)s,
         %(temp_mean_c)s, %(temp_max_c)s, %(temp_min_c)s)
    ON CONFLICT (district, day) DO UPDATE SET
        precipitation_mm = EXCLUDED.precipitation_mm,
        rain_mm          = EXCLUDED.rain_mm,
        temp_mean_c      = EXCLUDED.temp_mean_c,
        temp_max_c       = EXCLUDED.temp_max_c,
        temp_min_c       = EXCLUDED.temp_min_c,
        loaded_at        = now();
"""


def ensure_table() -> None:
    PostgresHook(postgres_conn_id=CONN_ID).run(CREATE_TABLE_SQL)


def load_district_list() -> list[dict]:
    records = PostgresHook(postgres_conn_id=CONN_ID).get_records(
        "SELECT district, latitude::float, longitude::float FROM raw.districts ORDER BY district;"
    )
    if len(records) != 25:
        raise ValueError(
            f"Expected 25 districts, found {len(records)}. Run load_districts first.")
    return [{"name": r[0], "lat": r[1], "lon": r[2]} for r in records]


def fetch_daily(url: str, district: dict, start_day: date, end_day: date) -> list[dict]:
    params = {
        "latitude": district["lat"],
        "longitude": district["lon"],
        "daily": ",".join(DAILY_VARS),
        "timezone": TZ,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
    }
    resp = requests.get(url, params=params, timeout=60)
    if resp.status_code == 429:
        # Print Open-Meteo's reason (minutely / hourly / daily limit) into the Airflow log
        print(f"Rate limited by Open-Meteo: {resp.text}")
    resp.raise_for_status()
    daily = resp.json()["daily"]
    return [
        {
            "district": district["name"],
            "day": day,
            "precipitation_mm": daily["precipitation_sum"][i],
            "rain_mm": daily["rain_sum"][i],
            "temp_mean_c": daily["temperature_2m_mean"][i],
            "temp_max_c": daily["temperature_2m_max"][i],
            "temp_min_c": daily["temperature_2m_min"][i],
        }
        for i, day in enumerate(daily["time"])
    ]


def upsert_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    with conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    conn.close()
    return len(rows)


def count_existing(district_name: str, start_day: date, end_day: date) -> int:
    """How many days in this range are already saved for this district?"""
    return PostgresHook(postgres_conn_id=CONN_ID).get_first(
        "SELECT COUNT(*) FROM raw.weather_daily WHERE district = %s AND day BETWEEN %s AND %s;",
        parameters=(district_name, start_day, end_day),
    )[0]
