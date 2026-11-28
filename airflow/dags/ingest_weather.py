"""
DAG: ingest_weather
Every day at 06:00 Sri Lanka time, fetch the last 7 days of daily weather
for all 25 districts from Open-Meteo and upsert into raw.weather_daily.

Why 7 days and not just yesterday? If the laptop was off for a few days,
the next run fills the gap automatically ("self-healing").
Weather data by Open-Meteo.com (CC BY 4.0).
"""
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import Asset, dag, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dengue_db"
API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS = [
    "precipitation_sum",
    "rain_sum",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
]
LOOKBACK_DAYS = 7
TZ = "Asia/Colombo"

# An Asset = a named piece of data. Later (Step 16), dbt will run
# automatically whenever this asset is updated.
RAW_WEATHER = Asset("raw_weather_daily")


@dag(
    dag_id="ingest_weather",
    # every day at 06:00 (time zone comes from start_date)
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,  # don't create runs for missed days; the 7-day window covers gaps
    tags=["denguewatch", "ingest", "weather"],
)
def ingest_weather():

    @task
    def create_table():
        PostgresHook(postgres_conn_id=CONN_ID).run("""
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
        """)

    @task
    def get_districts() -> list[dict]:
        """Read the 25 districts we loaded in Step 5."""
        records = PostgresHook(postgres_conn_id=CONN_ID).get_records(
            "SELECT district, latitude::float, longitude::float "
            "FROM raw.districts ORDER BY district;"
        )
        if len(records) != 25:
            raise ValueError(
                f"Expected 25 districts, found {len(records)}. Run load_districts first.")
        return [{"name": r[0], "lat": r[1], "lon": r[2]} for r in records]

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        max_active_tis_per_dag=5,  # max 5 districts at once → gentle on RAM and on the API
        # show "Colombo" in the UI, not "0"
        map_index_template="{{ district_name }}",
    )
    def fetch_weather(district: dict) -> list[dict]:
        context = get_current_context()
        context["district_name"] = district["name"]

        # Use the run's logical date, NOT now() → re-runs fetch the same days
        ref = context.get("logical_date") or pendulum.now(TZ)
        end_day = pendulum.instance(ref).in_timezone(
            TZ).date() - timedelta(days=1)
        start_day = end_day - timedelta(days=LOOKBACK_DAYS - 1)

        params = {
            "latitude": district["lat"],
            "longitude": district["lon"],
            "daily": ",".join(DAILY_VARS),
            "timezone": TZ,
            "start_date": start_day.isoformat(),
            "end_date": end_day.isoformat(),
        }
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()  # HTTP error → task fails → Airflow retries
        daily = resp.json()["daily"]

        rows = []
        for i, day in enumerate(daily["time"]):
            rows.append({
                "district": district["name"],
                "day": day,
                "precipitation_mm": daily["precipitation_sum"][i],
                "rain_mm": daily["rain_sum"][i],
                "temp_mean_c": daily["temperature_2m_mean"][i],
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
            })
        print(
            f"{district['name']}: {len(rows)} days ({start_day} → {end_day})")
        return rows

    # "this task updates the raw_weather_daily asset"
    @task(outlets=[RAW_WEATHER])
    def upsert(batches) -> int:
        rows = [row for batch in batches for row in batch]  # 25 lists → 1 list

        # Data quality check: every district must be present
        districts_seen = {r["district"] for r in rows}
        if len(districts_seen) != 25:
            raise ValueError(
                f"Only {len(districts_seen)} districts returned weather")

        sql = """
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
        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        with conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.close()
        print(f"Upserted {len(rows)} rows for {len(districts_seen)} districts")
        return len(rows)

    table = create_table()
    districts = get_districts()
    # ✨ 25 tasks, one per district
    fetched = fetch_weather.expand(district=districts)
    loaded = upsert(fetched)
    table >> loaded


ingest_weather()
