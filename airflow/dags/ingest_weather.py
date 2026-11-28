from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

from denguewatch_lib.weather import (
    FORECAST_URL, TZ, ensure_table, fetch_daily, load_district_list, upsert_rows,
)

LOOKBACK_DAYS = 7
RAW_WEATHER = Asset("raw_weather_daily")


@dag(
    dag_id="ingest_weather",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    tags=["denguewatch", "ingest", "weather"],
)
def ingest_weather():

    @task
    def create_table():
        ensure_table()

    @task
    def get_districts() -> list[dict]:
        return load_district_list()

    @task(
        retries=3,
        retry_delay=timedelta(minutes=2),
        max_active_tis_per_dag=5,
        map_index_template="{{ district_name }}",
    )
    def fetch_weather(district: dict) -> list[dict]:
        context = get_current_context()
        context["district_name"] = district["name"]
        ref = context.get("logical_date") or pendulum.now(TZ)
        end_day = pendulum.instance(ref).in_timezone(TZ).date() - timedelta(days=1)
        start_day = end_day - timedelta(days=LOOKBACK_DAYS - 1)
        rows = fetch_daily(FORECAST_URL, district, start_day, end_day)
        print(f"{district['name']}: {len(rows)} days ({start_day} → {end_day})")
        return rows  # 7 small rows → fine for XCom

    @task(outlets=[RAW_WEATHER])
    def upsert(batches) -> int:
        rows = [row for batch in batches for row in batch]
        districts_seen = {r["district"] for r in rows}
        if len(districts_seen) != 25:
            raise ValueError(f"Only {len(districts_seen)} districts returned weather")
        count = upsert_rows(rows)
        print(f"Upserted {count} rows for {len(districts_seen)} districts")
        return count

    table = create_table()
    fetched = fetch_weather.expand(district=get_districts())
    loaded = upsert(fetched)
    table >> loaded


ingest_weather()