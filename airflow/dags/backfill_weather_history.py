import time
from datetime import date, timedelta

import pendulum
from airflow.sdk import Asset, Param, dag, get_current_context, task

from denguewatch_lib.weather import (
    ARCHIVE_URL, TZ, count_existing, ensure_table, fetch_daily, load_district_list, upsert_rows,
)

RAW_WEATHER = Asset("raw_weather_daily")
PAUSE_SECONDS = 10  # pause between API calls


@dag(
    dag_id="backfill_weather_history",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,  # never two backfills at the same time
    tags=["denguewatch", "backfill", "weather"],
    params={
        "start_date": Param("2010-01-01", type="string", description="First day to load (YYYY-MM-DD)"),
        "end_date": Param(None, type=["null", "string"], description="Last day (YYYY-MM-DD). Empty = 7 days ago"),
    },
)
def backfill_weather_history():

    @task
    def create_table():
        ensure_table()

    @task
    def get_districts() -> list[dict]:
        return load_district_list()

    @task(
        retries=5,
        retry_delay=timedelta(minutes=65),  # wait out the hourly limit, then continue
        max_active_tis_per_dag=1,           # one district at a time
        map_index_template="{{ district_name }}",
    )
    def backfill_district(district: dict) -> int:
        context = get_current_context()
        context["district_name"] = district["name"]
        p = context["params"]

        start = date.fromisoformat(p["start_date"])
        end = date.fromisoformat(p["end_date"]) if p["end_date"] else pendulum.now(TZ).date() - timedelta(days=7)

        total = 0
        for year in range(start.year, end.year + 1):
            chunk_start = max(start, date(year, 1, 1))
            chunk_end = min(end, date(year, 12, 31))

            # ⏭️ Skip years already fully saved
            expected = (chunk_end - chunk_start).days + 1
            if count_existing(district["name"], chunk_start, chunk_end) >= expected:
                print(f"{district['name']} {year}: already complete, skipping")
                continue

            rows = fetch_daily(ARCHIVE_URL, district, chunk_start, chunk_end)
            total += upsert_rows(rows)  # saved immediately → progress is never lost
            print(f"{district['name']} {year}: {len(rows)} days loaded")
            time.sleep(PAUSE_SECONDS)
        return total

    @task(outlets=[RAW_WEATHER])
    def summary(counts) -> int:
        total = sum(counts)
        print(f"Backfill complete: {total} new rows")
        return total

    table = create_table()
    done = backfill_district.expand(district=get_districts())
    table >> done
    summary(done)


backfill_weather_history()