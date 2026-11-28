"""
DAG: load_districts
Loads the 25 Sri Lankan districts + coordinates into the warehouse (raw.districts).
Run it manually whenever districts.csv changes. Safe to run many times.
"""
import csv

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

SEED_FILE = "/opt/airflow/data/seeds/districts.csv"
CONN_ID = "dengue_db"  # the connection we defined in docker-compose.yaml


@dag(
    dag_id="load_districts",
    schedule=None,  # no schedule: only runs when you click Trigger
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Colombo"),
    catchup=False,
    tags=["denguewatch", "setup"],
)
def load_districts():

    @task
    def create_table():
        """Create the schema + table if they don't exist yet."""
        PostgresHook(postgres_conn_id=CONN_ID).run("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.districts (
                district   TEXT PRIMARY KEY,
                province   TEXT NOT NULL,
                latitude   NUMERIC(8,5) NOT NULL,
                longitude  NUMERIC(8,5) NOT NULL,
                loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

    @task
    def read_csv() -> list[dict]:
        """Read the CSV and check we have exactly 25 districts."""
        with open(SEED_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if len(rows) != 25:
            raise ValueError(f"Expected 25 districts, found {len(rows)}")
        return rows  # small list → fine to pass through XCom

    @task
    def upsert(rows: list[dict]) -> int:
        """Insert new districts, update existing ones (idempotent)."""
        sql = """
            INSERT INTO raw.districts (district, province, latitude, longitude)
            VALUES (%(district)s, %(province)s, %(latitude)s, %(longitude)s)
            ON CONFLICT (district) DO UPDATE
            SET province  = EXCLUDED.province,
                latitude  = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                loaded_at = now();
        """
        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        with conn, conn.cursor() as cur:  # 'with conn' = commit on success
            cur.executemany(sql, rows)
        conn.close()
        print(f"Upserted {len(rows)} districts")
        return len(rows)

    table = create_table()
    rows = read_csv()
    # passing 'rows' creates the arrow read_csv → upsert
    loaded = upsert(rows)
    table >> loaded         # and the table must exist before we insert


load_districts()
