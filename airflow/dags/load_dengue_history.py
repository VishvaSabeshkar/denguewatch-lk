import csv

import pendulum
from airflow.sdk import Asset, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dengue_db"
CSV_FILE = "/opt/airflow/data/raw/denguedatahub_srilanka_weekly.csv"
SOURCE = "denguedatahub"
RAW_DENGUE = Asset("raw_dengue_weekly")

CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.dengue_weekly (
        source       TEXT        NOT NULL,   -- 'denguedatahub' now, 'epid_wer' (PDFs) later
        year         INT         NOT NULL,
        week         INT         NOT NULL,
        start_date   TEXT,                   -- kept as text, exactly as the source wrote it
        end_date     TEXT,
        district     TEXT        NOT NULL,   -- original spelling (e.g. 'NuwaraEliya')
        cases        INT,
        source_file  TEXT,
        loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (source, year, week, district)
    );
"""

UPSERT_SQL = """
    INSERT INTO raw.dengue_weekly
        (source, year, week, start_date, end_date, district, cases, source_file)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, year, week, district) DO UPDATE SET
        start_date  = EXCLUDED.start_date,
        end_date    = EXCLUDED.end_date,
        cases       = EXCLUDED.cases,
        source_file = EXCLUDED.source_file,
        loaded_at   = now();
"""


def to_int(value: str):
    """CSV numbers may look like '71.0' (R stores numbers as decimals) or be empty."""
    if value is None or value.strip() in ("", "NA", "nan"):
        return None
    return int(float(value))


@dag(
    dag_id="load_dengue_history",
    schedule=None,  # one-off, manual
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Colombo"),
    catchup=False,
    tags=["denguewatch", "backfill", "dengue"],
)
def load_dengue_history():

    @task
    def create_table():
        PostgresHook(postgres_conn_id=CONN_ID).run(CREATE_TABLE_SQL)

    @task(outlets=[RAW_DENGUE])
    def load_csv() -> int:
        # 1) Read + convert numbers. Keyed by (year, week, district) → duplicates collapse
        rows = {}
        duplicates = 0
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                key = (to_int(r["year"]), to_int(
                    r["week"]), r["district"].strip())
                if key in rows:
                    duplicates += 1
                rows[key] = (
                    SOURCE, key[0], key[1], r["start.date"], r["end.date"],
                    key[2], to_int(
                        r["cases"]), "denguedatahub_srilanka_weekly.csv",
                )
        print(f"Read {len(rows)} unique rows ({duplicates} duplicates removed)")

        # 2) Bulk insert: thousands of rows per round-trip instead of one by one
        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        with conn, conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, list(rows.values()))
        conn.close()
        print(f"Upserted {len(rows)} rows")
        return len(rows)  # only a number goes to XCom, not 25k rows

    @task
    def quality_check():
        """Fail loudly if the loaded data looks wrong."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        total, names, min_y, max_y, negatives, nulls = hook.get_first("""
            SELECT COUNT(*), COUNT(DISTINCT district), MIN(year), MAX(year),
                   COUNT(*) FILTER (WHERE cases < 0),
                   COUNT(*) FILTER (WHERE cases IS NULL)
            FROM raw.dengue_weekly WHERE source = %s;
        """, parameters=(SOURCE,))
        print(
            f"rows={total} names={names} years={min_y}-{max_y} negatives={negatives} nulls={nulls}")

        problems = []
        if total < 25000:
            problems.append(f"too few rows ({total})")
        if names != 26:
            problems.append(f"expected 26 region names, got {names}")
        if negatives > 0:
            problems.append(f"{negatives} negative case counts")
        if problems:
            raise ValueError("Quality check failed: " + "; ".join(problems))
        print("✅ Quality check passed")

    create_table() >> load_csv() >> quality_check()


load_dengue_history()
