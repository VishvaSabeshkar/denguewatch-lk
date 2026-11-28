import re
import time
from pathlib import Path
from urllib.parse import urljoin

import pendulum
import requests
from airflow.sdk import Asset, Param, dag, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "dengue_db"
TZ = "Asia/Colombo"
LIST_URL = "https://www.epid.gov.lk/weekly-epidemiological-report/weekly-epidemiological-report"
PDF_DIR = Path("/opt/airflow/wer_pdfs")
HEADERS = {
    "User-Agent": "DengueWatchLK/1.0 (non-commercial portfolio project)"}
# Matches links like ..._Vol_53_no_05-english.pdf (both link styles)
LINK_RE = re.compile(
    r'href="([^"]*?Vol_(\d+)_no_(\d+)[^"]*?\.pdf)"', re.IGNORECASE)
WER_PDFS = Asset("wer_pdfs")

CREATE_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.wer_files (
        year           INT         NOT NULL,
        volume         INT         NOT NULL,
        number         INT         NOT NULL,   -- report number within the year
        url            TEXT        NOT NULL,
        file_path      TEXT        NOT NULL,
        size_bytes     INT,
        downloaded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (year, number)
    );
"""


@dag(
    dag_id="download_wer_pdfs",
    # every Tuesday 07:00 (reports are published irregularly)
    schedule="0 7 * * 2",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    tags=["denguewatch", "ingest", "dengue"],
    params={"year": Param(None, type=[
                          "null", "integer"], description="Year to fetch. Empty = current year")},
)
def download_wer_pdfs():

    @task
    def create_table():
        PostgresHook(postgres_conn_id=CONN_ID).run(CREATE_TABLE_SQL)

    @task(retries=2, retry_delay=pendulum.duration(minutes=10))
    def find_reports() -> list[dict]:
        """Read the list page and collect English PDF links for the chosen year."""
        context = get_current_context()
        year = context["params"]["year"] or pendulum.now(TZ).year
        volume = year - 1973

        resp = requests.get(LIST_URL, headers=HEADERS, timeout=60)
        resp.raise_for_status()

        found = {}
        for href, vol, num in LINK_RE.findall(resp.text):
            if int(vol) != volume or "english" not in href.lower():
                continue  # other years, or Sinhala/Tamil versions
            found.setdefault(int(num), urljoin(LIST_URL, href)
                             )  # first link per number wins

        if not found:
            raise ValueError(
                f"No Vol_{volume} English PDFs found on the page. Has the website changed?")
        print(
            f"Found {len(found)} reports for {year} (Vol {volume}): numbers {sorted(found)}")
        return [{"year": year, "volume": volume, "number": n, "url": u} for n, u in sorted(found.items())]

    @task
    def pick_new(reports: list[dict]) -> list[dict]:
        """Keep only reports we haven't downloaded yet (idempotent)."""
        if not reports:
            return []
        year = reports[0]["year"]
        have = {r[0] for r in PostgresHook(postgres_conn_id=CONN_ID).get_records(
            "SELECT number FROM raw.wer_files WHERE year = %s;", parameters=(year,)
        )}
        new = [r for r in reports if r["number"] not in have]
        print(
            f"Already have {len(have)}, new to download: {[r['number'] for r in new]}")
        return new

    @task(
        retries=3,
        retry_delay=pendulum.duration(minutes=5),
        max_active_tis_per_dag=2,          # gentle on a government website 🙏
        map_index_template="{{ report_label }}",
        outlets=[WER_PDFS],
    )
    def download(report: dict) -> str:
        context = get_current_context()
        context["report_label"] = f"{report['year']} No {report['number']:02d}"

        resp = requests.get(report["url"], headers=HEADERS, timeout=120)
        resp.raise_for_status()
        # real PDFs always start with %PDF
        if not resp.content.startswith(b"%PDF"):
            raise ValueError(f"Not a PDF: {report['url']}")

        folder = PDF_DIR / str(report["year"])
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"wer_{report['year']}_no_{report['number']:02d}.pdf"
        path.write_bytes(resp.content)

        PostgresHook(postgres_conn_id=CONN_ID).run(
            """INSERT INTO raw.wer_files (year, volume, number, url, file_path, size_bytes)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (year, number) DO NOTHING;""",
            parameters=(report["year"], report["volume"], report["number"],
                        report["url"], str(path), len(resp.content)),
        )
        print(f"Saved {path} ({len(resp.content) / 1024:.0f} KB)")
        time.sleep(2)  # polite pause
        return str(path)

    table = create_table()
    new_reports = pick_new(find_reports())
    table >> new_reports
    download.expand(report=new_reports)


download_wer_pdfs()
