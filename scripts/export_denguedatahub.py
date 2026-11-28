from pathlib import Path

import pyreadr
import requests

URLS = [
    "https://raw.githubusercontent.com/cran/denguedatahub/master/data/srilanka_weekly_data.rda",
    "https://raw.githubusercontent.com/thiyangt/denguedatahub/master/data/srilanka_weekly_data.rda",
    "https://raw.githubusercontent.com/thiyangt/denguedatahub/main/data/srilanka_weekly_data.rda",
]
TMP = Path("/tmp/srilanka_weekly_data.rda")
OUT = Path("data/raw/denguedatahub_srilanka_weekly.csv")

for url in URLS:
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200:
        TMP.write_bytes(resp.content)
        print("Downloaded:", url)
        break
    print("Not found:", url)
else:
    raise SystemExit("Could not download the .rda file from any URL")

df = next(iter(pyreadr.read_r(TMP).values()))  # the one table inside the file
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)

# A quick look so we know what we're dealing with
print(f"\nSaved {len(df)} rows -> {OUT}")
print("Columns:", list(df.columns))
print("Years:", int(df["year"].min()), "->", int(df["year"].max()))
print("Unique districts:", df["district"].nunique())
print(sorted(df["district"].unique()))
