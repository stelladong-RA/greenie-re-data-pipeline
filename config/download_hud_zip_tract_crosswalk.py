"""
download_hud_zip_tract_crosswalk.py

Downloads the ZIP→Census Tract crosswalk using the official HUD USPS API.

Requires:
    export HUD_API_KEY="your_token_here"

Produces:
    config/hud_zip_tract_crosswalk.csv
    with columns: ZIP, STATE, COUNTY, TRACT, RES_RATIO
"""

import os
from pathlib import Path
import requests
import pandas as pd

API_URL = "https://www.huduser.gov/hudapi/public/usps"

CONFIG_DIR = Path("config")
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = CONFIG_DIR / "hud_zip_tract_crosswalk.csv"

HUD_API_KEY = os.getenv("HUD_API_KEY")
if HUD_API_KEY is None:
    raise RuntimeError("HUD_API_KEY environment variable is not set.")


def main():
    print("Requesting ZIP→Tract crosswalk from HUD API...")

    headers = {
        "Authorization": f"Bearer {HUD_API_KEY}"
    }

    # type = 1 => ZIP -> tract
    # query = "All" (per HUD docs, case-sensitive)
    params = {
        "type": 1,
        "query": "All"
        # you can add "year" and "quarter" if you want a specific vintage
        # e.g., "year": 2024, "quarter": 2
    }

    resp = requests.get(API_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()

    data_json = resp.json()

    if "data" not in data_json or "results" not in data_json["data"]:
        raise ValueError(
            "Unexpected HUD API response format. "
            "Expected data['data']['results'] to contain records."
        )

    records = data_json["data"]["results"]
    print(f"Received {len(records):,} ZIP→tract records from HUD.")

    if not records:
        raise ValueError("HUD API returned zero records. Check query/year/quarter params.")

    df = pd.DataFrame(records)

    # Inspect typical columns:
    # For type=1 the docs say we get: zip, geoid, res_ratio, bus_ratio, oth_ratio, tot_ratio, etc.
    # We will normalize these to: ZIP, STATE, COUNTY, TRACT, RES_RATIO.

    df.columns = [c.lower() for c in df.columns]

    if "zip" not in df.columns or "geoid" not in df.columns or "res_ratio" not in df.columns:
        raise ValueError(
            f"Missing expected fields in HUD response. "
            f"Available columns: {list(df.columns)}"
        )

    # Keep only what we need
    df_use = df[["zip", "geoid", "res_ratio"]].copy()

    # Clean formats
    df_use["zip"] = df_use["zip"].astype(str).str.zfill(5)
    df_use["geoid"] = df_use["geoid"].astype(str).str.zfill(11)
    df_use["res_ratio"] = pd.to_numeric(df_use["res_ratio"], errors="coerce").fillna(0.0)

    # Split 11-digit tract FIPS into state (2), county (3), tract (6)
    df_use["state"] = df_use["geoid"].str.slice(0, 2)
    df_use["county"] = df_use["geoid"].str.slice(2, 5)
    df_use["tract"] = df_use["geoid"].str.slice(5, 11)

    # Rename to our canonical names
    df_out = df_use.rename(
        columns={
            "zip": "ZIP",
            "state": "STATE",
            "county": "COUNTY",
            "tract": "TRACT",
            "res_ratio": "RES_RATIO",
        }
    )[["ZIP", "STATE", "COUNTY", "TRACT", "RES_RATIO"]]

    df_out.to_csv(OUT_FILE, index=False)

    print("\n=== HUD ZIP→TRACT CROSSWALK READY ===")
    print(f"Saved normalized CSV to: {OUT_FILE}")
    print("You can now run: python step4_zip_to_tract_mapping.py")


if __name__ == "__main__":
    main()
