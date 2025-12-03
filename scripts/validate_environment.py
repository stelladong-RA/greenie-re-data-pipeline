"""
step4_zip_to_tract_mapping.py

Step 4 of the GreenieRE pipeline:
ZIP → Census Tract Mapping (using HUD crosswalk)

Inputs:
    output_step3/silver_project_with_zip.csv
    config/hud_zip_tract_crosswalk.csv

Outputs:
    output_step4/silver_location_enriched.csv
    output_step4/exceptions_step4_tract_mapping.csv

What this script does:
    - Loads project records that have ZIP codes (Step 3 output)
    - Loads HUD ZIP→tract crosswalk
    - For each ZIP, chooses the tract with the highest RES_RATIO
      (standard method used by HUD/insurers)
    - Adds:
        * tract_fips
        * state_fips
        * county_fips
        * tract_match_ratio
        * tract_match_flag
        * tract_error_reason
    - Rows with no valid tract match go into an exceptions file
"""

from pathlib import Path
import numpy as np
import pandas as pd

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

STEP3_DIR = Path("output_step3")
STEP4_DIR = Path("output_step4")
STEP4_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = STEP3_DIR / "silver_project_with_zip.csv"
OUTPUT_MAIN = STEP4_DIR / "silver_location_enriched.csv"
OUTPUT_EXCEPTIONS = STEP4_DIR / "exceptions_step4_tract_mapping.csv"

# HUD ZIP→tract crosswalk (download from HUD USPS Crosswalk)
# Expected columns: ZIP, STATE, COUNTY, TRACT, RES_RATIO
HUD_CROSSWALK_FILE = Path("config/hud_zip_tract_crosswalk.csv")


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def load_projects(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    df = pd.read_csv(path, dtype={"zip_code": str})
    return df


def load_hud_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"HUD crosswalk file not found: {path}")

    # Read all as string except RES_RATIO which we convert to float
    hud = pd.read_csv(path, dtype=str)

    # Normalize column names to upper (in case HUD changes casing)
    hud.columns = [c.upper() for c in hud.columns]

    required_cols = ["ZIP", "STATE", "COUNTY", "TRACT", "RES_RATIO"]
    missing = [c for c in required_cols if c not in hud.columns]
    if missing:
        raise ValueError(
            f"HUD file is missing required columns: {missing}. "
            f"Found columns: {list(hud.columns)}"
        )

    # Clean basic fields
    hud["ZIP"] = hud["ZIP"].astype(str).str.zfill(5)
    hud["STATE"] = hud["STATE"].astype(str).str.zfill(2)      # state FIPS
    hud["COUNTY"] = hud["COUNTY"].astype(str).str.zfill(3)    # county FIPS
    hud["TRACT"] = hud["TRACT"].astype(str).str.zfill(6)      # tract code within county

    # Full 11-digit tract FIPS = state (2) + county (3) + tract (6)
    hud["TRACT_FIPS"] = hud["STATE"] + hud["COUNTY"] + hud["TRACT"]

    # Convert RES_RATIO to float
    hud["RES_RATIO"] = pd.to_numeric(hud["RES_RATIO"], errors="coerce").fillna(0.0)

    return hud


def build_best_tract_per_zip(hud: pd.DataFrame) -> pd.DataFrame:
    """
    For each ZIP, select the tract row with the highest RES_RATIO.

    Returns a dataframe with one row per ZIP:
        ZIP, STATE, COUNTY, TRACT_FIPS, RES_RATIO
    """
    # Sort by RES_RATIO descending so the first row per ZIP is best
    hud_sorted = hud.sort_values(["ZIP", "RES_RATIO"], ascending=[True, False])

    # Drop duplicates, keeping the first (highest RES_RATIO)
    hud_best = hud_sorted.drop_duplicates(subset=["ZIP"], keep="first")

    # Keep only the needed columns
    hud_best = hud_best[["ZIP", "STATE", "COUNTY", "TRACT_FIPS", "RES_RATIO"]].copy()
    hud_best.rename(
        columns={
            "STATE": "state_fips",
            "COUNTY": "county_fips",
            "TRACT_FIPS": "tract_fips",
            "RES_RATIO": "tract_match_ratio",
        },
        inplace=True,
    )
    return hud_best


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print(f"Loading project data with ZIP from: {INPUT_FILE}")
    df = load_projects(INPUT_FILE)

    required_cols = ["project_id", "zip_code"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column in input data: {c}")

    # Clean ZIP codes
    df["zip_code"] = df["zip_code"].astype(str).str.strip().str.zfill(5)

    print(f"Loading HUD ZIP→tract crosswalk from: {HUD_CROSSWALK_FILE}")
    hud = load_hud_crosswalk(HUD_CROSSWALK_FILE)

    print("Building best tract per ZIP (highest residential ratio)...")
    hud_best = build_best_tract_per_zip(hud)

    # Merge projects with HUD best-tract mapping
    print("Joining projects to HUD crosswalk...")
    df_merged = df.merge(
        hud_best,
        how="left",
        left_on="zip_code",
        right_on="ZIP",
        indicator=True,
    )

    # Drop the HUD ZIP column after merge
    df_merged.drop(columns=["ZIP"], inplace=True)

    # tract_match_flag + error reasons
    df_merged["tract_match_flag"] = df_merged["_merge"].eq("both")
    df_merged.drop(columns=["_merge"], inplace=True)

    # Initialize error reason column
    df_merged["tract_error_reason"] = ""

    # Rows with no match
    no_match_mask = ~df_merged["tract_match_flag"]
    df_merged.loc[no_match_mask, "tract_error_reason"] = "NO_HUD_ZIP_MATCH"

    # If needed, we could add more nuanced reasons later (e.g., missing ZIP)
    # For now, if there's no HUD match, we treat it as NO_HUD_ZIP_MATCH.

    # Split into main vs exceptions
    df_main = df_merged[df_merged["tract_match_flag"]].copy()
    df_ex = df_merged[~df_merged["tract_match_flag"]].copy()

    # Save outputs
    df_main.to_csv(OUTPUT_MAIN, index=False)
    df_ex.to_csv(OUTPUT_EXCEPTIONS, index=False)

    print("\n=== STEP 4 COMPLETE ===")
    print(f"Main location-enriched file: {OUTPUT_MAIN} (rows: {len(df_main)})")
    print(f"Exceptions (no tract match): {OUTPUT_EXCEPTIONS} (rows: {len(df_ex)})")


if __name__ == "__main__":
    main()
