#!/usr/bin/env python3
"""
step5_lidac_eligibility_cejst.py

Phase 1: LIDAC-style eligibility using ONLY:
  - HUD ZIP→tract crosswalk   (used already in Step 4)
  - CEJST v2 communities CSV  (tract-level disadvantaged flag)

Pipeline:
  1. Read location-enriched project file from Step 4 (with tract_fips).
  2. Read CEJST communities CSV.
  3. Normalize CEJST tract IDs into 11-digit tract_fips.
  4. Decide a boolean "cejst_disadvantaged".
  5. Join to project data on tract_fips.
  6. Output gold_lidac_classified + exceptions.
"""

import os
import sys
import pandas as pd

# --------- Paths (adjust if needed) ---------
STEP4_FILE = "output_step4/silver_location_enriched.csv"
CEJST_FILE = "config/external_data/cejst_v2_communities.csv"

OUTPUT_DIR = "output_step5"
OUTPUT_MAIN = os.path.join(OUTPUT_DIR, "gold_lidac_classified.csv")
OUTPUT_EXCEPTIONS = os.path.join(OUTPUT_DIR, "exceptions_step5_missing_cejst_match.csv")


# --------- Helpers ---------
def ensure_output_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def standardize_tract_fips(series: pd.Series) -> pd.Series:
    """
    Normalize tract identifiers to plain 11-digit FIPS strings.
    Handles:
      - plain 11-digit (e.g. '42037000100')
      - with '1400000US' prefix (sometimes used)
      - numeric-like entries
    """
    def _clean(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        # Remove known CEJST prefix if present
        if s.startswith("1400000US"):
            s = s.replace("1400000US", "")
        # Keep only digits
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 11:
            return digits
        return None  # invalid -> treated as missing

    return series.apply(_clean)


def load_step4_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Step 4 file not found: {path}")
    df = pd.read_csv(path, dtype=str)
    if "tract_fips" not in df.columns:
        raise ValueError("Expected 'tract_fips' column in Step 4 data.")
    df["tract_fips"] = standardize_tract_fips(df["tract_fips"])
    return df


def load_cejst_table(path: str) -> pd.DataFrame:
    """
    Load the CEJST CSV and create:
      - tract_fips (11-digit string)
      - cejst_disadvantaged (True/False)
      - lidac_eligible, lidac_reason
    Specifically adapted to your schema, which includes:
      - 'Census tract 2010 ID'
      - 'Identified as disadvantaged'
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CEJST file not found: {path}")

    df = pd.read_csv(path, dtype=str, low_memory=False)

    # ---- Identify the tract column ----
    # Include the actual column your file has: 'Census tract 2010 ID'
    candidate_cols = [
        "Census tract 2010 ID",
        "GEOID10",
        "GEOID",
        "tract",
        "tract_fips",
        "tract_cef_fips",
    ]
    tract_col = None
    for c in candidate_cols:
        if c in df.columns:
            tract_col = c
            break

    if tract_col is None:
        raise ValueError(
            f"Could not find a tract column in CEJST file. "
            f"Tried: {candidate_cols}. Found columns: {list(df.columns)[:20]}"
        )

    df["tract_fips"] = standardize_tract_fips(df[tract_col])

    # ---- Identify the disadvantaged flag column ----
    # From your error output, the file has: 'Identified as disadvantaged'
    disadv_candidates = [
        "Identified as disadvantaged",
        "Identified as disadvantaged without considering neighbors",
        "is_disadvantaged",
        "disadvantaged",
        "d_index",
        "DI",
    ]
    disadv_col = None
    for c in disadv_candidates:
        if c in df.columns:
            disadv_col = c
            break

    if disadv_col is None:
        raise ValueError(
            f"Could not find a 'disadvantaged' column in CEJST file. "
            f"Tried: {disadv_candidates}. Found columns: {list(df.columns)[:20]}"
        )

    # Normalize disadvantaged flag to boolean
    def to_bool(x):
        if pd.isna(x):
            return False
        s = str(x).strip().lower()
        # CEJST often uses 1/0, TRUE/FALSE, or Y/N
        return s in ("1", "true", "yes", "y", "disadvantaged")

    df["cejst_disadvantaged"] = df[disadv_col].apply(to_bool)

    # Slim lookup
    df_lookup = (
        df[["tract_fips", "cejst_disadvantaged"]]
        .dropna(subset=["tract_fips"])
        .drop_duplicates()
    )

    df_lookup["lidac_eligible"] = df_lookup["cejst_disadvantaged"].map(
        lambda v: "Yes" if v else "No"
    )
    df_lookup["lidac_reason"] = df_lookup["cejst_disadvantaged"].map(
        lambda v: "CEJST_disadvantaged" if v else "Not_disadvantaged"
    )

    return df_lookup


def main():
    print(f"Loading Step 4 location-enriched data from: {STEP4_FILE}")
    df_loc = load_step4_data(STEP4_FILE)
    print(f"Loaded {len(df_loc):,} rows from Step 4.")

    print(f"Loading CEJST communities table from: {CEJST_FILE}")
    df_cejst = load_cejst_table(CEJST_FILE)
    print(f"Loaded {len(df_cejst):,} distinct tracts from CEJST.")

    # Join
    print("Joining Step 4 data to CEJST eligibility on tract_fips...")
    df_merged = df_loc.merge(df_cejst, on="tract_fips", how="left", indicator=True)

    df_main = df_merged[df_merged["_merge"] == "both"].drop(columns=["_merge"])
    df_missing = df_merged[df_merged["_merge"] == "left_only"].drop(columns=["_merge"])

    print(f"Matched CEJST eligibility for {len(df_main):,} rows.")
    print(f"Missing CEJST match for {len(df_missing):,} rows.")

    # Ensure output dirs
    ensure_output_dir(OUTPUT_MAIN)
    ensure_output_dir(OUTPUT_EXCEPTIONS)

    df_main.to_csv(OUTPUT_MAIN, index=False)
    df_missing.to_csv(OUTPUT_EXCEPTIONS, index=False)

    print("\n=== STEP 5 (CEJST LIDAC) COMPLETE ===")
    print(f"Main classified file: {OUTPUT_MAIN} (rows: {len(df_main):,})")
    print(f"Exceptions (no CEJST match): {OUTPUT_EXCEPTIONS} (rows: {len(df_missing):,})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[ERROR]", e)
        sys.exit(1)
