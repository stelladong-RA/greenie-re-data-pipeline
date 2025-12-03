#!/usr/bin/env python3
"""
step7_zip_accumulation.py

Step 7 – ZIP-level accumulation detection.

Inputs:
  - output_step5/gold_lidac_classified.csv

Outputs:
  - output_step7/gold_zip_accumulation_flags.csv
  - output_step7/exceptions_step7_missing_zip_or_premium.csv

For each ZIP:
  * count projects
  * count distinct carriers
  * sum gross premium and penal amount
  * assign accumulation_flag + note
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd

INPUT_FILE = "output_step5/gold_lidac_classified.csv"
OUTPUT_DIR = "output_step7"

ACCUM_FILE = os.path.join(OUTPUT_DIR, "gold_zip_accumulation_flags.csv")
EXCEPTIONS_FILE = os.path.join(OUTPUT_DIR, "exceptions_step7_missing_zip_or_premium.csv")


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] {msg}")


def main() -> None:
    log("=== STEP 7 – ZIP Accumulation START ===")

    if not os.path.exists(INPUT_FILE):
        log(f"[ERROR] Input file not found: {INPUT_FILE}")
        return

    log(f"Loading LIDAC-classified data from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, dtype="unicode", low_memory=False)
    log(f"Loaded {len(df)} project rows from Step 5.")

    # Ensure expected columns exist
    required_cols = ["zip_code", "carrier_id", "gross_premium", "penal_amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log(f"[ERROR] Missing required columns for Step 7: {missing}")
        log(f"Available columns: {list(df.columns)}")
        return

    # Convert numeric columns
    for col in ["gross_premium", "penal_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Basic ZIP validity – rely on Step 3's work if present
    has_valid_flag = "zip_valid_flag" in df.columns
    if has_valid_flag:
        log("Using zip_valid_flag from earlier steps.")
        df["zip_valid_flag"] = df["zip_valid_flag"].astype(str).str.lower().isin(["true", "1", "yes"])
    else:
        df["zip_valid_flag"] = df["zip_code"].astype(str).str.match(r"^\d{5}$", na=False)

    # Exceptions: missing or invalid ZIP
    exceptions_mask = df["zip_code"].isna() | (~df["zip_valid_flag"])
    exceptions = df[exceptions_mask].copy()
    valid = df[~exceptions_mask].copy()

    log(f"Rows with missing/invalid ZIP: {len(exceptions)}")
    log(f"Rows used for accumulation:   {len(valid)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    exceptions.to_csv(EXCEPTIONS_FILE, index=False)

    if len(valid) == 0:
        log("[WARN] No valid rows for accumulation. Exiting.")
        # Still write an empty accumulation file for consistency
        pd.DataFrame(
            columns=[
                "zip_code",
                "project_count",
                "carriers_involved",
                "total_gross_premium",
                "total_penal_amount",
                "accumulation_flag",
                "accumulation_note",
            ]
        ).to_csv(ACCUM_FILE, index=False)
        log("=== STEP 7 – COMPLETE (no valid rows) ===")
        return

    # Group by ZIP
    agg = (
        valid.groupby("zip_code", dropna=False)
             .agg(
                 project_count=("project_id", "count"),
                 carriers_involved=("carrier_id", "nunique"),
                 total_gross_premium=("gross_premium", "sum"),
                 total_penal_amount=("penal_amount", "sum"),
             )
             .reset_index()
    )

    # Replace NaNs with 0 in numeric cols
    for col in ["project_count", "carriers_involved", "total_gross_premium", "total_penal_amount"]:
        agg[col] = agg[col].fillna(0)

    # Convert counts to int
    agg["project_count"] = agg["project_count"].astype(int)
    agg["carriers_involved"] = agg["carriers_involved"].astype(int)

    # Flag logic (tweak thresholds as you like)
    def flag_row(row):
        proj = row["project_count"]
        penl = row["total_penal_amount"]

        if proj >= 4 or penl >= 5_000_000:
            return "RED", "High density or penal amount – review"
        elif proj >= 2 or penl >= 2_000_000:
            return "YELLOW", "Moderate density or penal amount"
        else:
            return "GREEN", "Low density / low penal amount"

    flags, notes = [], []
    for _, r in agg.iterrows():
        f, n = flag_row(r)
        flags.append(f)
        notes.append(n)

    agg["accumulation_flag"] = flags
    agg["accumulation_note"] = notes

    agg.to_csv(ACCUM_FILE, index=False)

    log(f"=== STEP 7 – COMPLETE ===")
    log(f"ZIP accumulation file: {ACCUM_FILE} (rows: {len(agg)})")
    log(f"Exceptions (missing ZIP): {EXCEPTIONS_FILE} (rows: {len(exceptions)})")

    # Print a quick preview
    log("\nTop 5 ZIP accumulation rows:")
    print(agg.sort_values("total_penal_amount", ascending=False).head(5).to_string(index=False))


if __name__ == "__main__":
    main()
