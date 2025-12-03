#!/usr/bin/env python3
"""
step1_ingest_raw_files.py

Step 1 (Bronze layer):

- Read all raw bordereaux files from data/raw/
- Support .xlsx and .csv
- Add metadata:
    - carrier_id (derived from filename)
    - source_file
    - source_row_number
    - ingestion_timestamp_utc
- DO NOT clean values yet; keep raw strings.
- Save combined output to:
    output_step1/bronze_bordereaux_raw.csv
"""

import os
from datetime import datetime, timezone

import pandas as pd

from config.schemas import STEP1_SCHEMA
from schema_utils import enforce_schema

RAW_DIR = "data/raw"
OUTPUT_DIR = "output_step1"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "bronze_bordereaux_raw.csv")


def list_raw_files(raw_dir: str):
    """Return list of .xlsx and .csv files in data/raw/."""
    files = []
    for name in os.listdir(raw_dir):
        if name.lower().endswith((".xlsx", ".xls", ".csv")):
            files.append(os.path.join(raw_dir, name))
    return sorted(files)


def infer_carrier_id_from_filename(path: str) -> str:
    """
    Example:
        data/raw/CarrierAlpha_Phase1_Bordereaux.xlsx -> CarrierAlpha
    """
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    # take first chunk before first underscore
    carrier_id = name.split("_")[0]
    return carrier_id


def read_raw_file(path: str) -> pd.DataFrame:
    """
    Read a raw bordereaux file.
    We want:
        - all columns as 'object'/string initially
        - no type coercion
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=str)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = list_raw_files(RAW_DIR)
    if not files:
        print(f"No raw files found in {RAW_DIR}")
        return

    print(f"Found {len(files)} raw file(s) in {RAW_DIR}:")
    for f in files:
        print(f"  - {f}")

    all_rows = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for path in files:
        carrier_id = infer_carrier_id_from_filename(path)
        print(f"\nReading file: {path} (carrier_id={carrier_id})")

        df = read_raw_file(path)

        # Add metadata columns
        df["carrier_id"] = carrier_id
        df["source_file"] = os.path.basename(path)
        # Row number starting at 1 for human-friendly tracing
        df["source_row_number"] = (df.index + 1).astype("Int64")
        df["ingestion_timestamp_utc"] = now_utc

        print(f"  -> Loaded {len(df)} rows from {path}")
        all_rows.append(df)

    if not all_rows:
        print("No rows loaded, nothing to write.")
        return

    combined = pd.concat(all_rows, ignore_index=True)

    # Reorder columns: metadata first, then raw fields in STEP1_SCHEMA order
    # (where present).
    cols_order = []
    for col in STEP1_SCHEMA.keys():
        if col in combined.columns:
            cols_order.append(col)

    # Add any extra columns at the end
    for col in combined.columns:
        if col not in cols_order:
            cols_order.append(col)

    combined = combined[cols_order]

    # Enforce schema (types)
    combined = enforce_schema(combined, STEP1_SCHEMA, step_name="STEP1")

    # Write output
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"\n=== STEP 1 COMPLETE ===")
    print(f"Combined raw bordereaux written to: {OUTPUT_FILE}")
    print(f"Total rows: {len(combined)}")


if __name__ == "__main__":
    main()
