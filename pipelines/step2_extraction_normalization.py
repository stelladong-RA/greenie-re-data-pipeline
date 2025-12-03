#!/usr/bin/env python3
"""
STEP 2 — Extraction & Normalization (Silver layer)

- Reads raw MGA bordereaux from data/raw/*.csv / *.xlsx
- Maps Wenhua's raw columns → standardized silver schema
- Normalizes dates, numbers, text
- Adds lineage fields:
    * carrier_id  (from filename)
    * project_id  (synthetic, unique per row)
    * source_file
    * source_row_number
    * ingestion_timestamp_utc
    * as_of_date (same as ingestion date, for now)

Outputs:
- output_step2/silver_project_records.csv
"""

from __future__ import annotations
import os
import sys
from datetime import datetime

from typing import List

import numpy as np
import pandas as pd

from schema_registry import SILVER_PROJECT_COLUMNS, SILVER_PROJECT_DTYPES

RAW_DIR = "data/raw"
OUTPUT_DIR = "output_step2"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "silver_project_records.csv")


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] {msg}")


RAW_TO_STANDARD = {
    "Effective Date": "effective_date",
    "Expiration Date": "expiration_date",
    "Gross Premium": "gross_premium",
    "Quota Share %": "quota_share_pct",
    "Commission Rate": "commission_rate_pct",
    "Commission": "commission_amount",
    "Ceded Commission": "ceded_commission_amount",
    "Net Premium": "net_premium",
    "Product": "product_name",
    "Premium State": "premium_state",
    "Principal": "principal_name",
    "Principal / Account Mailing Address": "principal_address",
    "Penal Amount": "penal_amount",
    "Broker Name": "broker_name",
    "Broker State": "broker_state",
    "Obligee Name": "obligee_name",
    "Obligee State": "obligee_state",
}


NUMERIC_COLUMNS = [
    "gross_premium",
    "net_premium",
    "commission_amount",
    "ceded_commission_amount",
    "commission_rate_pct",
    "quota_share_pct",
    "penal_amount",
]

DATE_COLUMNS = [
    "effective_date",
    "expiration_date",
    "as_of_date",  # will be filled from ingestion date
]


def list_raw_files() -> List[str]:
    if not os.path.isdir(RAW_DIR):
        log(f"[ERROR] Raw directory not found: {RAW_DIR}")
        sys.exit(1)

    files = []
    for name in os.listdir(RAW_DIR):
        path = os.path.join(RAW_DIR, name)
        if not os.path.isfile(path):
            continue
        lower = name.lower()
        if lower.endswith(".csv") or lower.endswith(".xlsx"):
            files.append(path)

    if not files:
        log(f"[ERROR] No .csv/.xlsx files found in {RAW_DIR}")
        sys.exit(1)

    log(f"Found {len(files)} raw files in {RAW_DIR}:")
    for f in files:
        log(f"  - {f}")
    return files


def infer_carrier_id_from_filename(path: str) -> str:
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    # Example: "CarrierAlpha_Phase1_Bordereaux" → "CarrierAlpha"
    parts = name.split("_")
    return parts[0] if parts else name


def clean_numeric(series: pd.Series) -> pd.Series:
    if series.dtype == "float" or series.dtype == "int":
        return series.astype(float)

    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace("", np.nan)
        .pipe(pd.to_numeric, errors="coerce")
    )


def normalize_single_file(path: str) -> pd.DataFrame:
    log(f"Reading raw file: {path}")
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        raw = pd.read_csv(path, dtype="unicode", keep_default_na=False, na_values=[""])
    else:
        raw = pd.read_excel(path, dtype="unicode")

    log(f"  Raw rows: {len(raw)}")

    # Standardize column names: strip spaces
    raw_cols = {c: c.strip() for c in raw.columns}
    raw = raw.rename(columns=raw_cols)

    # Map to standard names
    df = pd.DataFrame()
    for raw_col, std_col in RAW_TO_STANDARD.items():
        if raw_col in raw.columns:
            df[std_col] = raw[raw_col]
        else:
            df[std_col] = np.nan  # missing in this file

    # Lineage fields
    carrier_id = infer_carrier_id_from_filename(path)
    df["carrier_id"] = carrier_id
    df["source_file"] = os.path.basename(path)

    # Excel rows typically start at 2 (row 1 header), but we just need relative lineage.
    df["source_row_number"] = raw.index.to_series() + 2

    # Ingestion timestamp (UTC) and as_of_date
    now = pd.Timestamp.utcnow().normalize()
    df["ingestion_timestamp_utc"] = pd.Timestamp.utcnow()
    df["as_of_date"] = now

    # Synthetic project_id
    df["project_id"] = [
        f"{carrier_id}_{i+1:06d}" for i in range(len(df))
    ]

    return df


def enforce_silver_schema(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure all silver columns exist
    for col in SILVER_PROJECT_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # Reorder columns
    df = df[SILVER_PROJECT_COLUMNS].copy()

    # Cast numerics
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    # Cast dates
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Cast according to SILVER_PROJECT_DTYPES
    for col, dtype in SILVER_PROJECT_DTYPES.items():
        if dtype.startswith("datetime64"):
            # already handled above
            continue
        if dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "string":
            df[col] = df[col].astype("string")
        else:
            # fallback
            df[col] = df[col].astype(dtype, errors="ignore")

    return df


def main() -> None:
    log("=== STEP 2 — Extraction & Normalization START ===")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = list_raw_files()

    dfs = []
    for path in files:
        df_file = normalize_single_file(path)
        dfs.append(df_file)

    df_all = pd.concat(dfs, ignore_index=True)
    log(f"Combined rows from all carriers: {len(df_all)}")

    df_all = enforce_silver_schema(df_all)

    df_all.to_csv(OUTPUT_FILE, index=False)
    log(f"Silver project records written to: {OUTPUT_FILE}")
    log("=== STEP 2 — COMPLETE ===")


if __name__ == "__main__":
    main()
