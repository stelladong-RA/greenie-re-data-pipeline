#!/usr/bin/env python3
"""
STEP 3 — ZIP Extraction & Validation

Inputs
------
- output_step2/silver_project_records.csv

Optional
--------
- config/hud_zip_tract_crosswalk.csv

Outputs
-------
- output_step3/silver_project_with_zip.csv
- output_step3/exceptions_step3_zip_issues.csv
"""

from __future__ import annotations
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

from schema_registry import (
    SILVER_WITH_ZIP_COLUMNS,
    SILVER_WITH_ZIP_DTYPES,
)

INPUT_FILE = "output_step2/silver_project_records.csv"
HUD_CROSSWALK_FILE = "config/hud_zip_tract_crosswalk.csv"

OUTPUT_DIR = "output_step3"
MAIN_OUTPUT = os.path.join(OUTPUT_DIR, "silver_project_with_zip.csv")
EXCEPTIONS_OUTPUT = os.path.join(OUTPUT_DIR, "exceptions_step3_zip_issues.csv")


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] {msg}")


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def extract_zip(address: str) -> str | None:
    """
    Extract a 5-digit ZIP from address:

    - if not a string -> None
    - keep only digits
    - require at least 5 digits
    - take LAST 5
    - zero-pad to length 5
    """
    if not isinstance(address, str):
        return None

    digits = "".join(c for c in address if c.isdigit())
    if len(digits) < 5:
        return None

    zip5 = digits[-5:]
    zip5 = zip5.zfill(5)
    return zip5


def load_hud_zip_list(path: str) -> set[str] | None:
    if not os.path.exists(path):
        log(f"[WARN] HUD crosswalk not found at {path}; "
            "skipping HUD ZIP membership validation.")
        return None

    log(f"Loading HUD ZIP→tract crosswalk from: {path}")
    hud = pd.read_csv(path, dtype="unicode", low_memory=False)

    # try a few column names
    candidates = ["ZIP", "zip", "zip_code", "usps_zip_pref", "usps_zip"]
    zip_col = None
    for c in candidates:
        if c in hud.columns:
            zip_col = c
            break

    if zip_col is None:
        log(f"[WARN] No ZIP column found in HUD crosswalk. Columns: {list(hud.columns)}")
        return None

    hud["ZIP_norm"] = (
        hud[zip_col]
        .astype(str)
        .str.extract(r"(\d{5})", expand=False)
        .str.zfill(5)
    )
    hud = hud[~hud["ZIP_norm"].isna()].copy()
    hud_zip_set = set(hud["ZIP_norm"].unique().tolist())
    log(f"HUD ZIP universe size: {len(hud_zip_set):,}")
    return hud_zip_set


def enforce_zip_schema(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure all columns exist
    for col in SILVER_WITH_ZIP_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[SILVER_WITH_ZIP_COLUMNS].copy()

    # Cast according to SILVER_WITH_ZIP_DTYPES
    for col, dtype in SILVER_WITH_ZIP_DTYPES.items():
        if dtype.startswith("datetime64"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "boolean":
            df[col] = df[col].astype("boolean")
        elif dtype == "string":
            df[col] = df[col].astype("string")
        else:
            df[col] = df[col].astype(dtype, errors="ignore")

    return df


def main() -> None:
    log("=== STEP 3 — ZIP Extraction & Validation START ===")

    if not os.path.exists(INPUT_FILE):
        log(f"[ERROR] Step 2 file not found: {INPUT_FILE}")
        sys.exit(1)

    log(f"Loading Step 2 data from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, dtype="unicode", low_memory=False)
    log(f"Loaded {len(df):,} rows from Step 2.")

    if "principal_address" not in df.columns:
        log("[ERROR] 'principal_address' not found in Step 2 output.")
        log(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    # 1) Extract ZIP
    log("Extracting ZIP codes from principal_address...")
    df["zip_code"] = df["principal_address"].apply(extract_zip)
    df["zip_code"] = df["zip_code"].astype("string")

    # 2) Format validation
    df["zip_valid_format"] = df["zip_code"].str.match(r"^\d{5}$", na=False)

    # 3) HUD membership check (optional)
    hud_zip_set = load_hud_zip_list(HUD_CROSSWALK_FILE)
    if hud_zip_set is not None:
        df["zip_in_hud"] = df["zip_code"].apply(
            lambda z: (isinstance(z, str) and z in hud_zip_set)
        )
    else:
        df["zip_in_hud"] = pd.NA

    # 4) Error reason + overall flag
    def zip_error_reason(row) -> str:
        if pd.isna(row["zip_code"]):
            return "ZIP missing from address"
        if not row["zip_valid_format"]:
            return "ZIP not 5 digits"
        if hud_zip_set is not None and row["zip_in_hud"] is False:
            return "ZIP not found in HUD crosswalk"
        return ""

    df["zip_error_reason"] = df.apply(zip_error_reason, axis=1)
    df["zip_valid_flag"] = df["zip_error_reason"].eq("")

    # 5) Schema + split
    df = enforce_zip_schema(df)

    exceptions = df[~df["zip_valid_flag"]].copy()
    valid = df[df["zip_valid_flag"]].copy()

    log(f"Valid ZIP rows: {len(valid):,}")
    log(f"ZIP issue rows: {len(exceptions):,}")

    ensure_output_dir(OUTPUT_DIR)
    df.to_csv(MAIN_OUTPUT, index=False)
    log(f"Main ZIP-enriched file written to: {MAIN_OUTPUT}")

    exceptions.to_csv(EXCEPTIONS_OUTPUT, index=False)
    log(f"Exceptions file written to: {EXCEPTIONS_OUTPUT}")

    log("=== STEP 3 — COMPLETE ===")


if __name__ == "__main__":
    main()
