#!/usr/bin/env python3
"""
step6_journal_entry_mapping.py

Step 6 – Journal Entry mapping for Intacct.

Reads:
  - output_step5/gold_lidac_classified.csv

Produces:
  - output_step6/gold_journal_entries_for_intacct.csv
  - output_step6/exceptions_step6_journal_mapping.csv

For each project row, we build 2–3 journal entry lines:
  1) DR MGA Payable (Net Premium)
  2) CR Written Premium Revenue (Gross Premium)
  3) DR Commission Expense (Commission)   [if commission_amount > 0]

We also carry risk/impact dimensions (ZIP, tract, LIDAC, etc.) into each line.
"""

import os
import sys
from typing import Optional, List

import pandas as pd

# ----------------------------------------------------
# Paths
# ----------------------------------------------------
STEP5_FILE = "output_step5/gold_lidac_classified.csv"

OUTPUT_DIR = "output_step6"
OUTPUT_JE = os.path.join(OUTPUT_DIR, "gold_journal_entries_for_intacct.csv")
OUTPUT_EXC = os.path.join(OUTPUT_DIR, "exceptions_step6_journal_mapping.csv")

# ----------------------------------------------------
# Column mapping – tuned to your actual Step 5 columns
#
# Step 5 columns you showed:
# ['project_id', 'carrier_id', 'as_of_date', 'effective_date', 'expiration_date',
#  'gross_premium', 'net_premium', 'commission_amount', 'ceded_commission_amount',
#  'commission_rate_pct', 'quota_share_pct', 'penal_amount', 'product_name',
#  'premium_state', 'principal_name', 'principal_address', 'broker_name',
#  'broker_state', 'obligee_name', 'obligee_state', 'source_file',
#  'source_row_number', 'ingestion_timestamp_utc', 'zip_code', 'zip_valid_flag',
#  'zip_error_reason', 'state_fips', 'county_fips', 'tract_fips',
#  'tract_match_ratio', 'tract_match_flag', 'tract_error_reason',
#  'cejst_disadvantaged', 'lidac_eligible', 'lidac_reason']
# ----------------------------------------------------
COLUMN_MAP = {
    # numeric fields
    "gross_premium": [
        "gross_premium",         # your actual column
    ],
    "net_premium": [
        "net_premium",           # your actual column
    ],
    "commission": [
        "commission_amount",     # your actual column
    ],
    "ceded_commission": [
        "ceded_commission_amount",   # your actual column
    ],
    "penal_amount": [
        "penal_amount",          # your actual column
    ],

    # dates / attributes
    "project_id": [
        "project_id",
    ],
    "as_of_date": [
        "as_of_date",
    ],
    "effective_date": [
        "effective_date",
    ],
    "expiration_date": [
        "expiration_date",
    ],
    "product": [
        "product_name",
    ],
    "premium_state": [
        "premium_state",
    ],
    "principal": [
        "principal_name",
    ],
    "principal_address": [
        "principal_address",
    ],
    "carrier_name": [
        "carrier_id",            # using ID as carrier dimension for now
    ],

    # geography / eligibility
    "zip": [
        "zip_code",
    ],
    "state_fips": [
        "state_fips",
    ],
    "county_fips": [
        "county_fips",
    ],
    "tract_fips": [
        "tract_fips",
    ],
    "lidac_eligible": [
        "lidac_eligible",
    ],
    "lidac_reason": [
        "lidac_reason",
    ],
}


# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def ensure_output_dir(path: str) -> None:
    """Create parent directory if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)


def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the first column in df that matches any name in candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def safe_to_float(x) -> float:
    """Normalize money-like values to float."""
    if pd.isna(x):
        return 0.0
    s = str(x).replace("$", "").replace(",", "").strip()
    if s == "":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def build_journal_entries(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """
    For each input row, build 2–3 journal entry lines:
      - DR MGA Payable (Net Premium)
      - CR Written Premium Revenue (Gross Premium)
      - DR Commission Expense (Commission) [if > 0]

    Returns:
      - je_df: long-format dataframe with 1 row per JE line
      - exc_df: rows that could not be mapped (no amounts)
    """
    records = []
    exceptions = []

    # Resolve column names once
    col_gross = find_column(df, COLUMN_MAP["gross_premium"])
    col_net = find_column(df, COLUMN_MAP["net_premium"])
    col_comm = find_column(df, COLUMN_MAP["commission"])
    col_ceded = find_column(df, COLUMN_MAP["ceded_commission"])
    col_penal = find_column(df, COLUMN_MAP["penal_amount"])

    col_project_id = find_column(df, COLUMN_MAP["project_id"])
    col_as_of = find_column(df, COLUMN_MAP["as_of_date"])
    col_eff = find_column(df, COLUMN_MAP["effective_date"])
    col_exp = find_column(df, COLUMN_MAP["expiration_date"])
    col_prod = find_column(df, COLUMN_MAP["product"])
    col_state = find_column(df, COLUMN_MAP["premium_state"])
    col_principal = find_column(df, COLUMN_MAP["principal"])
    col_addr = find_column(df, COLUMN_MAP["principal_address"])
    col_carrier = find_column(df, COLUMN_MAP["carrier_name"])

    col_zip = find_column(df, COLUMN_MAP["zip"])
    col_state_fips = find_column(df, COLUMN_MAP["state_fips"])
    col_county_fips = find_column(df, COLUMN_MAP["county_fips"])
    col_tract = find_column(df, COLUMN_MAP["tract_fips"])
    col_lidac_flag = find_column(df, COLUMN_MAP["lidac_eligible"])
    col_lidac_reason = find_column(df, COLUMN_MAP["lidac_reason"])

    # Debug: print resolved mapping
    print("\nResolved column mapping (Step 6):")
    resolved = {
        "gross_premium": col_gross,
        "net_premium": col_net,
        "commission": col_comm,
        "ceded_commission": col_ceded,
        "penal_amount": col_penal,
        "project_id": col_project_id,
        "as_of_date": col_as_of,
        "effective_date": col_eff,
        "expiration_date": col_exp,
        "product": col_prod,
        "premium_state": col_state,
        "principal": col_principal,
        "principal_address": col_addr,
        "carrier_name": col_carrier,
        "zip": col_zip,
        "state_fips": col_state_fips,
        "county_fips": col_county_fips,
        "tract_fips": col_tract,
        "lidac_eligible": col_lidac_flag,
        "lidac_reason": col_lidac_reason,
    }
    for k, v in resolved.items():
        print(f"  {k:18s} -> {v}")

    # Check required columns
    required_cols = [col_gross, col_net]
    if any(c is None for c in required_cols):
        missing = [
            name
            for name, col in zip(["gross_premium", "net_premium"], required_cols)
            if col is None
        ]
        raise ValueError(f"Missing required columns in Step 5 data: {missing}")

    # Build JE lines
    for idx, row in df.iterrows():
        gross = safe_to_float(row.get(col_gross))
        net = safe_to_float(row.get(col_net))
        comm = safe_to_float(row.get(col_comm)) if col_comm else 0.0

        # If everything is zero, push to exceptions
        if gross == 0.0 and net == 0.0 and comm == 0.0:
            exceptions.append(row.to_dict())
            continue

        # Shared dimensions
        project_id = row.get(col_project_id) if col_project_id else None
        as_of_date = row.get(col_as_of) if col_as_of else None
        effective_date = row.get(col_eff) if col_eff else None
        expiration_date = row.get(col_exp) if col_exp else None
        product = row.get(col_prod) if col_prod else None
        premium_state = row.get(col_state) if col_state else None
        principal = row.get(col_principal) if col_principal else None
        addr = row.get(col_addr) if col_addr else None
        carrier = row.get(col_carrier) if col_carrier else None

        zip_code = row.get(col_zip) if col_zip else None
        state_fips = row.get(col_state_fips) if col_state_fips else None
        county_fips = row.get(col_county_fips) if col_county_fips else None
        tract_fips = row.get(col_tract) if col_tract else None
        lidac_eligible = row.get(col_lidac_flag) if col_lidac_flag else None
        lidac_reason = row.get(col_lidac_reason) if col_lidac_reason else None

        # Simple JE ID (can be replaced later with real key)
        journal_entry_id = f"JE_{idx:06d}"

        # 1) DR MGA Payable – Net Premium
        records.append(
            {
                "journal_entry_id": journal_entry_id,
                "line_number": 1,
                "dr_cr": "DR",
                "gl_account": "2300 - MGA Payable",  # placeholder; to be mapped in Intacct
                "amount": net,
                "currency": "USD",
                "effective_date": effective_date,
                "as_of_date": as_of_date,
                "expiration_date": expiration_date,
                "description": f"Net Premium - {product or ''}",

                "project_id": project_id,
                "carrier": carrier,
                "product": product,
                "premium_state": premium_state,
                "principal": principal,
                "principal_address": addr,
                "zip": zip_code,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "tract_fips": tract_fips,
                "lidac_eligible": lidac_eligible,
                "lidac_reason": lidac_reason,

                "source_row_index": idx,
            }
        )

        # 2) CR Written Premium Revenue – Gross Premium
        records.append(
            {
                "journal_entry_id": journal_entry_id,
                "line_number": 2,
                "dr_cr": "CR",
                "gl_account": "4000 - Written Premium Revenue",  # placeholder
                "amount": gross,
                "currency": "USD",
                "effective_date": effective_date,
                "as_of_date": as_of_date,
                "expiration_date": expiration_date,
                "description": f"Gross Premium - {product or ''}",

                "project_id": project_id,
                "carrier": carrier,
                "product": product,
                "premium_state": premium_state,
                "principal": principal,
                "principal_address": addr,
                "zip": zip_code,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "tract_fips": tract_fips,
                "lidac_eligible": lidac_eligible,
                "lidac_reason": lidac_reason,

                "source_row_index": idx,
            }
        )

        # 3) DR Commission Expense – Commission (if > 0)
        if comm != 0.0:
            records.append(
                {
                    "journal_entry_id": journal_entry_id,
                    "line_number": 3,
                    "dr_cr": "DR",
                    "gl_account": "5200 - Commission Expense",  # placeholder
                    "amount": comm,
                    "currency": "USD",
                    "effective_date": effective_date,
                    "as_of_date": as_of_date,
                    "expiration_date": expiration_date,
                    "description": f"Commission - {product or ''}",

                    "project_id": project_id,
                    "carrier": carrier,
                    "product": product,
                    "premium_state": premium_state,
                    "principal": principal,
                    "principal_address": addr,
                    "zip": zip_code,
                    "state_fips": state_fips,
                    "county_fips": county_fips,
                    "tract_fips": tract_fips,
                    "lidac_eligible": lidac_eligible,
                    "lidac_reason": lidac_reason,

                    "source_row_index": idx,
                }
            )

    je_df = pd.DataFrame.from_records(records)
    exc_df = pd.DataFrame(exceptions) if exceptions else pd.DataFrame()

    return je_df, exc_df


# ----------------------------------------------------
# Main
# ----------------------------------------------------
def main():
    if not os.path.exists(STEP5_FILE):
        raise FileNotFoundError(f"Step 5 file not found: {STEP5_FILE}")

    print(f"Loading LIDAC-classified data from: {STEP5_FILE}")
    df = pd.read_csv(STEP5_FILE, dtype=str)
    print(f"Loaded {len(df):,} project rows from Step 5.")

    print("\nAvailable columns in Step 5:")
    print(df.columns.tolist())

    je_df, exc_df = build_journal_entries(df)

    ensure_output_dir(OUTPUT_JE)
    ensure_output_dir(OUTPUT_EXC)

    je_df.to_csv(OUTPUT_JE, index=False)
    exc_df.to_csv(OUTPUT_EXC, index=False)

    print("\n=== STEP 6 COMPLETE ===")
    print(f"Journal entries file: {OUTPUT_JE} (rows: {len(je_df):,})")
    print(f"Exceptions (could not map to JE): {OUTPUT_EXC} (rows: {len(exc_df):,})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[ERROR]", e)
        sys.exit(1)
