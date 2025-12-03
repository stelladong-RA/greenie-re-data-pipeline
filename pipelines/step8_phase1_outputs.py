#!/usr/bin/env python3
"""
step8_phase1_outputs.py

Step 8 – Phase 1 Outputs Packaging

This script collects the main gold-layer outputs from Steps 5–7
and packages them into a small set of deliverables for Wenhua:

1. Intacct-ready journal entries    -> phase1_intacct_export.csv
2. LIDAC / CEJST eligibility report -> phase1_lidac_report.csv
3. ZIP accumulation summary         -> phase1_zip_accumulation.csv
4. Exceptions summary (per step)    -> phase1_exceptions_summary.csv

Inputs (expected paths)
-----------------------
- output_step6/gold_journal_entries_for_intacct.csv
- output_step5/gold_lidac_classified.csv
- output_step7/gold_zip_accumulation_flags.csv

Optional inputs (if they exist)
-------------------------------
- output_step2/exceptions_step2_extraction.csv            (name guess)
- output_step3/exceptions_step3_zip_issues.csv
- output_step4/exceptions_step4_tract_mapping.csv
- output_step5/exceptions_step5_missing_cejst_match.csv
- output_step6/exceptions_step6_journal_mapping.csv
- output_step7/exceptions_step7_missing_zip_or_premium.csv

Outputs
-------
- output_step8/phase1_intacct_export.csv
- output_step8/phase1_lidac_report.csv
- output_step8/phase1_zip_accumulation.csv
- output_step8/phase1_exceptions_summary.csv
"""

import os
from datetime import datetime

import pandas as pd

# -----------------------------
# Config – input and output paths
# -----------------------------
JE_FILE = "output_step6/gold_journal_entries_for_intacct.csv"
LIDAC_FILE = "output_step5/gold_lidac_classified.csv"
ZIP_ACC_FILE = "output_step7/gold_zip_accumulation_flags.csv"

OUTPUT_DIR = "output_step8"

OUT_JE = os.path.join(OUTPUT_DIR, "phase1_intacct_export.csv")
OUT_LIDAC = os.path.join(OUTPUT_DIR, "phase1_lidac_report.csv")
OUT_ZIP_ACC = os.path.join(OUTPUT_DIR, "phase1_zip_accumulation.csv")
OUT_EXCEPTIONS_SUMMARY = os.path.join(OUTPUT_DIR, "phase1_exceptions_summary.csv")


EXCEPTION_FILES = [
    # (step_label, file_path)
    ("STEP2", "output_step2/exceptions_step2_extraction.csv"),
    ("STEP3", "output_step3/exceptions_step3_zip_issues.csv"),
    ("STEP4", "output_step4/exceptions_step4_tract_mapping.csv"),
    ("STEP5", "output_step5/exceptions_step5_missing_cejst_match.csv"),
    ("STEP6", "output_step6/exceptions_step6_journal_mapping.csv"),
    ("STEP7", "output_step7/exceptions_step7_missing_zip_or_premium.csv"),
]


# -----------------------------
# Helpers
# -----------------------------
def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    print(f"[{ts}Z] {msg}")


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_read_csv(path: str, description: str) -> pd.DataFrame:
    """Read CSV if it exists, otherwise return empty DataFrame."""
    if not os.path.exists(path):
        log(f"[WARN] {description} file not found: {path}")
        return pd.DataFrame()
    log(f"Loading {description} from: {path}")
    df = pd.read_csv(path, low_memory=False)
    log(f"{description} rows: {len(df)}")
    return df


# -----------------------------
# Builders
# -----------------------------
def build_intacct_export() -> pd.DataFrame:
    """
    Build the Intacct export file from Step 6 journal entries.
    For now we pass through the Step 6 file, but we can also subset
    or rename columns if needed.
    """
    df = safe_read_csv(JE_FILE, "Step 6 journal entries")
    if df.empty:
        log("[STEP8] No journal entry rows found; Intacct export will be empty.")
        return df

    # Optionally enforce a clear column order if present
    preferred_cols = [
        "journal_batch_id",
        "project_id",
        "carrier_id",
        "as_of_date",
        "effective_date",
        "expiration_date",
        "line_number",
        "dr_cr",
        "account_code",
        "amount",
        "currency",
        "premium_state",
        "zip_code",
        "state_fips",
        "county_fips",
        "tract_fips",
        "lidac_eligible",
        "lidac_reason",
        "source_file",
        "source_row_number",
    ]

    cols_to_use = [c for c in preferred_cols if c in df.columns]
    if cols_to_use:
        df = df[cols_to_use + [c for c in df.columns if c not in cols_to_use]]

    return df


def build_lidac_report() -> pd.DataFrame:
    """
    Build a compact LIDAC / CEJST eligibility report from Step 5 output.
    """
    df = safe_read_csv(LIDAC_FILE, "Step 5 LIDAC-classified data")
    if df.empty:
        log("[STEP8] No LIDAC rows found; LIDAC report will be empty.")
        return df

    # Choose a concise, business-facing subset of columns (if present)
    preferred_cols = [
        "project_id",
        "carrier_id",
        "as_of_date",
        "effective_date",
        "expiration_date",
        "gross_premium",
        "net_premium",
        "penal_amount",
        "product_name",
        "premium_state",
        "principal_name",
        "principal_address",
        "zip_code",
        "state_fips",
        "county_fips",
        "tract_fips",
        "cejst_disadvantaged",
        "lidac_eligible",
        "lidac_reason",
        "source_file",
        "source_row_number",
    ]

    cols_to_use = [c for c in preferred_cols if c in df.columns]
    if cols_to_use:
        df = df[cols_to_use + [c for c in df.columns if c not in cols_to_use]]

    return df


def build_zip_accumulation() -> pd.DataFrame:
    """
    Pass-through of Step 7 ZIP accumulation flags.
    """
    df = safe_read_csv(ZIP_ACC_FILE, "Step 7 ZIP accumulation flags")
    if df.empty:
        log("[STEP8] No ZIP accumulation rows found; accumulation file will be empty.")
    return df


def build_exceptions_summary() -> pd.DataFrame:
    """
    Summarize row counts across all exception files from Steps 2–7.
    """
    records = []
    for step_label, path in EXCEPTION_FILES:
        if not os.path.exists(path):
            # It's fine if some steps didn't produce exceptions
            continue
        try:
            df_exc = pd.read_csv(path, low_memory=False)
            row_count = len(df_exc)
        except Exception as e:
            log(f"[WARN] Could not read exceptions file {path}: {e}")
            row_count = None

        records.append(
            {
                "step": step_label,
                "file": path,
                "rows": row_count,
            }
        )

    if not records:
        log("[STEP8] No exceptions files found; exceptions summary will be empty.")
        return pd.DataFrame(columns=["step", "file", "rows"])

    summary_df = pd.DataFrame(records)
    return summary_df


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    log("=== STEP 8 – Phase 1 Outputs Packaging START ===")
    ensure_output_dir(OUTPUT_DIR)

    # 1. Intacct export
    je_df = build_intacct_export()
    je_df.to_csv(OUT_JE, index=False)
    log(f"[STEP8] Wrote Intacct export to: {OUT_JE} (rows: {len(je_df)})")

    # 2. LIDAC / CEJST report
    lidac_df = build_lidac_report()
    lidac_df.to_csv(OUT_LIDAC, index=False)
    log(f"[STEP8] Wrote LIDAC report to: {OUT_LIDAC} (rows: {len(lidac_df)})")

    # 3. ZIP accumulation summary
    zip_acc_df = build_zip_accumulation()
    zip_acc_df.to_csv(OUT_ZIP_ACC, index=False)
    log(f"[STEP8] Wrote ZIP accumulation summary to: {OUT_ZIP_ACC} (rows: {len(zip_acc_df)})")

    # 4. Exceptions summary
    exc_summary_df = build_exceptions_summary()
    exc_summary_df.to_csv(OUT_EXCEPTIONS_SUMMARY, index=False)
    log(f"[STEP8] Wrote exceptions summary to: {OUT_EXCEPTIONS_SUMMARY} (rows: {len(exc_summary_df)})")

    log("=== STEP 8 – Phase 1 Outputs Packaging COMPLETE ===")


if __name__ == "__main__":
    main()
