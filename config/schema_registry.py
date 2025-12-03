# schema_registry.py
"""
Central schema registry for GreenieRE Phase 1 pipeline.

Defines:
- SILVER_PROJECT_COLUMNS / DTYPES   (Step 2 output)
- SILVER_WITH_ZIP_COLUMNS / DTYPES (Step 3 output)
"""

from __future__ import annotations

# -------------------------------------------------------
# Step 2: silver_project_records (one row per project)
# -------------------------------------------------------

SILVER_PROJECT_COLUMNS = [
    "project_id",
    "carrier_id",
    "source_file",
    "source_row_number",
    "ingestion_timestamp_utc",
    "as_of_date",
    "effective_date",
    "expiration_date",
    "gross_premium",
    "net_premium",
    "commission_amount",
    "ceded_commission_amount",
    "commission_rate_pct",
    "quota_share_pct",
    "penal_amount",
    "product_name",
    "premium_state",
    "principal_name",
    "principal_address",
    "broker_name",
    "broker_state",
    "obligee_name",
    "obligee_state",
]

SILVER_PROJECT_DTYPES = {
    # identifiers / lineage
    "project_id": "string",
    "carrier_id": "string",
    "source_file": "string",
    "source_row_number": "Int64",
    "ingestion_timestamp_utc": "datetime64[ns]",
    "as_of_date": "datetime64[ns]",

    # dates
    "effective_date": "datetime64[ns]",
    "expiration_date": "datetime64[ns]",

    # numeric amounts
    "gross_premium": "float",
    "net_premium": "float",
    "commission_amount": "float",
    "ceded_commission_amount": "float",
    "commission_rate_pct": "float",   # e.g. 0.15 for 15% or 15.0 if you prefer
    "quota_share_pct": "float",       # same comment as above
    "penal_amount": "float",

    # categorical / text
    "product_name": "string",
    "premium_state": "string",
    "principal_name": "string",
    "principal_address": "string",
    "broker_name": "string",
    "broker_state": "string",
    "obligee_name": "string",
    "obligee_state": "string",
}

# -------------------------------------------------------
# Step 3: silver_project_with_zip
#   (extends Step 2 with zip + validation flags)
# -------------------------------------------------------

SILVER_WITH_ZIP_COLUMNS = SILVER_PROJECT_COLUMNS + [
    "zip_code",
    "zip_valid_format",
    "zip_in_hud",
    "zip_error_reason",
    "zip_valid_flag",
]

SILVER_WITH_ZIP_DTYPES = {
    **SILVER_PROJECT_DTYPES,
    "zip_code": "string",
    "zip_valid_format": "boolean",
    "zip_in_hud": "boolean",
    "zip_error_reason": "string",
    "zip_valid_flag": "boolean",
}
