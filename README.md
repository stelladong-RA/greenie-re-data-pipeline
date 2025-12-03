Greenie Re – Automated Phase 1 Data Pipeline

End-to-End MGA/Carrier Bordereaux Processing, Data Quality Controls, ZIP/Tract Mapping, CEJST/LIDAC Eligibility, and Intacct Integration

⸻

📌 Overview

This repository contains the full Phase 1 automated data pipeline for Greenie Re’s underwriting and compliance workflow.
The pipeline ingests MGA/Carrier bordereaux files, performs multi-layered quality checks, standardizes schemas, enriches records with geospatial identifiers (ZIP → Census Tract), applies CEJST/LIDAC eligibility classification, runs accumulation analytics, and generates reconciled accounting outputs for Sage Intacct.

The entire workflow is built to support:
    •    Regulatory compliance (CEJST, Climate and Economic Justice Screening Tool v2.0)
    •    Geospatial eligibility using HUD ZIP–Census Tract crosswalks
    •    MGA/Carrier operational automation (intake → validation → enrichment)
    •    Accounting / Journal Entry mappings for financial systems
    •    Risk accumulation detection at the ZIP level
    •    Production-scale extensibility for Phase 2 deployment

This Phase 1 repo uses synthetic carrier data for demonstration and validation of the full pipeline capability.

⸻

🚀 Pipeline Summary (8 Steps)

1. Intake & Data Quality
    •    Ingests all raw .xlsx/.csv files from /raw/
    •    Normalizes headers
    •    Performs structural validation (columns, types, formats)
    •    Produces /output_step1/intake_quality_report.csv

2. Extraction & Normalization
    •    Applies a unified schema registry (schema_registry.py)
    •    Standardizes data types (dates, decimals, strings)
    •    Ensures determinism across carriers
    •    Saves standardized table to /output_step2/silver_project_records.csv

3. ZIP Extraction & Validation
    •    Extracts ZIP codes from addresses using strict regex
    •    Pads ZIPs to 5-digit canonical form
    •    Validates ZIP against the HUD crosswalk table
    •    Saves enriched ZIP table → /output_step3/silver_project_with_zip.csv

4. ZIP → Census Tract Mapping
    •    Uses HUD ZIP–Tract crosswalk
    •    Computes best tract via RES_RATIO (residential weight)
    •    Appends FIPS: state_fips, county_fips, tract_fips
    •    Output: /output_step4/silver_location_enriched.csv

5. CEJST / LIDAC Eligibility Classification
    •    Loads CEJST v2.0 communities dataset
    •    Joins tract_fips to disadvantaged indicators
    •    Determines if project is:
    •    LIDAC Eligible
    •    Partially Eligible
    •    Not Eligible
    •    Output: /output_step5/gold_lidac_classified.csv

6. Journal Entry Mapping (Intacct)
    •    Maps premiums, commissions, penal amounts, program IDs to chart-of-account templates
    •    Produces ready-to-upload JE CSV
    •    Output: /output_step6/gold_journal_entries_for_intacct.csv

7. ZIP-Level Accumulation
    •    Aggregates project counts, premium sums, penal exposures
    •    Flags RED/YELLOW/GREEN accumulation zones
    •    Output: /output_step7/gold_zip_accumulation_flags.csv

8. Final Phase 1 Output Packaging

Creates complete Phase 1 deliverables:
    •    Intacct export
    •    LIDAC / CEJST report
    •    Accumulation report
    •    Exceptions summary

All saved under /output_step8/.

⸻

📂 Folder Structure

greenie-re-data-pipeline/
│
├── raw/                        # Input carrier bordereaux files
│   ├── C001_*.xlsx
│   ├── C002_*.xlsx
│   └── C003_*.xlsx
│
├── config/
│   ├── schema_registry.py      # Central schema definition
│   ├── generate_data.py        # Synthetic data generator
│   ├── download_hud_zip_tract_crosswalk.py
│   └── external_data/
│       ├── hud_zip_tract_crosswalk.csv
│       └── cejst_v2_communities.csv
│
├── doc/
│   ├── Phase1_Architecture.tex
│   ├── Phase1_Architecture.pdf
│   └── tikz diagrams
│
├── output_step1/
├── output_step2/
├── output_step3/
├── output_step4/
├── output_step5/
├── output_step6/
├── output_step7/
└── output_step8/


⸻

🛠️ Installation & Dependencies

Requirements
    •    Python 3.10+
    •    pip / conda environment

Install Dependencies

pip install -r requirements.txt

Dependencies include:
    •    pandas
    •    numpy
    •    openpyxl
    •    uszipcode (optional)
    •    requests
    •    python-dateutil

⸻

▶️ Running the Pipeline

Run each step sequentially:

python step1_intake_and_quality.py
python step2_extraction_normalization.py
python step3_zip_extraction.py
python step4_zip_to_tract_mapping.py
python step5_lidac_eligibility_cejst.py
python step6_journal_entry_mapping.py
python step7_zip_accumulation.py
python step8_phase1_outputs.py

Alternatively, you can create a master runner script in Phase 2.

⸻

📥 Inputs

Place client files in:

/raw/*.xlsx

Assumptions:
    •    Each file is a standard bordereaux format
    •    Columns must include key attributes defined in schema_registry.py

⸻

📤 Outputs

Final packaged results are stored in:

/output_step8/

Contains:
    •    Intacct JE CSV
    •    LIDAC/CEJST eligibility file
    •    ZIP-level accumulation file
    •    Exceptions report

⸻

🌱 Why This Pipeline Matters

This architecture demonstrates:
    •    A fully automated underwriting & compliance micro-pipeline
    •    Production-grade geospatial linking
    •    Deterministic eligibility classification
    •    Real-world accounting-system readiness
    •    Repeatable and scalable ingestion for hundreds of carriers / MGAs

Greenie Re can scale this to:
    •    Automate risk analytics
    •    Build dynamic LIDAC dashboards
    •    Integrate with API-based carrier feeds
    •    Support annual/quarterly filings
    •    Provide regulators auditable data lineage

⸻

🧩 Next Steps (Phase 2 & Phase 3)

Phase    Deliverables
Phase 2    UI portal, automated scheduler, API ingestion, S3/Blob storage, full audit logs
Phase 3    Production deployment, authentication, API endpoints for carriers & MGAs, dashboards, underwriting engine


⸻

👥 Authors

Greenie Re
Co-founders:
    •    Stella Dong – Applied Mathematics, ML Engineering, Reinsurance Data Systems
    •    James Finlay – Wharton, Reinsurance Strategy, Risk Finance

