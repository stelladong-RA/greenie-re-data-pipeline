Greenie Re – Automated MGA/Carrier Data Pipeline

End-to-end ingestion, quality checks, ZIP→tract mapping, CEJST/LIDAC eligibility scoring, accounting journal entry generation, and ZIP accumulation analytics.

⸻

📌 Overview

This repository implements the Phase 1 automated data pipeline for Wenhua Zhang’s MGA/carrier program.
It processes raw carrier bordereaux files, validates them, normalizes key financial fields, enriches each project with geographic indicators (ZIP → county → tract), evaluates CEJST/LIDAC eligibility, generates Intacct-ready journal entries, and produces ZIP accumulation reports.

The pipeline is fully modular and built in 8 sequential steps, each producing clean, traceable output in versioned folders (output_stepX/).

It uses a centralized Schema Registry and Data Quality Framework to guarantee consistency across all steps.

⸻

🧱 Pipeline Architecture (8 Steps)

Step 1 — Intake & Data Quality
    •    Reads raw carrier bordereaux from /raw/
    •    Detects file format (.xlsx / .csv)
    •    Applies schema normalization rules
    •    Validates required columns, numeric formats, date fields
    •    Flags anomalies (missing values, invalid types, unknown carriers)

Outputs:
    •    output_step1/silver_intake_parsed.csv
    •    output_step1/exceptions_step1_quality.csv

⸻

Step 2 — Extraction & Normalization
    •    Casts all numeric/date fields using Schema Registry (schema_registry.py)
    •    Standardizes column names across different carriers
    •    Normalizes premium, commission, QS%, dates, addresses
    •    Attaches metadata (source_file, ingestion_timestamp)

Outputs:
    •    output_step2/silver_project_records.csv
    •    output_step2/exceptions_step2_normalization.csv

⸻

Step 3 — ZIP Extraction & Validation
    •    Extracts ZIP code from any free-form address string
    •    Pads ZIP to 5 digits (0211 → 00211)
    •    Validates against HUD ZIP list
    •    Flags invalid or missing ZIPs

Outputs:
    •    output_step3/silver_project_with_zip.csv
    •    output_step3/exceptions_step3_zip_issues.csv

⸻

Step 4 — ZIP → Census Tract Mapping

Uses HUD ZIP-to-tract crosswalk (config/external_data/hud_zip_tract_crosswalk.csv) to compute:
    •    state FIPS
    •    county FIPS
    •    census tract FIPS

Selects tract with highest residential ratio per ZIP.

Outputs:
    •    output_step4/silver_location_enriched.csv
    •    output_step4/exceptions_step4_tract_mapping.csv

⸻

Step 5 — CEJST / LIDAC Eligibility Scoring

Using CEJST v2 dataset (config/external_data/cejst_v2_communities.csv), pipeline:
    •    Joins projects by census tract FIPS
    •    Determines whether tract qualifies as disadvantaged
    •    Computes LIDAC eligibility + reason string

Outputs:
    •    output_step5/gold_lidac_classified.csv
    •    output_step5/exceptions_step5_missing_cejst_match.csv

⸻

Step 6 — Journal Entry Mapping (Intacct)

Converts each project into accounting-ready line items:
    •    Gross premium
    •    Net premium
    •    Commission
    •    Ceded commission
    •    Penal amount
    •    Tract & ZIP metadata
    •    LIDAC eligibility fields

Outputs:
    •    output_step6/gold_journal_entries_for_intacct.csv
    •    output_step6/exceptions_step6_journal_mapping.csv

⸻

Step 7 — ZIP Accumulation Analysis

Aggregates exposure by ZIP:
    •    Project count
    •    Carriers involved
    •    Total gross premium
    •    Total penal amount
    •    Auto-classification (Green / Yellow / Red)

Outputs:
    •    output_step7/gold_zip_accumulation_flags.csv
    •    output_step7/exceptions_step7_missing_zip_or_premium.csv

⸻

Step 8 — Phase 1 Final Output Packaging

Assembles all key stakeholder deliverables:
    •    Intacct export
    •    LIDAC eligibility report
    •    ZIP accumulation report
    •    Exceptions summary across all steps

Outputs:
    •    output_step8/phase1_intacct_export.csv
    •    output_step8/phase1_lidac_report.csv
    •    output_step8/phase1_zip_accumulation.csv
    •    output_step8/phase1_exceptions_summary.csv

⸻

📂 Folder Structure

greenie-re-data-pipeline/
│
├── raw/                          # Source carrier files (.xlsx, .csv)
│   ├── C001_20251201_bordereaux.xlsx
│   ├── C002_20251201_bordereaux.xlsx
│   └── C003_20251201_bordereaux.xlsx
│
├── config/
│   ├── schema_registry.py        # Central schema + dtypes
│   ├── generate_data.py          # Fake data generator (optional)
│   ├── download_hud_zip_tract_crosswalk.py
│   └── external_data/
│       ├── cejst_v2_communities.csv
│       └── hud_zip_tract_crosswalk.csv
│
├── output_step1/ ... output_step8/
│                               # Each step's silver/gold outputs + exceptions
│
├── doc/
│   ├── Phase1_Architecture.tex  # LaTeX technical architecture document
│   ├── Phase1_Architecture.pdf
│   └── diagram assets
│
├── step1_intake_and_quality.py
├── step2_extraction_normalization.py
├── step3_zip_extraction.py
├── step4_zip_to_tract_mapping.py
├── step5_lidac_eligibility_cejst.py
├── step6_journal_entry_mapping.py
├── step7_zip_accumulation.py
└── step8_phase1_outputs.py


⸻

▶️ How to Run the Pipeline

1. Install dependencies

conda create -n greenie python=3.10
conda activate greenie
pip install -r requirements.txt

If requirements.txt does not exist, generate it:

pip freeze > requirements.txt


⸻

2. Place carrier raw files

Place all provided bordereaux files into:

raw/


⸻

3. Run all steps sequentially

You may run each step individually:

python step1_intake_and_quality.py
python step2_extraction_normalization.py
python step3_zip_extraction.py
python step4_zip_to_tract_mapping.py
python step5_lidac_eligibility_cejst.py
python step6_journal_entry_mapping.py
python step7_zip_accumulation.py
python step8_phase1_outputs.py


⸻

4. Final Deliverables

After Step 8, all Phase-1 outputs are located in:

output_step8/


⸻

📥 Expected Inputs

Raw files (raw/)
    •    One or more carrier bordereaux files
    •    Supported formats:
    •    .xlsx
    •    .csv
    •    Must contain a project record table with:
    •    Premium fields
    •    Dates
    •    Addresses for ZIP extraction
    •    Broker / obligee fields
    •    Carrier metadata

⸻

📤 Pipeline Outputs (Business Deliverables)

Output File    Description
phase1_intacct_export.csv    Intacct journal entries
phase1_lidac_report.csv    LIDAC eligibility report
phase1_zip_accumulation.csv    ZIP accumulation + red/yellow/green flags
phase1_exceptions_summary.csv    Cross-step exception logging


⸻

⚙️ Dependencies
    •    Python 3.10+
    •    pandas
    •    numpy
    •    openpyxl (Excel support)
    •    requests (HUD downloads)
    •    python-dateutil
    •    tqdm

(Optional)
    •    jupyter
    •    matplotlib

⸻

🧪 Testing & Validation

Unit tests (future roadmap):

tests/
  ├── test_schema_registry.py
  ├── test_zip_parsing.py
  ├── test_cejst_matching.py
  └── test_journal_mapping.py


⸻

📘 Additional Documentation Included

Located in /doc/:
    •    Phase1_Architecture.pdf
Full system architecture diagram (TikZ)
    •    Phase1_Architecture.tex
LaTeX source with diagram + technical narrative

⸻

🚀 Roadmap (Phase 2+)
    •    API layer for automated ingestion
    •    Deployment on AWS Lambda / ECS
    •    Scheduled runs via Airflow / Prefect
    •    UI dashboard for ZIP + LIDAC visualization
    •    Carrier-specific schema auto-detection
    •    Automated report emailing

⸻

📄 License

To be added once client delivery terms are finalized.
(Default is Proprietary – Not for Redistribution)

⸻

🤝 Contact

Greenie Re / Reinsurance Analytics
📧 stella.dong@reinsuranceanalytics.io

