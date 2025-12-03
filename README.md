# Greenie Re – Automated Phase 1 Data Pipeline

End‑to‑End MGA/Carrier Bordereaux Processing • Data Quality Controls • ZIP/Tract Mapping • CEJST/LIDAC Eligibility • Intacct Integration

---

## 📌 Overview

This repository contains the full **Phase 1 automated data pipeline** for Greenie Re’s underwriting, compliance, and accounting workflow.

The system ingests MGA/Carrier bordereaux files, applies rigorous data‑quality rules, standardizes schemas across carriers, enriches each record with geospatial identifiers (ZIP → Census Tract), classifies CEJST/LIDAC eligibility, performs ZIP‑level accumulation analytics, and generates accounting-ready outputs for **Sage Intacct**.

Synthetic carrier files are used in Phase 1 for demonstration and validation.

---

## 🚀 Pipeline Summary (8 Steps)

### **1. Intake & Data Quality**
- Reads raw `.xlsx` / `.csv` files from `/raw`
- Normalizes headers, validates structure & field formats  
- Generates `output_step1/intake_quality_report.csv`

### **2. Extraction & Normalization**
- Applies unified schema registry (`schema_registry.py`)
- Enforces types across carriers (dates, decimals, strings)
- Exports standardized records → `output_step2/silver_project_records.csv`

### **3. ZIP Extraction & Validation**
- Extracts ZIP from `principal_address`
- Canonicalizes ZIPs to **5‑digit padded** strings
- Validates against HUD ZIP–Tract table  
- Output → `output_step3/silver_project_with_zip.csv`

### **4. ZIP → Census Tract Mapping**
- Uses HUD crosswalk (RES_RATIO‑weighted)
- Adds FIPS fields: state, county, census tract
- Output → `output_step4/silver_location_enriched.csv`

### **5. CEJST / LIDAC Eligibility**
- Loads CEJST v2.0 dataset  
- Joins tract → CEJST indicators  
- Flags: **Eligible / Partial / Not Eligible**  
- Output → `output_step5/gold_lidac_classified.csv`

### **6. Journal Entry Mapping (Intacct)**
- Maps premiums, commissions, penal amounts  
- Generates accounting-ready JE lines  
- Output → `output_step6/gold_journal_entries_for_intacct.csv`

### **7. ZIP‑Level Accumulation**
- Aggregates by ZIP: project count, premium, penal  
- Flags **GREEN / YELLOW / RED** accumulation zones  
- Output → `output_step7/gold_zip_accumulation_flags.csv`

### **8. Phase 1 Final Packaging**
Creates entire Phase 1 deliverable bundle:
- Intacct export  
- CEJST/LIDAC eligibility report  
- ZIP‑accumulation report  
- Exceptions summary  
Output directory → `/output_step8/`

---

## 📂 Folder Structure

```
greenie-re-data-pipeline/
│
├── raw/                        # Client input files
│   ├── C001_*.xlsx
│   ├── C002_*.xlsx
│   └── C003_*.xlsx
│
├── config/
│   ├── schema_registry.py
│   ├── generate_data.py
│   ├── download_hud_zip_tract_crosswalk.py
│   └── external_data/
│       ├── hud_zip_tract_crosswalk.csv
│       └── cejst_v2_communities.csv
│
├── doc/
│   ├── Phase1_Architecture.tex
│   ├── Phase1_Architecture.pdf
│   └── tikz/                   # Architecture diagrams
│
├── step1_intake_and_quality.py
├── step2_extraction_normalization.py
├── step3_zip_extraction.py
├── step4_zip_to_tract_mapping.py
├── step5_lidac_eligibility_cejst.py
├── step6_journal_entry_mapping.py
├── step7_zip_accumulation.py
└── step8_phase1_outputs.py
```

---

## 🛠 Installation & Dependencies

**Requirements**
- Python 3.10+
- pip or conda environment

Install dependencies:

```bash
pip install -r requirements.txt
```

Core libraries:
- `pandas`
- `numpy`
- `openpyxl`
- `requests`
- `python-dateutil`
- `uszipcode` (optional)

---

## ▶️ Running the Pipeline

Run each step sequentially:

```bash
python step1_intake_and_quality.py
python step2_extraction_normalization.py
python step3_zip_extraction.py
python step4_zip_to_tract_mapping.py
python step5_lidac_eligibility_cejst.py
python step6_journal_entry_mapping.py
python step7_zip_accumulation.py
python step8_phase1_outputs.py
```

---

## 📥 Inputs

Place client files in:

```
/raw/*.xlsx
```

Assumptions:
- Bordereaux follow the schema in `schema_registry.py`
- Each file represents a carrier‑submitted period dataset

---

## 📤 Outputs

Final deliverables (Phase 1):

```
/output_step8/
├── phase1_intacct_export.csv
├── phase1_lidac_report.csv
├── phase1_zip_accumulation.csv
└── phase1_exceptions_summary.csv
```

---

## 🌱 Why This Pipeline Matters

This system demonstrates:
- Fully automated ingestion → enrichment → underwriting pipeline  
- Deterministic geospatial mapping (ZIP → tract)
- CEJST/LIDAC regulatory classification  
- Intacct‑ready accounting integrations  
- Scalable ingestion for 100+ MGAs/carriers  
- Clear audit trails and repeatability  

---

## 🧭 Roadmap (Phase 2 & Phase 3)

**Phase 2**  
- Web portal UI (carrier/MGA uploads)  
- Automated scheduler (Airflow / Prefect)  
- S3/Blob storage  
- Full audit logs  
- API / webhook ingestion  

**Phase 3**  
- Production deployment  
- Authentication & RBAC  
- Analytics dashboards  
- Regulatory‑ready reporting  
- Underwriting & pricing engine integration  

---

## 👥 Authors

**Greenie Re**

**Co‑founders**
- *Stella Dong* — Applied Mathematics, ML Engineering, Reinsurance Data Systems  
- *James Finlay* — Wharton, Reinsurance Strategy, Risk Finance  

---
