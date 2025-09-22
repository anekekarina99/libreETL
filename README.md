# 🛠️ Zero-Cost ETL Project Boilerplate = liberETL

A **lightweight, zero-cost ETL project template** for learning and collaboration.  
Built to help anyone practice ETL workflows using free tools, while maintaining  
**data governance, security, and collaboration standards** — without relying on CI/CD or expensive cloud platforms.

---

## 📂 Project Structure

```
my_zero_cost_etl_project/
├── data/
│   ├── raw/                   # Original raw files (do not modify)
│   │   ├── sales_20250922_v1.csv
│   │   └── external_api_20250922_v1.json
│   ├── processed/             # Cleaned & transformed datasets
│   │   └── sales_cleaned_20250922_v1.parquet
│   ├── database/              # Small databases (SQLite, etc.)
│   │   └── my_database.db
│   └── samples/               # Small datasets (≤1MB) for testing
│       └── sample_sales.csv
├── docs/
│   ├── README.md              # Project documentation
│   ├── data_dictionary.md     # Dataset field definitions
│   ├── data_standards.md      # Naming & formatting rules
│   └── governance.md          # Governance policies
├── logs/                      # ETL logs
├── scripts/                   # Python ETL scripts
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── run_etl_pipeline.py
├── .gitignore
└── requirements.txt
```

---

## 📋 Requirements

Dependencies are listed in `requirements.txt`:

- **pandas** → data manipulation  
- **requests** → API extraction  
- **sqlalchemy** → SQL operations  
- **pyarrow** → Parquet support  
- **openpyxl**, **xlrd** → Excel file support  
- **python-dotenv** → environment variable handling  
- **numpy**, **python-dateutil** → core dependencies  

Install with:

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

---

## 🔑 Data Governance

See [`docs/governance.md`](docs/governance.md) for full details.
Key rules:

* **Folder usage**

  * `data/raw` → raw/original files (immutable)
  * `data/processed` → cleaned outputs
  * `data/database` → SQLite or small databases
  * `data/samples` → ≤1MB datasets for quick testing

* **File naming convention**

  ```
  {datasetName}_{YYYYMMDD}_{v#}.{ext}
  ```

  Example: `sales_20250922_v1.csv` → `sales_20250922_v2.csv`

* **File size limits**

  * ≤50MB per file inside repo
  * Larger datasets → external storage (Google Drive, etc.)

* **Security**

  * Never commit `.env`, API keys, or sensitive data
  * `.gitignore` already excludes secrets and credentials

---

## 🤝 Collaboration

This repo is **open for direct commits** (no pull requests required).
Each contributor is responsible for following standards:

* Respect file naming rules (`docs/data_standards.md`)
* Keep raw data unchanged
* Place experiments under `scripts/experimental/`
* Update documentation when adding datasets or transformations

---

## 🚀 How to Use

1. Put your raw files into `data/raw/`
2. Write extraction code in `scripts/extract.py`
3. Apply transformations in `scripts/transform.py`
4. Load into SQLite (`data/database/`) via `scripts/load.py`
5. Run the pipeline with `scripts/run_etl_pipeline.py`

---

## 📖 Documentation

* **`README.md`** → project overview
* **`data_dictionary.md`** → field definitions for datasets
* **`data_standards.md`** → naming and formatting rules
* **`governance.md`** → governance and collaboration policies

---

## 🌱 Philosophy

This boilerplate is designed to be:

* **Zero-cost** → no paid tools required
* **Lightweight** → no CI/CD or complex setup
* **Collaborative** → open contributions, minimal barriers
* **Disciplined** → structure and governance instead of heavy monitoring

```
Simple rules > Expensive tools  
Shared responsibility > Strict gatekeeping
```

---

🔥 Ready to start building ETL pipelines without cloud overhead!