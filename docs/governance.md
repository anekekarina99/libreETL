# Data Governance (Zero-Cost ETL Project)

## 1. Folder Rules
- **data/raw**: only for raw (original) files, do not modify.
- **data/processed**: transformed data results.
- **data/database**: small database files (example: SQLite).
- **data/samples**: small datasets (≤ 1MB) for testing.

## 2. Naming Convention
Format: `{datasetName}_{YYYYMMDD}_{v#}.{ext}`
- Example: `sales_20250922_v1.csv`
- File updates → increment version (`v2`, `v3`), do not overwrite.

## 3. Size Limit
- Maximum file size in repo: **50MB**.
- Store large data in external storage (Google Drive, etc).

## 4. Security
- Do not commit credentials/API keys → use .env file (already in .gitignore).
- Do not upload sensitive data (PII, company secrets).

## 5. Collaboration
- No pull requests: everyone can commit & push.
- Experiments → save in `scripts/experimental/` to avoid disrupting main pipeline.
- Each contributor is responsible for their own work.

