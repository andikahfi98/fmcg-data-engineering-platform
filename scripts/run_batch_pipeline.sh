#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_DIR"

echo "============================================================"
echo "FMCG BATCH DATA PIPELINE"
echo "============================================================"

echo ""
echo "[1/3] Loading environment..."

set -a
source .env
set +a

echo "Environment loaded."

echo ""
echo "[2/3] Running Python ingestion..."

python -m src.ingestion.load_fact_sales
python -m src.ingestion.load_master_data

echo ""
echo "[3/3] Running dbt build..."

dbt build \
  --project-dir dbt \
  --profiles-dir dbt

echo ""
echo "============================================================"
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "============================================================"