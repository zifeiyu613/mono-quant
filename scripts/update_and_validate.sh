#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-scripts/fetch_config.json}"

echo "[STEP] Incremental fetch using ${CONFIG}"
python scripts/fetch_tushare_etf_daily.py --config "$CONFIG"

echo "[STEP] Validate local CSV files"
python scripts/validate_etf_csv.py --dir data/raw

echo "[DONE] Data update and validation complete"
