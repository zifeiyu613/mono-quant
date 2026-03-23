#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-scripts/fetch_config.json}"

echo "[STEP] Incremental fetch"
python scripts/fetch_tushare_etf_daily.py --config "$CONFIG"

echo "[STEP] Validate raw data"
python scripts/validate_etf_csv.py --dir data/raw

echo "[STEP] Build processed aligned layer"
python scripts/build_processed_etf_data.py --config "$CONFIG"

echo "[DONE] Raw + validation + processed build complete"
