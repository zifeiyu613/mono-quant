#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-scripts/fetch_config.json}"

echo "[步骤] 执行增量拉取"
python scripts/fetch_tushare_etf_daily.py --config "$CONFIG"

echo "[步骤] 校验 raw 数据"
python scripts/validate_etf_csv.py --dir data/raw

echo "[步骤] 构建 processed 对齐层"
python scripts/build_processed_etf_data.py --config "$CONFIG"

echo "[完成] raw 拉取、校验与 processed 构建全部完成"
