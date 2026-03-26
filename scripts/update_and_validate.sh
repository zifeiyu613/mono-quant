#!/usr/bin/env bash
set -euo pipefail

CONFIG="${1:-scripts/fetch_config.json}"

echo "[步骤] 使用 ${CONFIG} 执行增量拉取"
python scripts/fetch_tushare_etf_daily.py --config "$CONFIG"

echo "[步骤] 校验本地 CSV 文件"
python scripts/validate_etf_csv.py --dir data/raw

echo "[完成] 数据更新与校验完成"
