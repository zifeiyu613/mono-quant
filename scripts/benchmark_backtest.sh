#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="configs/momentum_topn.json"
RUNS=5
WARMUP=1
PROFILE="release"
SKIP_BUILD=0
OUTPUT_PATH=""

usage() {
  cat <<'EOF'
用法:
  scripts/benchmark_backtest.sh [选项]

选项:
  --config <path>     回测配置文件（默认: configs/momentum_topn.json）
  --runs <n>          正式计时轮数（默认: 5）
  --warmup <n>        预热轮数（默认: 1）
  --profile <name>    编译配置：release 或 debug（默认: release）
  --skip-build        跳过构建，直接运行已有二进制
  --output <path>     结果 JSON 输出路径（默认: output/benchmarks/ 自动生成）
  -h, --help          显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --runs)
      RUNS="${2:-}"
      shift 2
      ;;
    --warmup)
      WARMUP="${2:-}"
      shift 2
      ;;
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift 1
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "--runs 必须是正整数" >&2
  exit 1
fi
if ! [[ "$WARMUP" =~ ^[0-9]+$ ]]; then
  echo "--warmup 必须是非负整数" >&2
  exit 1
fi
if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
  echo "--profile 仅支持 release 或 debug" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "配置文件不存在: $CONFIG_PATH" >&2
  exit 1
fi

if [[ "$PROFILE" == "release" ]]; then
  BUILD_CMD=(cargo build --release -q)
  BIN_PATH="$REPO_ROOT/target/release/mono-quant"
else
  BUILD_CMD=(cargo build -q)
  BIN_PATH="$REPO_ROOT/target/debug/mono-quant"
fi
RUN_CMD=("$BIN_PATH" --config "$CONFIG_PATH")

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "[bench] 构建二进制 (${PROFILE})..."
  "${BUILD_CMD[@]}"
fi
if [[ ! -x "$BIN_PATH" ]]; then
  echo "找不到可执行文件: $BIN_PATH" >&2
  exit 1
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
config_base="$(basename "$CONFIG_PATH" .json)"
if [[ -z "$OUTPUT_PATH" ]]; then
  mkdir -p output/benchmarks
  OUTPUT_PATH="output/benchmarks/${config_base}_${PROFILE}_${timestamp}.json"
fi

now_ns() {
  python3 - <<'PY'
import time
print(time.perf_counter_ns())
PY
}

echo "[bench] 配置: $CONFIG_PATH"
echo "[bench] 命令: ${RUN_CMD[*]}"
echo "[bench] 预热: $WARMUP 次, 正式: $RUNS 次"

if [[ "$WARMUP" -gt 0 ]]; then
  for ((i=1; i<=WARMUP; i++)); do
    echo "[bench] warmup ${i}/${WARMUP}"
    "${RUN_CMD[@]}" >/dev/null 2>&1
  done
fi

durations_ms=()
for ((i=1; i<=RUNS; i++)); do
  start_ns="$(now_ns)"
  "${RUN_CMD[@]}" >/dev/null 2>&1
  end_ns="$(now_ns)"
  elapsed_ns=$((end_ns - start_ns))
  elapsed_ms="$(awk -v ns="$elapsed_ns" 'BEGIN {printf "%.3f", ns/1000000.0}')"
  durations_ms+=("$elapsed_ms")
  echo "[bench] run ${i}/${RUNS}: ${elapsed_ms} ms"
done

summary_json="$(
python3 - "${durations_ms[@]}" <<'PY'
import json
import math
import statistics
import sys

values = [float(v) for v in sys.argv[1:]]
values_sorted = sorted(values)

def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_values[int(pos)]
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac

summary = {
    "runs": len(values),
    "raw_ms": values,
    "min_ms": min(values),
    "max_ms": max(values),
    "mean_ms": statistics.mean(values),
    "median_ms": statistics.median(values),
    "p95_ms": percentile(values_sorted, 0.95),
    "stdev_ms": statistics.stdev(values) if len(values) > 1 else 0.0,
}
print(json.dumps(summary, ensure_ascii=False, indent=2))
PY
)"

mkdir -p "$(dirname "$OUTPUT_PATH")"
printf '%s\n' "$summary_json" > "$OUTPUT_PATH"

echo "[bench] 完成。结果已写入: $OUTPUT_PATH"
python3 - "$OUTPUT_PATH" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)

print(
    "[bench] 汇总: "
    f"mean={data['mean_ms']:.3f} ms, "
    f"median={data['median_ms']:.3f} ms, "
    f"p95={data['p95_ms']:.3f} ms, "
    f"min={data['min_ms']:.3f} ms, "
    f"max={data['max_ms']:.3f} ms"
)
PY
