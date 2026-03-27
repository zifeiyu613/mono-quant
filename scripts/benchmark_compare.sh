#!/usr/bin/env bash
set -euo pipefail

BASELINE_PATH=""
CANDIDATE_PATH=""
OUTPUT_PATH=""
FAIL_ON_ANY_REGRESS=0
MAX_MEAN_REGRESS_PCT=""
MAX_P95_REGRESS_PCT=""

usage() {
  cat <<'EOF'
用法:
  scripts/benchmark_compare.sh --baseline <path> --candidate <path> [选项]

选项:
  --baseline <path>   基线基准结果 JSON（优化前）
  --candidate <path>  候选基准结果 JSON（优化后）
  --output <path>     对比结果 JSON 输出路径（可选）
  --fail-on-regress   只要 mean/p95 任一变慢即返回非 0
  --max-mean-regress-pct <n>
                      mean_ms 允许最大变慢百分比（超过则返回非 0）
  --max-p95-regress-pct <n>
                      p95_ms 允许最大变慢百分比（超过则返回非 0）
  -h, --help          显示帮助
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline)
      BASELINE_PATH="${2:-}"
      shift 2
      ;;
    --candidate)
      CANDIDATE_PATH="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    --fail-on-regress)
      FAIL_ON_ANY_REGRESS=1
      shift 1
      ;;
    --max-mean-regress-pct)
      MAX_MEAN_REGRESS_PCT="${2:-}"
      shift 2
      ;;
    --max-p95-regress-pct)
      MAX_P95_REGRESS_PCT="${2:-}"
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

if [[ -z "$BASELINE_PATH" || -z "$CANDIDATE_PATH" ]]; then
  echo "必须同时提供 --baseline 与 --candidate" >&2
  usage
  exit 1
fi

if [[ ! -f "$BASELINE_PATH" ]]; then
  echo "baseline 文件不存在: $BASELINE_PATH" >&2
  exit 1
fi

if [[ ! -f "$CANDIDATE_PATH" ]]; then
  echo "candidate 文件不存在: $CANDIDATE_PATH" >&2
  exit 1
fi

if [[ -n "$MAX_MEAN_REGRESS_PCT" ]] && ! [[ "$MAX_MEAN_REGRESS_PCT" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--max-mean-regress-pct 必须是非负数字" >&2
  exit 1
fi
if [[ -n "$MAX_P95_REGRESS_PCT" ]] && ! [[ "$MAX_P95_REGRESS_PCT" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--max-p95-regress-pct 必须是非负数字" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPARE_JSON="$(
python3 - "$BASELINE_PATH" "$CANDIDATE_PATH" <<'PY'
import json
import sys

baseline_path = sys.argv[1]
candidate_path = sys.argv[2]

with open(baseline_path, "r", encoding="utf-8") as f:
    baseline = json.load(f)
with open(candidate_path, "r", encoding="utf-8") as f:
    candidate = json.load(f)

metrics = ["mean_ms", "median_ms", "p95_ms", "min_ms", "max_ms", "stdev_ms"]

comparison = {}
for key in metrics:
    old = float(baseline.get(key, 0.0))
    new = float(candidate.get(key, 0.0))
    delta = new - old
    pct = (delta / old * 100.0) if old != 0 else 0.0
    speedup = (old / new) if new != 0 else 0.0
    comparison[key] = {
        "baseline": old,
        "candidate": new,
        "delta_ms": delta,
        "delta_pct": pct,
        "speedup_x": speedup,
    }

result = {
    "baseline_path": baseline_path,
    "candidate_path": candidate_path,
    "baseline_runs": int(baseline.get("runs", 0)),
    "candidate_runs": int(candidate.get("runs", 0)),
    "comparison": comparison,
}
print(json.dumps(result, ensure_ascii=False, indent=2))
PY
)"

if [[ -n "$OUTPUT_PATH" ]]; then
  mkdir -p "$(dirname "$OUTPUT_PATH")"
  printf '%s\n' "$COMPARE_JSON" > "$OUTPUT_PATH"
  echo "[compare] 对比结果已写入: $OUTPUT_PATH"
fi

python3 - "$COMPARE_JSON" "$FAIL_ON_ANY_REGRESS" "$MAX_MEAN_REGRESS_PCT" "$MAX_P95_REGRESS_PCT" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
fail_on_any = bool(int(sys.argv[2]))
max_mean_regress_pct = float(sys.argv[3]) if sys.argv[3] else None
max_p95_regress_pct = float(sys.argv[4]) if sys.argv[4] else None
keys = ["mean_ms", "median_ms", "p95_ms", "min_ms", "max_ms", "stdev_ms"]

print("[compare] baseline:", data["baseline_path"])
print("[compare] candidate:", data["candidate_path"])
print(f"[compare] runs: baseline={data['baseline_runs']}, candidate={data['candidate_runs']}")

for key in keys:
    m = data["comparison"][key]
    print(
        f"[compare] {key}: "
        f"{m['baseline']:.3f} -> {m['candidate']:.3f} ms, "
        f"delta={m['delta_ms']:+.3f} ms ({m['delta_pct']:+.2f}%), "
        f"speedup={m['speedup_x']:.3f}x"
    )

violations = []
mean_delta_pct = data["comparison"]["mean_ms"]["delta_pct"]
p95_delta_pct = data["comparison"]["p95_ms"]["delta_pct"]

if fail_on_any and (mean_delta_pct > 0 or p95_delta_pct > 0):
    violations.append(
        f"启用 --fail-on-regress，且检测到回归: mean={mean_delta_pct:+.2f}%, p95={p95_delta_pct:+.2f}%"
    )
if max_mean_regress_pct is not None and mean_delta_pct > max_mean_regress_pct:
    violations.append(
        f"mean_ms 回归 {mean_delta_pct:+.2f}% 超过阈值 {max_mean_regress_pct:.2f}%"
    )
if max_p95_regress_pct is not None and p95_delta_pct > max_p95_regress_pct:
    violations.append(
        f"p95_ms 回归 {p95_delta_pct:+.2f}% 超过阈值 {max_p95_regress_pct:.2f}%"
    )

if violations:
    print("[compare] RESULT: FAIL")
    for item in violations:
        print(f"[compare] violation: {item}")
    sys.exit(1)

print("[compare] RESULT: PASS")
PY
