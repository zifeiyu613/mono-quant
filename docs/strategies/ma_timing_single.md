# `ma_timing_single` 策略说明

## 1. 策略定位

`ma_timing_single` 是面向 processed 数据层的单资产均线择时策略。

它和 `ma_single` 的区别在于：

- `ma_single` 走原始单资产 CSV 路径
- `ma_timing_single` 走 processed-first 路径，可以直接接入统一回测框架、统一对比输出和 `daily_signal`

## 2. 信号规则

每到调仓日：

1. 计算 `benchmark_asset` 的 `fast` 和 `slow` 简单均线
2. 若 `fast > slow`，持有 `benchmark_asset`
3. 否则持有 `defensive_asset`，如果未配置则空仓

## 3. 关键参数

- `benchmark_asset`
- `fast`
- `slow`
- `rebalance_freq`
- `defensive_asset`

建议：

- `fast < slow`
- `slow` 要明显长于 `fast`，否则趋势过滤意义会变弱

## 4. 优点

- 比简单买入持有更重视趋势状态
- 比多资产轮动更容易解释和执行
- 直接兼容 processed 层和运行层输出

## 5. 缺点

- 依然只观察一个风险资产
- 在震荡行情中容易被来回洗出
- 快慢线参数对结果影响较敏感

## 6. 更适合的用途

- 低复杂度运行候选
- 与 `absolute_momentum_single` 比较“收益阈值过滤 vs 均线趋势过滤”
- 与 `ma_single` 比较 raw / processed 两条执行路径

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/ma_timing_single.json`
- `configs/daily_signal_ma_timing_single.json`
