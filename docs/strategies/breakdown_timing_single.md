# `breakdown_timing_single` 策略说明

## 1. 策略定位

`breakdown_timing_single` 是单资产风险控制优先的择时策略。

它关注的不是“有没有向上突破”，而是：

**是否跌破过去一段时间的低点。**

当价格结构被破坏时，策略会先退出风险资产，转入防守资产或空仓。

## 2. 信号规则

每到调仓日：

1. 读取 `benchmark_asset` 当前收盘价
2. 找出过去 `lookback` 个交易日的最低收盘价
3. 若当前收盘价小于等于这段时间最低价，则退出到 `defensive_asset`（未配置则空仓）
4. 若未跌破，则继续持有 `benchmark_asset`

## 3. 关键参数

- `benchmark_asset`
- `lookback`
- `rebalance_freq`
- `defensive_asset`

其中：

- `lookback` 越短，退出更敏感
- `lookback` 越长，退出更保守

## 4. 优点

- 规则简单，风险控制口径直观
- 与“突破追涨”逻辑互补，适合成对对照
- 便于纳入运行层人工审核

## 5. 缺点

- 只覆盖单资产风险资产，表达力有限
- 震荡市可能出现频繁切换
- 依赖 `defensive_asset` 质量，防守资产过弱会拖累效果

## 6. 更适合的用途

- 运行层的“先避险”对照策略
- 与 `breakout_timing_single` 对照“追趋势”与“守底线”的差异
- 与 `absolute_momentum_single` 对照“结构止损”与“收益阈值”差异

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/breakdown_timing_single.json`
- `configs/daily_signal_breakdown_timing_single.json`
