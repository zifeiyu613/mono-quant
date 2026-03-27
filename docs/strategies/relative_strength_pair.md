# `relative_strength_pair` 策略说明

## 1. 策略定位

`relative_strength_pair` 是一个非常轻量的双资产切换策略。

它只回答一个问题：

**在这两个资产里，最近谁更强，就持有谁。**

相比多资产轮动，它更容易解释；相比单资产开关，它又保留了最基本的横向比较能力。

## 2. 信号规则

每到调仓日：

1. 计算 `benchmark_asset` 在 `lookback` 区间内的收益
2. 计算 `defensive_asset` 在同一窗口内的收益
3. 谁的收益更高，就持有谁
4. 每次只持有一个资产，权重为 `100%`

这里的 `defensive_asset` 不一定必须是“纯防守资产”，它也可以只是第二候选资产。

## 3. 关键参数

- `benchmark_asset`
- `defensive_asset`
- `lookback`
- `rebalance_freq`

这组参数决定的是：

- 比较的是哪两个资产
- 比较窗口有多长
- 切换频率有多快

## 4. 优点

- 逻辑非常容易讲清楚
- 比多资产排序更适合做运行层审核
- 很适合作为“单资产择时”和“多资产轮动”之间的中间层策略

## 5. 缺点

- 只有两个候选资产，表达能力有限
- 很依赖这对资产本身是否有清晰轮动关系
- 如果两者长期都弱，策略仍可能只能在“两个一般选项”里切换

## 6. 更适合的用途

- 低复杂度运行候选
- 与 `absolute_momentum_single` 对照“单资产开关”与“双资产相对强弱”的差别
- 与 `dual_momentum` 对照“轻量两选一”与“多资产筛选 + 防守回退”的差别

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/relative_strength_pair.json`
- `configs/daily_signal_relative_strength_pair.json`
