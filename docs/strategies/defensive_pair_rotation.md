# `defensive_pair_rotation` 策略说明

## 1. 策略定位

`defensive_pair_rotation` 是“防守资产内部轮动”策略。

它不参与风险资产进攻，只在两类防守资产之间切换：

**回看期谁更强，就持有谁。**

## 2. 信号规则

每到调仓日：

1. 计算 `benchmark_asset` 在 `lookback` 窗口内收益
2. 计算 `defensive_asset` 在同窗口收益
3. 谁更高就持有谁
4. 每次只持有一个资产，权重 `100%`

## 3. 关键参数

- `benchmark_asset`（主防守资产）
- `defensive_asset`（次防守资产）
- `lookback`
- `rebalance_freq`

## 4. 优点

- 结构简单，运行口径稳定
- 可作为高波动阶段的防守层对照
- 适合和进攻型策略做“组合分层”研究

## 5. 缺点

- 长牛环境可能明显跑输进攻策略
- 防守资产选择不当时收益弹性不足
- 两资产范围有限，分散度不高

## 6. 更适合的用途

- 防守层策略对照
- 与 `relative_strength_pair` 对照“通用二选一”与“防守二选一”差异
- 与 `buy_hold_single` 对照“静态防守”与“动态防守”差异

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/defensive_pair_rotation.json`
- `configs/daily_signal_defensive_pair_rotation.json`
