# `risk_off_rotation` 策略说明

## 1. 策略定位

`risk_off_rotation` 是项目里最贴近“进攻 / 防守切换”口径的多资产策略。

它先在风险资产池内部选最强者，再决定要不要切到防守资产。

## 2. 信号规则

每到调仓日：

1. 在 `risk_assets` 中找出 `lookback` 收益最高的资产
2. 检查该资产收益是否达到 `absolute_momentum_floor`
3. 若达标，则只持有这个最强风险资产
4. 若不达标，则切到 `defensive_asset`

## 3. 关键参数

- `risk_assets`
  - 风险资产池
- `defensive_asset`
  - 防守资产
- `lookback`
- `rebalance_freq`
- `absolute_momentum_floor`

## 4. 优点

- 非常直观，便于人工理解
- 始终只有一个进攻资产，执行层更简单
- 防守逻辑明确，适合 daily signal 场景

## 5. 缺点

- 组合集中度高
- 对单个最强资产判断错误时，波动会更明显
- 在多个资产都不错的阶段，无法分享分散收益

## 6. 更适合的用途

- 运行候选
- 与 `dual_momentum` 比较“集中持有 vs 分散持有”

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/risk_off_rotation.json`
- `configs/daily_signal_risk_off_rotation.json`
