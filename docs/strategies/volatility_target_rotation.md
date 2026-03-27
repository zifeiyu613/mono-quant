# `volatility_target_rotation` 策略说明

## 1. 策略定位

`volatility_target_rotation` 是在轮动框架里加入“简单波动目标控制”的策略。

它先做动量选资产，再根据当前组合波动是否超标，自动降低风险资产数量。

## 2. 信号规则

每到调仓日：

1. 对风险资产按 `lookback` 收益排序
2. 取前 `top_n` 作为风险资产候选
3. 估算候选组合近 `lookback` 日波动
4. 若波动不超过 `target_volatility`，保持风险候选
5. 若波动超标，按 `target_volatility / 当前波动` 缩减风险资产数量
6. 缩减后若配置了 `defensive_asset`，自动补入防守资产

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`
- `target_volatility`
- `defensive_asset`

## 4. 优点

- 在高波动阶段自动收敛风险暴露
- 比纯动量轮动更注重回撤控制
- 仍保持低复杂度，便于审计与落地

## 5. 缺点

- 属于离散近似（通过资产数量调节），不是连续仓位优化
- `target_volatility` 设得过低会长期偏防守
- 仍依赖防守资产质量

## 6. 更适合的用途

- 运行层风险更敏感的轮动候选
- 与 `adaptive_dual_momentum` 对照“广度驱动”与“波动驱动”差异
- 与 `dual_momentum` 对照“固定风险暴露”与“动态风险暴露”差异

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/volatility_target_rotation.json`
- `configs/daily_signal_volatility_target_rotation.json`
