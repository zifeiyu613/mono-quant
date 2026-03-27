# `adaptive_dual_momentum` 策略说明

## 1. 策略定位

`adaptive_dual_momentum` 是 `dual_momentum` 的轻量自适应版本。

核心目标是：在市场广度走弱时，自动降低进攻强度；广度改善时，再恢复配置上限。

## 2. 信号规则

每到调仓日：

1. 对风险资产按 `lookback` 收益排序（相对动量）
2. 计算正收益广度（收益 >= 0 的资产占比）
3. 根据广度分层，动态调整：
   - `adaptive_top_n`
   - `adaptive_floor`
4. 用 `adaptive_floor` 过滤，再取前 `adaptive_top_n`
5. 若无资产满足，则回退 `defensive_asset` 或空仓

当前分层规则（内置）：

- 广度 `>= 2/3`：使用原始 `top_n` 和原始 `absolute_momentum_floor`
- 广度 `[1/3, 2/3)`：`top_n` 至多为 `2`，门槛至少为 `0`
- 广度 `< 1/3`：`top_n = 1`，门槛至少为 `2%`

## 3. 关键参数

- `lookback`
- `rebalance_freq`
- `top_n`
- `absolute_momentum_floor`
- `defensive_asset`

## 4. 优点

- 比固定参数的 `dual_momentum` 更稳健
- 在弱势阶段自动收敛仓位集中度
- 不引入复杂外部依赖，接入成本低

## 5. 缺点

- 内置分层规则是经验型，不是最优解
- 强趋势行情里可能略慢于激进版 `dual_momentum`
- 仍需后续批量研究验证参数稳定性

## 6. 更适合的用途

- `dual_momentum` 的运行层增强候选
- 与 `dual_momentum` 对照“固定参数 vs 自适应参数”
- 作为后续 `volatility_target_rotation` 的过渡版本

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/adaptive_dual_momentum.json`
- `configs/daily_signal_adaptive_dual_momentum.json`
