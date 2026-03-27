# `absolute_momentum_single` 策略说明

## 1. 策略定位

`absolute_momentum_single` 是一个非常适合实盘前评估的“单资产进攻 / 防守开关”策略。

它不在多个风险资产之间轮动，只回答一个问题：

**“当前基准资产本身是否值得持有？”**

如果答案是“值得”，持有基准资产；如果答案是“不值得”，就去防守资产或空仓。

## 2. 信号规则

每到调仓日：

1. 计算 `benchmark_asset` 在 `lookback` 周期内的收益
2. 若收益大于等于 `absolute_momentum_floor`，持有 `benchmark_asset`
3. 否则持有 `defensive_asset`，如果未配置则空仓

## 3. 关键参数

- `benchmark_asset`
  - 需要做开关判断的核心资产，例如 `hs300`
- `lookback`
  - 判断绝对动量的回看窗口
- `rebalance_freq`
  - 检查是否切换的频率
- `absolute_momentum_floor`
  - 最低收益门槛，常见是 `0.0`
- `defensive_asset`
  - 不满足门槛时的防守资产；不填则切到空仓

## 4. 优点

- 规则非常清晰，适合真实运行前的人工审核
- 相比多资产轮动，换手通常更低
- 对“市场整体是否值得冒险”有明确回答

## 5. 缺点

- 不会在多个风险资产之间做细分选择
- 如果门槛设置不合适，容易在边缘状态下频繁切换
- 收益上限通常不如更主动的轮动策略

## 6. 更适合的用途

- 实盘前的低复杂度运行候选
- 作为 `dual_momentum` 和 `risk_off_rotation` 的低复杂度对照
- 作为“进攻 / 防守切换”是否有效的基础验证

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `risk_events.csv`（如触发）
- `risk_summary.txt`
- `diagnostics.txt`

## 8. 对应配置

- `configs/absolute_momentum_single.json`
- `configs/daily_signal_absolute_momentum_single.json`
