# `buy_hold_equal_weight` 策略说明

## 1. 策略定位

`buy_hold_equal_weight` 是多资产等权基准策略。它在首个可交易日把资金平均分配给资产池中的所有 ETF，然后长期持有。

相比 `buy_hold_single`，它回答的是另一个基础问题：

**“如果什么主动判断都不做，只做最朴素的分散持有，结果会怎样？”**

## 2. 信号规则

- 初始建仓：对 `asset_files` 全部资产等权配置
- 后续不主动再平衡
- 仅在风控触发时可能被动切出

## 3. 关键参数

- `asset_files`
  - 参与等权配置的 ETF 池
- `commission` / `slippage`
  - 仅影响初始建仓及风控切出时的成本

## 4. 优点

- 提供天然的分散化对照
- 不依赖任何择时或排序逻辑
- 很适合用来检验“轮动策略是否真的优于简单分散”

## 5. 缺点

- 不会自动把资金集中到更强资产
- 也不会主动回避明显走弱资产
- 在风格切换较快时，可能显得“既不进攻也不防守”

## 6. 更适合的用途

- 多资产 baseline
- 对照 `momentum_topn`、`dual_momentum`、`risk_off_rotation`

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `diagnostics.txt`

## 8. 对应配置

- `configs/buy_hold_equal_weight.json`
