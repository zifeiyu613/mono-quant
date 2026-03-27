# `buy_hold_single` 策略说明

## 1. 策略定位

`buy_hold_single` 是单资产基准策略。它只在第一个可交易日建仓一次，然后长期持有指定基准 ETF。

它的主要作用是回答一个非常重要的问题：

**“复杂策略到底有没有显著优于最简单的长期持有？”**

## 2. 信号规则

- 初始建仓：买入 `benchmark_asset`
- 后续不主动调仓
- 仅在风控触发时可能被动切出

## 3. 关键参数

- `benchmark_asset`
  - 需要长期持有的基准 ETF
- `commission` / `slippage`
  - 仅影响初始建仓及风控切出时的成本

## 4. 优点

- 规则极简，解释成本最低
- 很适合做所有主动策略的底线 benchmark
- 能真实反映单一基准资产的长期贝塔暴露

## 5. 缺点

- 没有择时能力
- 没有跨资产切换能力
- 回撤通常高度依赖基准资产本身

## 6. 更适合的用途

- 作为对照组
- 用来判断主动轮动是否真的创造了超额收益或更优回撤

## 7. 输出文件

- `equity_curve.csv`
- `rebalance_log.csv`
- `holdings_trace.csv`
- `asset_contribution.csv`
- `diagnostics.txt`

## 8. 对应配置

- `configs/buy_hold_single.json`
