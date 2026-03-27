# 策略说明总览

本目录按“策略用途 + 信号规则 + 适用场景”的方式整理项目中的每个策略。

如果你想先看“当前应该优先推进哪个策略”，建议先读：

- `docs/strategy-selection-guide.md`

## 策略清单

- `ma_single`：`docs/strategies/ma_single.md`
- `buy_hold_single`：`docs/strategies/buy_hold_single.md`
- `buy_hold_equal_weight`：`docs/strategies/buy_hold_equal_weight.md`
- `momentum_topn`：`docs/strategies/momentum_topn.md`
- `absolute_momentum_breadth`：`docs/strategies/absolute_momentum_breadth.md`
- `low_volatility_topn`：`docs/strategies/low_volatility_topn.md`
- `volatility_adjusted_momentum`：`docs/strategies/volatility_adjusted_momentum.md`
- `reversal_bottomn`：`docs/strategies/reversal_bottomn.md`
- `absolute_momentum_single`：`docs/strategies/absolute_momentum_single.md`
- `dual_momentum`：`docs/strategies/dual_momentum.md`
- `risk_off_rotation`：`docs/strategies/risk_off_rotation.md`
- `ma_timing_single`：`docs/strategies/ma_timing_single.md`
- `ma_rotation_topn`：`docs/strategies/ma_rotation_topn.md`
- `relative_strength_pair`：`docs/strategies/relative_strength_pair.md`
- `breakout_rotation_topn`：`docs/strategies/breakout_rotation_topn.md`
- `breakout_timing_single`：`docs/strategies/breakout_timing_single.md`

## 推荐阅读顺序

如果你刚接触这个项目，建议按下面顺序阅读：

1. `buy_hold_single`
2. `buy_hold_equal_weight`
3. `absolute_momentum_single`
4. `absolute_momentum_breadth`
5. `low_volatility_topn`
6. `ma_timing_single`
7. `ma_rotation_topn`
8. `breakout_timing_single`
9. `relative_strength_pair`
10. `momentum_topn`
11. `volatility_adjusted_momentum`
12. `breakout_rotation_topn`
13. `reversal_bottomn`
14. `dual_momentum`
15. `risk_off_rotation`
16. `ma_single`

## 如何使用这些文档

每份文档都尽量回答同一组问题：

- 这个策略到底在做什么
- 它的信号生成规则是什么
- 关键参数分别影响什么
- 优点、缺点和常见误用是什么
- 更适合拿来做 benchmark、研究候选，还是运行候选

这样做的目的是让后续新增策略时，也能保持统一的说明口径。

## 待实现候选

下面这些策略已经进入待实现池，但当前仓库里**还没有实现**：

- `defensive_pair_rotation`
  - 在两类防守资产之间择强，而不是只固定持有一个防守资产
- `adaptive_dual_momentum`
  - 根据广度或波动动态调整 `top_n` / `absolute_momentum_floor`
- `breakdown_timing_single`
  - 从“向上突破”扩到“向下跌破即退出”的镜像风险控制版本
- `volatility_target_rotation`
  - 在轮动基础上加入简单波动目标，让仓位强弱跟随波动环境变化

这些名字和定位已经先写进文档，后续继续扩策略时会优先从这里选。
