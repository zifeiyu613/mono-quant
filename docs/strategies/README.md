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
- `adaptive_dual_momentum`：`docs/strategies/adaptive_dual_momentum.md`
- `volatility_target_rotation`：`docs/strategies/volatility_target_rotation.md`
- `risk_off_rotation`：`docs/strategies/risk_off_rotation.md`
- `ma_timing_single`：`docs/strategies/ma_timing_single.md`
- `ma_rotation_topn`：`docs/strategies/ma_rotation_topn.md`
- `relative_strength_pair`：`docs/strategies/relative_strength_pair.md`
- `defensive_pair_rotation`：`docs/strategies/defensive_pair_rotation.md`
- `breakout_rotation_topn`：`docs/strategies/breakout_rotation_topn.md`
- `breakout_timing_single`：`docs/strategies/breakout_timing_single.md`
- `breakdown_timing_single`：`docs/strategies/breakdown_timing_single.md`

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
9. `breakdown_timing_single`
10. `relative_strength_pair`
11. `defensive_pair_rotation`
12. `momentum_topn`
13. `volatility_adjusted_momentum`
14. `breakout_rotation_topn`
15. `reversal_bottomn`
16. `dual_momentum`
17. `adaptive_dual_momentum`
18. `volatility_target_rotation`
19. `risk_off_rotation`
20. `ma_single`

## 如何使用这些文档

每份文档都尽量回答同一组问题：

- 这个策略到底在做什么
- 它的信号生成规则是什么
- 关键参数分别影响什么
- 优点、缺点和常见误用是什么
- 更适合拿来做 benchmark、研究候选，还是运行候选

这样做的目的是让后续新增策略时，也能保持统一的说明口径。

## 待实现候选

当前待实现池已清空，后续如新增候选会在此处更新。
