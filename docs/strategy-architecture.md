# 策略模块抽象说明

本文档描述当前策略模块的抽象边界，以及后续新增策略的最小接入步骤。

## 1. 抽象目标

当前抽象主要解决两个问题：

- 避免每新增一个策略都在 `main.rs` 复制整段数据加载、回测、输出逻辑
- 让策略本身只关注“参数定义 + 选资产规则 + 调用回测引擎”

## 2. 当前结构

### 策略规格层

文件：`src/strategy/runtime.rs`

- `RotationStrategySpec`：统一表达可运行的轮动策略规格
- `from_app_config`：从配置解析具体策略规格
- `required_lookback`：统一返回样本最小要求
- `detail_rows`：统一输出策略参数到诊断文件
- `required_assets`：声明策略硬依赖资产（例如基准资产）
- `run`：统一分派到具体回测函数

### 策略规则层

文件：`src/strategy/*.rs`

- `ma_cross.rs`：单资产均线信号规则
- `momentum_topn.rs`：TopN 动量排序规则
- `dual_momentum.rs`：双动量选资产规则（相对+绝对）

### 执行流程层

文件：`src/main.rs`

- `run_processed_rotation_strategy`：
  - 统一做 processed 输入校验
  - 统一做数据加载与对齐日期检查
  - 统一执行策略回测
  - 统一写出 `equity_curve/rebalance/holdings/contribution/risk/diagnostics`
- 主流程只负责：
  - 选择策略类型
  - 调用 `RotationStrategySpec::from_app_config`
  - 调用统一执行函数

### 回测引擎层

文件：`src/engine/backtest.rs`

- `run_rotation_backtest`：轮动类策略公共回测内核
- `run_momentum_topn_backtest` / `run_buy_hold_*` / `run_dual_momentum_backtest`：
  作为策略包装函数，复用统一内核

## 3. 新增一个策略的最小步骤

以新增轮动类策略为例：

1. 在 `src/strategy/` 新增规则文件（只写选资产逻辑）
2. 在 `src/engine/backtest.rs` 新增一个包装函数，复用 `run_rotation_backtest`
3. 在 `src/strategy/runtime.rs` 的 `RotationStrategySpec` 增加一个枚举分支
4. 在 `from_app_config` 中补参数解析
5. 在 `detail_rows` / `required_lookback` / `required_assets` / `run` 中补该分支
6. 新增对应 `configs/*.json`
7. 运行 `cargo test` + `cargo run -- --config <新配置>`

这套路径的特点是：

- 对主流程侵入小
- 输出格式天然一致
- 参数校验位置集中
- 后续扩展成本可控

## 4. 当前限制

当前抽象主要覆盖“轮动类策略”。

如果后续要接入完全不同的策略族（例如事件驱动、日内策略），建议新增并行规格类型，而不是把所有策略硬塞进 `RotationStrategySpec`。

