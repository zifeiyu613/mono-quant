# 真实数据接入说明（Tushare ETF 日线）

当前项目已经支持：
- **真实 ETF 日线下载**
- **增量更新**
- **本地 CSV 校验**
- **processed 层构建（对齐、标准化、稳定回测输入）**
- **processed 层摘要输出（summary + manifest）**

---

## 一、为什么要引入 processed 层？

`raw/` 和 `processed/` 的职责应该分开：

### `data/raw/`
表示：
- 从 Tushare 拉下来的原始数据
- 尽量保留原始字段
- 用于留档与重新处理

### `data/processed/`
表示：
- 已经过统一清洗
- 已经过日期对齐
- 已经适合直接送给回测引擎

### 为什么这很重要？
因为如果你让回测直接吃 `raw/`，会越来越容易出问题：
- 日期不齐
- 字段不一致
- 不同资产长度不同
- 重复行 / 排序问题混进回测逻辑里

更合理的做法是：

**下载层负责拿数据，processed 层负责把数据整理成“稳定输入”。**

---

## 二、当前支持的真实数据流程

### 第一步：安装依赖
```bash
pip install -r scripts/requirements.txt
```

### 第二步：设置 Tushare Token
```bash
export TUSHARE_TOKEN=你的token
```

### 第三步：首次全量拉取
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json --full
```

### 第四步：之后增量更新
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json
```

### 第五步：校验 raw 层
```bash
python scripts/validate_etf_csv.py --dir data/raw
```

### 第六步：构建 processed 层
```bash
python scripts/build_processed_etf_data.py --config scripts/fetch_config.json
```

### 一条命令完成更新 + 校验 + processed 构建
```bash
./scripts/prepare_data.sh scripts/fetch_config.json
```

---

## 三、processed 层现在做了什么

当前 `scripts/build_processed_etf_data.py` 会做这些事：

1. 读取你配置中的多个 ETF 原始文件
2. 校验最基本字段是否存在（至少 `date/open/close`）
3. 按日期升序排序
4. 去掉重复日期
5. 计算所有资产的**共同日期交集**
6. 只保留共同日期
7. 按统一列顺序输出到 `data/processed/`
8. 输出 `alignment_manifest.json`
9. 输出 `processed_summary.json`
10. 输出 `processed_summary.txt`

---

## 四、processed 层输出什么

### 输出目录
```text
data/processed/
```

### 当前会生成
- `data/processed/hs300.csv`
- `data/processed/zz500.csv`
- `data/processed/cyb.csv`
- `data/processed/dividend.csv`
- `data/processed/alignment_manifest.json`
- `data/processed/processed_summary.json`
- `data/processed/processed_summary.txt`

### `alignment_manifest.json` 作用
它会记录：
- 一共对齐了多少资产
- 最终共同日期有多少行
- 对齐后的起止日期
- 每个输出文件路径和行数

### `processed_summary.json / txt` 作用
它会记录：
- 当前 processed 数据层的总览
- 每个资产 raw 行数 / processed 行数
- 每个资产丢掉了多少行
- raw 日期范围与 processed 日期范围
- 是否完全对齐

这非常适合做回测前核对。

---

## 五、Rust 现在如何使用 processed 摘要

多资产回测启动时，现在会先检查：
- `data/processed/*.csv`
- `alignment_manifest.json`
- `processed_summary.json`
- `processed_summary.txt`

然后会在日志里打印 processed 摘要前几行。

如果缺失，会提示你先运行：
```bash
./scripts/prepare_data.sh scripts/fetch_config.json
```

---

## 六、为什么 processed 层更适合回测

因为现在回测读到的数据会更稳定：
- 日期一致
- 行数一致
- 字段顺序统一
- 不需要每次在引擎里重新处理 raw 层脏活
- 启动前能看到本次使用的数据摘要

你可以把它理解成：

- `raw/` = 原始档案
- `processed/` = 可直接上引擎的标准化数据层
- `processed_summary.*` = 本次数据批次说明书

---

## 七、建议你接下来怎么用

### 单 ETF
单 ETF 仍然可以先直接吃 `raw/`，因为问题不大。

### 多 ETF / 动量轮动 / 批量实验
从现在开始，更推荐你逐渐切到吃：
```text
data/processed/*.csv
```

并且默认按下面流程：
```bash
./scripts/prepare_data.sh scripts/fetch_config.json
cargo run -- --config configs/momentum_topn.json
cargo run -- --config configs/momentum_batch.json
```

---

## 八、当前限制
目前 processed 层还是 v1.4 基础版，还没有做：
- 缺失日期填补策略
- 分钟级别处理
- 复权标准化
- 因子字段加工
- 异常样本统计
- 数据准备时间戳追踪

---

## 九、下一步最值得做
当 v1.4 跑稳后，下一步建议做：
1. 数据准备批次编号与时间戳
2. processed 层异常报告
3. Rust 输出中自动记录本次数据批次
4. 单资产策略也支持 processed-first 模式
