# 真实数据接入说明（Tushare ETF 日线）

当前项目已经支持用 **Tushare 的 `fund_daily` 接口** 拉取 A股 ETF 日线数据，并补上了：
- **增量更新**
- **本地 CSV 校验**

## 为什么先用 Tushare
- 官方接口相对稳定
- 覆盖 ETF 日线足够入门
- 字段清晰：`trade_date, open, high, low, close, vol, amount`
- 适合先把“真实数据 -> Rust 回测”链路打通

## 官方接口信息
- 接口名：`fund_daily`
- 用途：ETF 日线行情
- 关键参数：`ts_code`, `start_date`, `end_date`
- 典型代码：`510300.SH`, `510500.SH`, `159915.SZ`, `510880.SH`

## 前提
你需要：
1. 一个 Tushare 账号
2. 一个可用的 Token
3. 满足 ETF 日线接口所需积分权限

## 安装 Python 依赖
```bash
pip install -r scripts/requirements.txt
```

## 设置 Token
```bash
export TUSHARE_TOKEN=你的token
```

## 全量下载
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json --full
```

## 增量更新
默认不加 `--full` 就是增量模式：
```bash
python scripts/fetch_tushare_etf_daily.py --config scripts/fetch_config.json
```

脚本会：
- 检查本地是否已有 CSV
- 如果有，读取最后一个 `date`
- 从 **下一天** 开始继续拉取
- 自动与本地数据合并
- 自动按 `date` 去重并升序排序

## 数据校验
### 校验整个目录
```bash
python scripts/validate_etf_csv.py --dir data/raw
```

### 校验单个文件
```bash
python scripts/validate_etf_csv.py --file data/raw/hs300.csv
```

## 一条命令更新 + 校验
```bash
./scripts/update_and_validate.sh scripts/fetch_config.json
```

## 校验内容
当前会检查：
- 必要字段是否存在：`date`, `open`, `close`
- `date` 是否是合法 `YYYY-MM-DD`
- 日期是否升序
- 日期是否重复
- 价格列是否为正数
- `high >= low`（如果字段存在）
- 必填字段是否为空

## 输出位置
脚本会把 CSV 写到：
```text
data/raw/
```

默认会生成：
- `data/raw/hs300.csv`
- `data/raw/zz500.csv`
- `data/raw/cyb.csv`
- `data/raw/dividend.csv`

## CSV 字段
脚本输出字段：
```text
date,open,high,low,close,vol,amount
```

当前 Rust 回测最核心依赖的是：
- `date`
- `open`
- `close`

## 接入当前 Rust 项目
### 单 ETF 版本
修改 `configs/ma_single.json` 中的 `data_file` 指向某个真实 CSV，例如：
```json
"data_file": "data/raw/hs300.csv"
```
然后执行：
```bash
cargo run -- --config configs/ma_single.json
```

### 多 ETF 动量轮动版本
```bash
cargo run -- --config configs/momentum_topn.json
```

### 批量实验版本
```bash
cargo run -- --config configs/momentum_batch.json
```

## 建议的数据工作流
```text
data/
├── raw/         # 从 Tushare 直接下载的原始 CSV
└── processed/   # 后续清洗、对齐、补字段后的数据
```

当前阶段你可以先只用 `raw/`。

## 当前限制
- 目前下载脚本使用 Python，不是 Rust 原生
- 目前只接了 ETF 日线，不包含分钟线
- 目前校验规则仍然偏基础
- 目前还没有自动增量更新日志落盘

## 下一步最值得做
当 v1.1 跑稳后，下一步建议做：
1. 数据校验结果输出到文件
2. 增量更新日志输出到 `output/data_updates/`
3. processed 层（对齐、裁剪、补字段）
4. Rust 原生数据下载器（如果你后面想完全 Rust 化）
