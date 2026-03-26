mod config;
mod data;
mod engine;
mod metrics;
mod research;
mod report;
mod strategy;

use anyhow::{anyhow, Context};
use chrono::NaiveDate;
use config::load_config;
use report::{
    ensure_output_dir, write_contributions, write_diagnostics, write_equity_curve,
    write_csv_rows, write_experiment_index, write_holdings_trace, write_hypothesis_assessments,
    write_rebalance_log, EquityRow, ExperimentIndexRow,
};
use research::{
    apply_manual_override, assessments_to_rows, assess_hypotheses, build_evidence_summary,
    build_sample_split_plan, build_walk_forward_windows, cost_sensitivity_detail_rows,
    decide_research_state, render_governance_summary, render_research_decision,
    render_research_plan, render_walk_forward_plan, summarize_cost_sensitivity,
    summarize_walk_forward_assessments, walk_forward_detail_rows, BatchRowView,
};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize)]
struct BatchResultRow {
    experiment_id: String,
    lookback: usize,
    rebalance_freq: usize,
    top_n: usize,
    unit_cost: f64,
    total_return: f64,
    max_drawdown: f64,
    trade_count: usize,
    total_cost_paid: f64,
    final_equity: f64,
    top_contributor: String,
    worst_contributor: String,
    output_dir: String,
}

fn write_batch_results_csv(path: &str, rows: &[BatchResultRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

fn to_batch_row_views(rows: &[BatchResultRow]) -> Vec<BatchRowView> {
    rows.iter()
        .map(|row| BatchRowView {
            lookback: row.lookback,
            rebalance_freq: row.rebalance_freq,
            top_n: row.top_n,
            unit_cost: row.unit_cost,
            total_return: row.total_return,
            max_drawdown: row.max_drawdown,
            total_cost_paid: row.total_cost_paid,
        })
        .collect()
}

fn push_batch_result_row(
    rows: &mut Vec<BatchResultRow>,
    exp_id: &str,
    exp_dir: &str,
    lookback: usize,
    rebalance_freq: usize,
    top_n: usize,
    unit_cost: f64,
    result: &engine::backtest::MomentumTopNResult,
) {
    let top_contributor = result
        .top_contributor
        .clone()
        .map(|x| x.0)
        .unwrap_or_default();
    let worst_contributor = result
        .worst_contributor
        .clone()
        .map(|x| x.0)
        .unwrap_or_default();

    rows.push(BatchResultRow {
        experiment_id: exp_id.to_string(),
        lookback,
        rebalance_freq,
        top_n,
        unit_cost,
        total_return: result.summary.total_return,
        max_drawdown: result.summary.max_drawdown,
        trade_count: result.summary.trade_count,
        total_cost_paid: result.summary.total_cost_paid,
        final_equity: result.summary.final_equity,
        top_contributor,
        worst_contributor,
        output_dir: exp_dir.to_string(),
    });
}

/// 为较长的研究流程打印统一格式的信息日志。
fn log_info(message: &str) {
    println!("[信息] {}", message);
}

fn validate_risk_config(risk: Option<&config::RiskConfig>) -> anyhow::Result<()> {
    if let Some(risk_cfg) = risk {
        if let Some(limit) = risk_cfg.max_single_asset_weight {
            if !(0.0..=1.0).contains(&limit) || limit == 0.0 {
                return Err(anyhow!("risk.max_single_asset_weight 必须介于 0 和 1 之间"));
            }
        }
        if let Some(limit) = risk_cfg.max_drawdown_limit {
            if !(0.0..=1.0).contains(&limit) || limit == 0.0 {
                return Err(anyhow!("risk.max_drawdown_limit 必须介于 0 和 1 之间"));
            }
        }
        if let Some(limit) = risk_cfg.max_rebalance_turnover {
            if !(0.0..=1.0).contains(&limit) {
                return Err(anyhow!("risk.max_rebalance_turnover 必须介于 0 和 1 之间"));
            }
        }
    }
    Ok(())
}

/// 解析 `--config` 命令行参数，并返回 JSON 配置路径。
fn parse_config_path() -> anyhow::Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 || args[1] != "--config" {
        return Err(anyhow!("用法：cargo run -- --config <json配置路径>"));
    }
    Ok(args[2].clone())
}

/// 根据第一个 processed 资产文件路径推导对齐清单文件路径。
fn infer_manifest_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("alignment_manifest.json"))
    })
}

/// 根据第一个 processed 资产文件路径推导 processed 摘要 JSON 路径。
fn infer_summary_json_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("processed_summary.json"))
    })
}

/// 根据第一个 processed 资产文件路径推导 processed 摘要 TXT 路径。
fn infer_summary_txt_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("processed_summary.txt"))
    })
}

/// 检查多资产输入是否全部来自 processed 数据层。
fn all_assets_use_processed_layer(asset_files: &HashMap<String, String>) -> bool {
    asset_files
        .values()
        .all(|path| path.contains("data/processed/"))
}

/// 在运行多资产研究前，校验 processed 数据层文件是否齐全。
fn validate_processed_inputs(asset_files: &HashMap<String, String>) -> anyhow::Result<()> {
    if !all_assets_use_processed_layer(asset_files) {
        return Err(anyhow!(
            "多资产策略现在要求输入来自 data/processed/ 下的 processed 数据层，请先更新配置中的 asset_files。"
        ));
    }

    for (name, path) in asset_files {
        if !Path::new(path).exists() {
            return Err(anyhow!(
                "缺少 {} 的 processed 资产文件：{}\n请先运行：./scripts/prepare_data.sh scripts/fetch_config.json",
                name,
                path
            ));
        }
    }

    let manifest_path = infer_manifest_path(asset_files)
        .ok_or_else(|| anyhow!("无法从 asset_files 推导 processed 对齐清单路径"))?;
    if !manifest_path.exists() {
        return Err(anyhow!(
            "缺少 processed 对齐清单文件：{}\n请先运行：./scripts/prepare_data.sh scripts/fetch_config.json",
            manifest_path.display()
        ));
    }

    let summary_json_path = infer_summary_json_path(asset_files)
        .ok_or_else(|| anyhow!("无法从 asset_files 推导 processed 摘要 JSON 路径"))?;
    if !summary_json_path.exists() {
        return Err(anyhow!(
            "缺少 processed 摘要 JSON 文件：{}\n请先运行：./scripts/prepare_data.sh scripts/fetch_config.json",
            summary_json_path.display()
        ));
    }

    let summary_txt_path = infer_summary_txt_path(asset_files)
        .ok_or_else(|| anyhow!("无法从 asset_files 推导 processed 摘要 TXT 路径"))?;
    if !summary_txt_path.exists() {
        return Err(anyhow!(
            "缺少 processed 摘要 TXT 文件：{}\n请先运行：./scripts/prepare_data.sh scripts/fetch_config.json",
            summary_txt_path.display()
        ));
    }

    Ok(())
}

/// 在多资产运行前打印一小段 processed 数据层摘要。
fn log_processed_summary(asset_files: &HashMap<String, String>) -> anyhow::Result<()> {
    let summary_txt_path = infer_summary_txt_path(asset_files)
        .ok_or_else(|| anyhow!("无法从 asset_files 推导 processed 摘要 TXT 路径"))?;
    let content = fs::read_to_string(&summary_txt_path)
        .with_context(|| format!("读取 processed 摘要失败：{}", summary_txt_path.display()))?;

    println!("[信息] processed 数据摘要：{}", summary_txt_path.display());
    for line in content.lines().take(12) {
        println!("[信息] {}", line);
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path()?;
    log_info(&format!("正在加载配置：{}", config_path));
    let cfg = load_config(&config_path)?;

    println!("实验名称：{}", cfg.experiment_name);
    println!("策略类型：{}", cfg.strategy);

    match cfg.strategy.as_str() {
        "ma_single" => {
            ensure_output_dir(&cfg.output_dir)?;
            let data_file = cfg
                .data_file
                .clone()
                .ok_or_else(|| anyhow!("ma_single 需要提供 data_file"))?;
            let fast = cfg.fast.ok_or_else(|| anyhow!("ma_single 需要提供 fast"))?;
            let slow = cfg.slow.ok_or_else(|| anyhow!("ma_single 需要提供 slow"))?;
            let commission = cfg
                .commission
                .ok_or_else(|| anyhow!("ma_single 需要提供 commission"))?;
            let slippage = cfg
                .slippage
                .ok_or_else(|| anyhow!("ma_single 需要提供 slippage"))?;
            let stamp_tax_sell = cfg.stamp_tax_sell.unwrap_or(0.0);

            log_info(&format!("正在读取单资产数据：{}", data_file));
            let bars = data::read_bars(&data_file)?;
            if bars.len() <= slow {
                return Err(anyhow!(
                    "K 线数量不足：当前 {} 行，至少需要大于 slow 窗口 {}",
                    bars.len(),
                    slow
                ));
            }

            println!(
                "数据区间：{} -> {}（共 {} 根K线）",
                bars.first().unwrap().date,
                bars.last().unwrap().date,
                bars.len()
            );
            let signals = strategy::ma_cross::generate_signals(&bars, fast, slow);
            let (summary, curve) =
                engine::backtest::run_ma_backtest(&bars, &signals, commission, slippage, stamp_tax_sell);

            let equity_rows: Vec<EquityRow> = bars
                .iter()
                .zip(curve.iter())
                .map(|(bar, equity)| EquityRow {
                    date: bar.date.to_string(),
                    equity: *equity,
                })
                .collect();
            let equity_path = format!("{}/equity_curve.csv", cfg.output_dir);
            write_equity_curve(&equity_path, &equity_rows)?;

            let diagnostics = format!(
                "=== 诊断信息 ===\n实验名称: {}\n策略类型: {}\n数据文件: {}\nfast: {}\nslow: {}\n手续费: {}\n滑点: {}\n卖出印花税: {}\nK线数量: {}\n开始日期: {}\n结束日期: {}\n总收益: {:.2}%\n最大回撤: {:.2}%\n交易次数: {}\n总成本: {:.4}\n期末净值: {:.4}\n",
                cfg.experiment_name,
                cfg.strategy,
                data_file,
                fast,
                slow,
                commission,
                slippage,
                stamp_tax_sell,
                bars.len(),
                bars.first().unwrap().date,
                bars.last().unwrap().date,
                summary.total_return * 100.0,
                summary.max_drawdown * 100.0,
                summary.trade_count,
                summary.total_cost_paid,
                summary.final_equity
            );
            write_diagnostics(&format!("{}/diagnostics.txt", cfg.output_dir), &diagnostics)?;

            println!("=== 回测摘要 ===");
            println!("总收益：{:.2}%", summary.total_return * 100.0);
            println!("最大回撤：{:.2}%", summary.max_drawdown * 100.0);
            println!("交易次数：{}", summary.trade_count);
            println!("总成本：{:.4}", summary.total_cost_paid);
            println!("期末净值：{:.4}", summary.final_equity);
        }
        "momentum_topn" => {
            ensure_output_dir(&cfg.output_dir)?;
            validate_risk_config(cfg.risk.as_ref())?;
            let asset_files = cfg
                .asset_files
                .clone()
                .ok_or_else(|| anyhow!("momentum_topn 需要提供 asset_files"))?;
            let lookback = cfg
                .lookback
                .ok_or_else(|| anyhow!("momentum_topn 需要提供 lookback"))?;
            let rebalance_freq = cfg
                .rebalance_freq
                .ok_or_else(|| anyhow!("momentum_topn 需要提供 rebalance_freq"))?;
            let top_n = cfg.top_n.ok_or_else(|| anyhow!("momentum_topn 需要提供 top_n"))?;
            let commission = cfg
                .commission
                .ok_or_else(|| anyhow!("momentum_topn 需要提供 commission"))?;
            let slippage = cfg
                .slippage
                .ok_or_else(|| anyhow!("momentum_topn 需要提供 slippage"))?;

            log_info("正在校验 momentum_topn 的 processed 输入");
            validate_processed_inputs(&asset_files)?;
            if let Some(manifest_path) = infer_manifest_path(&asset_files) {
                log_info(&format!("使用 processed 对齐清单：{}", manifest_path.display()));
            }
            if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
                log_info(&format!("使用 processed 摘要 JSON：{}", summary_json_path.display()));
            }
            log_processed_summary(&asset_files)?;

            log_info("正在加载 momentum_topn 的多资产数据");
            let mut asset_maps = HashMap::new();
            for (name, path) in &asset_files {
                log_info(&format!("正在加载资产 {}：{}", name, path));
                asset_maps.insert(
                    name.clone(),
                    data::read_bars_map(path)
                        .with_context(|| format!("读取资产 {} 失败：{}", name, path))?,
                );
            }

            let dates = data::intersect_dates(&asset_maps);
            if dates.len() <= lookback + 1 {
                return Err(anyhow!(
                    "momentum_topn 的对齐交易日不足：当前对齐后仅 {} 个交易日",
                    dates.len()
                ));
            }
            if let Some(min_days) = cfg.risk.as_ref().and_then(|risk| risk.min_aligned_days) {
                if dates.len() < min_days {
                    return Err(anyhow!(
                        "momentum_topn 的对齐交易日不足：当前 {}，低于风控要求的最小样本 {}",
                        dates.len(),
                        min_days
                    ));
                }
            }

            println!(
                "对齐区间：{} -> {}（共 {} 个对齐交易日）",
                dates.first().unwrap(),
                dates.last().unwrap(),
                dates.len()
            );
            log_info("正在运行 momentum_topn 回测");
            let result = engine::backtest::run_momentum_topn_backtest(
                &asset_maps,
                lookback,
                rebalance_freq,
                top_n,
                commission,
                slippage,
                cfg.risk.as_ref(),
            );

            let equity_rows: Vec<EquityRow> = result
                .equity_curve
                .iter()
                .map(|(d, e)| EquityRow {
                    date: d.to_string(),
                    equity: *e,
                })
                .collect();
            write_equity_curve(&format!("{}/equity_curve.csv", cfg.output_dir), &equity_rows)?;
            write_rebalance_log(&format!("{}/rebalance_log.csv", cfg.output_dir), &result.rebalances)?;
            write_holdings_trace(&format!("{}/holdings_trace.csv", cfg.output_dir), &result.holdings_trace)?;
            write_contributions(
                &format!("{}/asset_contribution.csv", cfg.output_dir),
                &result.contributions,
            )?;
            if !result.risk_events.is_empty() {
                write_csv_rows(&format!("{}/risk_events.csv", cfg.output_dir), &result.risk_events)?;
            }

            let manifest_path = infer_manifest_path(&asset_files).unwrap();
            let summary_json_path = infer_summary_json_path(&asset_files).unwrap();
            let summary_txt_path = infer_summary_txt_path(&asset_files).unwrap();
            let diagnostics = format!(
                "=== 诊断信息 ===\n实验名称: {}\n策略类型: {}\n数据层: processed\nprocessed 清单: {}\nprocessed 摘要 JSON: {}\nprocessed 摘要 TXT: {}\n资产列表: {}\nlookback: {}\nrebalance_freq: {}\ntop_n: {}\n手续费: {}\n滑点: {}\n对齐交易日数量: {}\n开始日期: {}\n结束日期: {}\n总收益: {:.2}%\n最大回撤: {:.2}%\n调仓次数: {}\n总成本: {:.6}\n期末净值: {:.4}\n是否触发风控停机: {}\n风控停机原因: {}\n贡献最高资产: {:?}\n贡献最低资产: {:?}\n输出文件:\n- equity_curve.csv\n- rebalance_log.csv\n- holdings_trace.csv\n- asset_contribution.csv\n- risk_events.csv（如触发风控）\n",
                cfg.experiment_name,
                cfg.strategy,
                manifest_path.display(),
                summary_json_path.display(),
                summary_txt_path.display(),
                asset_files.keys().cloned().collect::<Vec<_>>().join(","),
                lookback,
                rebalance_freq,
                top_n,
                commission,
                slippage,
                dates.len(),
                dates.first().unwrap(),
                dates.last().unwrap(),
                result.summary.total_return * 100.0,
                result.summary.max_drawdown * 100.0,
                result.summary.trade_count,
                result.summary.total_cost_paid,
                result.summary.final_equity,
                result.summary.halted_by_risk,
                result.summary
                    .halt_reason
                    .clone()
                    .unwrap_or_else(|| "未触发".to_string()),
                result.top_contributor,
                result.worst_contributor,
            );
            write_diagnostics(&format!("{}/diagnostics.txt", cfg.output_dir), &diagnostics)?;

            println!("=== 动量轮动摘要 ===");
            println!("总收益：{:.2}%", result.summary.total_return * 100.0);
            println!("最大回撤：{:.2}%", result.summary.max_drawdown * 100.0);
            println!("调仓次数：{}", result.summary.trade_count);
            println!("总成本：{:.6}", result.summary.total_cost_paid);
            println!("期末净值：{:.4}", result.summary.final_equity);
            println!("是否触发风控停机：{}", result.summary.halted_by_risk);
            println!("贡献最高资产：{:?}", result.top_contributor);
            println!("贡献最低资产：{:?}", result.worst_contributor);
        }
        "momentum_batch" => {
            validate_risk_config(cfg.risk.as_ref())?;
            let asset_files = cfg
                .asset_files
                .clone()
                .ok_or_else(|| anyhow!("momentum_batch 需要提供 asset_files"))?;
            let lookbacks = cfg
                .lookbacks
                .clone()
                .ok_or_else(|| anyhow!("momentum_batch 需要提供 lookbacks"))?;
            let rebalance_freqs = cfg
                .rebalance_freqs
                .clone()
                .ok_or_else(|| anyhow!("momentum_batch 需要提供 rebalance_freqs"))?;
            let top_ns = cfg.top_ns.clone().ok_or_else(|| anyhow!("momentum_batch 需要提供 top_ns"))?;
            let unit_costs = cfg
                .unit_costs
                .clone()
                .ok_or_else(|| anyhow!("momentum_batch 需要提供 unit_costs"))?;

            log_info("正在校验 momentum_batch 的 processed 输入");
            validate_processed_inputs(&asset_files)?;
            if let Some(manifest_path) = infer_manifest_path(&asset_files) {
                log_info(&format!("使用 processed 对齐清单：{}", manifest_path.display()));
            }
            if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
                log_info(&format!("使用 processed 摘要 JSON：{}", summary_json_path.display()));
            }
            log_processed_summary(&asset_files)?;

            ensure_output_dir(&cfg.output_dir)?;
            fs::create_dir_all(format!("{}/experiments", cfg.output_dir))?;
            fs::copy(&config_path, format!("{}/config_snapshot.json", cfg.output_dir))?;
            log_info("正在加载批量实验所需的多资产数据");

            let mut asset_maps = HashMap::new();
            for (name, path) in &asset_files {
                log_info(&format!("正在加载资产 {}：{}", name, path));
                asset_maps.insert(
                    name.clone(),
                    data::read_bars_map(path)
                        .with_context(|| format!("读取资产 {} 失败：{}", name, path))?,
                );
            }
            let aligned_dates = data::intersect_dates(&asset_maps);
            if let Some(min_days) = cfg.risk.as_ref().and_then(|risk| risk.min_aligned_days) {
                if aligned_dates.len() < min_days {
                    return Err(anyhow!(
                        "momentum_batch 的对齐交易日不足：当前 {}，低于风控要求的最小样本 {}",
                        aligned_dates.len(),
                        min_days
                    ));
                }
            }
            let sample_split_plan = cfg
                .research
                .as_ref()
                .and_then(|research_cfg| research_cfg.sample_split.as_ref())
                .map(|split_cfg| build_sample_split_plan(split_cfg, &aligned_dates))
                .transpose()?;
            let walk_forward_windows = cfg
                .research
                .as_ref()
                .and_then(|research_cfg| research_cfg.walk_forward.as_ref())
                .map(|walk_cfg| build_walk_forward_windows(walk_cfg, &aligned_dates))
                .transpose()?;
            let in_sample_asset_maps = sample_split_plan.as_ref().map(|plan| {
                data::filter_asset_maps_by_date_range(&asset_maps, plan.in_sample_start, plan.in_sample_end)
            });
            let out_sample_asset_maps = sample_split_plan.as_ref().map(|plan| {
                data::filter_asset_maps_by_date_range(
                    &asset_maps,
                    plan.out_sample_start,
                    plan.out_sample_end,
                )
            });

            let mut rows: Vec<BatchResultRow> = Vec::new();
            let mut in_sample_rows: Vec<BatchResultRow> = Vec::new();
            let mut out_sample_rows: Vec<BatchResultRow> = Vec::new();
            let mut walk_forward_rows: Vec<Vec<BatchResultRow>> = walk_forward_windows
                .as_ref()
                .map(|windows| (0..windows.len()).map(|_| Vec::new()).collect())
                .unwrap_or_default();
            let mut index_rows: Vec<ExperimentIndexRow> = Vec::new();
            let mut exp_num = 1usize;

            for lookback in lookbacks {
                for rebalance_freq in &rebalance_freqs {
                    for top_n in &top_ns {
                        for unit_cost in &unit_costs {
                            let exp_id = format!("exp_{:03}", exp_num);
                            let exp_dir = format!("{}/experiments/{}", cfg.output_dir, exp_id);
                            fs::create_dir_all(&exp_dir)?;
                            log_info(&format!(
                                "正在运行 {}：lookback={}, rebalance_freq={}, top_n={}, unit_cost={}",
                                exp_id, lookback, rebalance_freq, top_n, unit_cost
                            ));

                            let result = engine::backtest::run_momentum_topn_backtest(
                                &asset_maps,
                                lookback,
                                *rebalance_freq,
                                *top_n,
                                unit_cost / 2.0,
                                unit_cost / 2.0,
                                cfg.risk.as_ref(),
                            );

                            let equity_rows: Vec<EquityRow> = result
                                .equity_curve
                                .iter()
                                .map(|(d, e)| EquityRow {
                                    date: d.to_string(),
                                    equity: *e,
                                })
                                .collect();
                            write_equity_curve(&format!("{}/equity_curve.csv", exp_dir), &equity_rows)?;
                            write_rebalance_log(&format!("{}/rebalance_log.csv", exp_dir), &result.rebalances)?;
                            write_holdings_trace(&format!("{}/holdings_trace.csv", exp_dir), &result.holdings_trace)?;
                            write_contributions(
                                &format!("{}/asset_contribution.csv", exp_dir),
                                &result.contributions,
                            )?;
                            if !result.risk_events.is_empty() {
                                write_csv_rows(&format!("{}/risk_events.csv", exp_dir), &result.risk_events)?;
                            }
                            let top_contributor = result
                                .top_contributor
                                .clone()
                                .map(|x| x.0)
                                .unwrap_or_default();
                            let worst_contributor = result
                                .worst_contributor
                                .clone()
                                .map(|x| x.0)
                                .unwrap_or_default();

                            let diag = format!(
                                "实验ID: {}\n数据层: processed\nlookback: {}\nrebalance_freq: {}\ntop_n: {}\n单位成本: {}\n总收益: {:.2}%\n最大回撤: {:.2}%\n交易次数: {}\n总成本: {:.6}\n期末净值: {:.4}\n是否触发风控停机: {}\n风控停机原因: {}\n贡献最高资产: {:?}\n贡献最低资产: {:?}\n",
                                exp_id,
                                lookback,
                                rebalance_freq,
                                top_n,
                                unit_cost,
                                result.summary.total_return * 100.0,
                                result.summary.max_drawdown * 100.0,
                                result.summary.trade_count,
                                result.summary.total_cost_paid,
                                result.summary.final_equity,
                                result.summary.halted_by_risk,
                                result.summary
                                    .halt_reason
                                    .clone()
                                    .unwrap_or_else(|| "未触发".to_string()),
                                result.top_contributor,
                                result.worst_contributor,
                            );
                            write_diagnostics(&format!("{}/diagnostics.txt", exp_dir), &diag)?;

                            push_batch_result_row(
                                &mut rows,
                                &exp_id,
                                &exp_dir,
                                lookback,
                                *rebalance_freq,
                                *top_n,
                                *unit_cost,
                                &result,
                            );

                            if let Some(scope_asset_maps) = &in_sample_asset_maps {
                                let scope_dates: Vec<NaiveDate> = data::intersect_dates(scope_asset_maps);
                                if scope_dates.len() > lookback + 1 {
                                    let scoped_result = engine::backtest::run_momentum_topn_backtest(
                                        scope_asset_maps,
                                        lookback,
                                        *rebalance_freq,
                                        *top_n,
                                        unit_cost / 2.0,
                                        unit_cost / 2.0,
                                        cfg.risk.as_ref(),
                                    );
                                    push_batch_result_row(
                                        &mut in_sample_rows,
                                        &exp_id,
                                        &exp_dir,
                                        lookback,
                                        *rebalance_freq,
                                        *top_n,
                                        *unit_cost,
                                        &scoped_result,
                                    );
                                }
                            }

                            if let Some(scope_asset_maps) = &out_sample_asset_maps {
                                let scope_dates: Vec<NaiveDate> = data::intersect_dates(scope_asset_maps);
                                if scope_dates.len() > lookback + 1 {
                                    let scoped_result = engine::backtest::run_momentum_topn_backtest(
                                        scope_asset_maps,
                                        lookback,
                                        *rebalance_freq,
                                        *top_n,
                                        unit_cost / 2.0,
                                        unit_cost / 2.0,
                                        cfg.risk.as_ref(),
                                    );
                                    push_batch_result_row(
                                        &mut out_sample_rows,
                                        &exp_id,
                                        &exp_dir,
                                        lookback,
                                        *rebalance_freq,
                                        *top_n,
                                        *unit_cost,
                                        &scoped_result,
                                    );
                                }
                            }

                            if let Some(windows) = &walk_forward_windows {
                                for (window_index, window) in windows.iter().enumerate() {
                                    let scope_asset_maps = data::filter_asset_maps_by_date_range(
                                        &asset_maps,
                                        window.test_start,
                                        window.test_end,
                                    );
                                    let scope_dates = data::intersect_dates(&scope_asset_maps);
                                    if scope_dates.len() > lookback + 1 {
                                        let scoped_result = engine::backtest::run_momentum_topn_backtest(
                                            &scope_asset_maps,
                                            lookback,
                                            *rebalance_freq,
                                            *top_n,
                                            unit_cost / 2.0,
                                            unit_cost / 2.0,
                                            cfg.risk.as_ref(),
                                        );
                                        push_batch_result_row(
                                            &mut walk_forward_rows[window_index],
                                            &exp_id,
                                            &exp_dir,
                                            lookback,
                                            *rebalance_freq,
                                            *top_n,
                                            *unit_cost,
                                            &scoped_result,
                                        );
                                    }
                                }
                            }

                            index_rows.push(ExperimentIndexRow {
                                experiment_id: exp_id,
                                lookback,
                                rebalance_freq: *rebalance_freq,
                                top_n: *top_n,
                                unit_cost: *unit_cost,
                                total_return: result.summary.total_return,
                                max_drawdown: result.summary.max_drawdown,
                                trade_count: result.summary.trade_count,
                                total_cost_paid: result.summary.total_cost_paid,
                                final_equity: result.summary.final_equity,
                                top_contributor,
                                worst_contributor,
                                output_dir: exp_dir,
                            });

                            exp_num += 1;
                        }
                    }
                }
            }

            rows.sort_by(|a, b| b.total_return.partial_cmp(&a.total_return).unwrap());
            in_sample_rows.sort_by(|a, b| b.total_return.partial_cmp(&a.total_return).unwrap());
            out_sample_rows.sort_by(|a, b| b.total_return.partial_cmp(&a.total_return).unwrap());
            write_batch_results_csv(&format!("{}/batch_results.csv", cfg.output_dir), &rows)?;
            write_experiment_index(&format!("{}/experiment_index.csv", cfg.output_dir), &index_rows)?;
            if sample_split_plan.is_some() {
                write_batch_results_csv(
                    &format!("{}/batch_results_in_sample.csv", cfg.output_dir),
                    &in_sample_rows,
                )?;
                write_batch_results_csv(
                    &format!("{}/batch_results_out_of_sample.csv", cfg.output_dir),
                    &out_sample_rows,
                )?;
            }
            if let Some(windows) = &walk_forward_windows {
                for (index, rows) in walk_forward_rows.iter_mut().enumerate() {
                    rows.sort_by(|a, b| b.total_return.partial_cmp(&a.total_return).unwrap());
                    write_batch_results_csv(
                        &format!(
                            "{}/batch_results_walk_forward_window_{:02}.csv",
                            cfg.output_dir,
                            index + 1
                        ),
                        rows,
                    )?;
                }
                write_diagnostics(
                    &format!("{}/walk_forward_plan.txt", cfg.output_dir),
                    &render_walk_forward_plan(windows),
                )?;
            }

            let top_by_return: Vec<String> = rows
                .iter()
                .take(3)
                .map(|r| format!("{} ({:.2}%)", r.experiment_id, r.total_return * 100.0))
                .collect();
            let low_drawdown_candidate = rows
                .iter()
                .min_by(|a, b| a.max_drawdown.partial_cmp(&b.max_drawdown).unwrap())
                .map(|r| format!("{} ({:.2}%)", r.experiment_id, r.max_drawdown * 100.0))
                .unwrap_or_else(|| "N/A".to_string());
            let manifest_path = infer_manifest_path(&asset_files).unwrap();
            let summary_json_path = infer_summary_json_path(&asset_files).unwrap();
            let summary_txt_path = infer_summary_txt_path(&asset_files).unwrap();

            if let Some(research_cfg) = &cfg.research {
                let full_row_views = to_batch_row_views(&rows);
                let full_assessments = assess_hypotheses(research_cfg, &full_row_views);
                let full_assessment_rows = assessments_to_rows(&full_assessments);
                let in_sample_assessments = if sample_split_plan.is_some() {
                    Some(assess_hypotheses(research_cfg, &to_batch_row_views(&in_sample_rows)))
                } else {
                    None
                };
                let out_sample_assessments = if sample_split_plan.is_some() {
                    Some(assess_hypotheses(
                        research_cfg,
                        &to_batch_row_views(&out_sample_rows),
                    ))
                } else {
                    None
                };
                let walk_forward_assessments: Vec<Vec<_>> = if walk_forward_windows.is_some() {
                    walk_forward_rows
                        .iter()
                        .map(|rows| assess_hypotheses(research_cfg, &to_batch_row_views(rows)))
                        .collect()
                } else {
                    Vec::new()
                };
                let walk_forward_detail = if let Some(windows) = &walk_forward_windows {
                    walk_forward_detail_rows(windows, &walk_forward_assessments)
                } else {
                    Vec::new()
                };
                let walk_forward_summary =
                    summarize_walk_forward_assessments(research_cfg, &walk_forward_assessments);
                let cost_sensitivity_detail =
                    cost_sensitivity_detail_rows(research_cfg, &full_row_views);
                let cost_sensitivity_summary =
                    summarize_cost_sensitivity(research_cfg, &cost_sensitivity_detail);
                let evidence_summary = build_evidence_summary(
                    research_cfg,
                    &full_assessments,
                    in_sample_assessments.as_deref(),
                    out_sample_assessments.as_deref(),
                    &walk_forward_summary,
                    &cost_sensitivity_summary,
                    aligned_dates.first().copied(),
                    aligned_dates.last().copied(),
                );
                let auto_decision = decide_research_state(
                    research_cfg,
                    &full_assessments,
                    in_sample_assessments.as_deref(),
                    out_sample_assessments.as_deref(),
                );
                let final_decision = if let Some(override_cfg) = &research_cfg.decision_override {
                    apply_manual_override(&auto_decision, override_cfg)
                } else {
                    auto_decision.clone()
                };

                write_hypothesis_assessments(
                    &format!("{}/hypothesis_assessment.csv", cfg.output_dir),
                    &full_assessment_rows,
                )?;
                if let Some(assessments) = &in_sample_assessments {
                    write_hypothesis_assessments(
                        &format!("{}/hypothesis_assessment_in_sample.csv", cfg.output_dir),
                        &assessments_to_rows(assessments),
                    )?;
                }
                if let Some(assessments) = &out_sample_assessments {
                    write_hypothesis_assessments(
                        &format!("{}/hypothesis_assessment_out_of_sample.csv", cfg.output_dir),
                        &assessments_to_rows(assessments),
                    )?;
                }
                if !walk_forward_detail.is_empty() {
                    write_csv_rows(
                        &format!("{}/walk_forward_assessment_detail.csv", cfg.output_dir),
                        &walk_forward_detail,
                    )?;
                }
                if !walk_forward_summary.is_empty() {
                    write_csv_rows(
                        &format!("{}/walk_forward_assessment_summary.csv", cfg.output_dir),
                        &walk_forward_summary,
                    )?;
                }
                if !cost_sensitivity_detail.is_empty() {
                    write_csv_rows(
                        &format!("{}/cost_sensitivity_detail.csv", cfg.output_dir),
                        &cost_sensitivity_detail,
                    )?;
                }
                if !cost_sensitivity_summary.is_empty() {
                    write_csv_rows(
                        &format!("{}/cost_sensitivity_summary.csv", cfg.output_dir),
                        &cost_sensitivity_summary,
                    )?;
                }
                if !evidence_summary.is_empty() {
                    write_csv_rows(
                        &format!("{}/research_evidence_summary.csv", cfg.output_dir),
                        &evidence_summary,
                    )?;
                }
                write_diagnostics(
                    &format!("{}/research_plan.txt", cfg.output_dir),
                    &render_research_plan(research_cfg),
                )?;
                write_diagnostics(
                    &format!("{}/research_decision_auto.txt", cfg.output_dir),
                    &render_research_decision(
                        "自动研究决策",
                        &auto_decision,
                        &full_assessments,
                        in_sample_assessments.as_deref(),
                        out_sample_assessments.as_deref(),
                        &evidence_summary,
                    ),
                )?;
                write_diagnostics(
                    &format!("{}/research_decision.txt", cfg.output_dir),
                    &render_research_decision(
                        "最终研究决策",
                        &final_decision,
                        &full_assessments,
                        in_sample_assessments.as_deref(),
                        out_sample_assessments.as_deref(),
                        &evidence_summary,
                    ),
                )?;
                write_diagnostics(
                    &format!("{}/governance_summary.txt", cfg.output_dir),
                    &render_governance_summary(
                        sample_split_plan.as_ref(),
                        walk_forward_windows.as_deref(),
                        &auto_decision,
                        &final_decision,
                        &evidence_summary,
                    ),
                )?;
            }

            let summary = format!(
                "=== 批量实验摘要 ===\n实验名称: {}\n策略类型: {}\n数据层: processed\nprocessed 清单: {}\nprocessed 摘要 JSON: {}\nprocessed 摘要 TXT: {}\n实验数量: {}\n收益前三组合: {}\n最低回撤候选: {}\n配置快照: {}/config_snapshot.json\n结果总表: {}/batch_results.csv\n实验索引: {}/experiment_index.csv\n",
                cfg.experiment_name,
                cfg.strategy,
                manifest_path.display(),
                summary_json_path.display(),
                summary_txt_path.display(),
                rows.len(),
                top_by_return.join(", "),
                low_drawdown_candidate,
                cfg.output_dir,
                cfg.output_dir,
                cfg.output_dir,
            );
            write_diagnostics(&format!("{}/batch_summary.txt", cfg.output_dir), &summary)?;

            let stage_report = if let Some(research_cfg) = &cfg.research {
                let full_assessments =
                    assess_hypotheses(research_cfg, &to_batch_row_views(&rows));
                let in_sample_assessments = if sample_split_plan.is_some() {
                    Some(assess_hypotheses(research_cfg, &to_batch_row_views(&in_sample_rows)))
                } else {
                    None
                };
                let out_sample_assessments = if sample_split_plan.is_some() {
                    Some(assess_hypotheses(
                        research_cfg,
                        &to_batch_row_views(&out_sample_rows),
                    ))
                } else {
                    None
                };
                let auto_decision = decide_research_state(
                    research_cfg,
                    &full_assessments,
                    in_sample_assessments.as_deref(),
                    out_sample_assessments.as_deref(),
                );
                let final_decision = if let Some(override_cfg) = &research_cfg.decision_override {
                    apply_manual_override(&auto_decision, override_cfg)
                } else {
                    auto_decision.clone()
                };
                format!(
                    "=== 阶段报告 ===\n实验名称: {}\n当前阶段: {}\n研究主题: {}\n研究轮次: {}\n决策来源: {}\n关键产出:\n1. processed_summary.json / processed_summary.txt 已生成。\n2. 多资产回测启动前会读取 processed 摘要并打印。\n3. hypothesis_assessment.csv + 样本内/样本外评估已生成。\n4. walk_forward_assessment_summary.csv / cost_sensitivity_summary.csv / research_evidence_summary.csv 已生成。\n5. research_decision_auto.txt / research_decision.txt / governance_summary.txt 已生成。\n\n下一步建议:\n- {}\n- 针对最强支持假设继续缩小参数区间。\n- 对最弱假设补充样本外或成本敏感性验证。\n",
                    cfg.experiment_name,
                    final_decision.state,
                    research_cfg.topic,
                    research_cfg.round,
                    final_decision.decision_source,
                    final_decision.recommended_action,
                )
            } else {
                format!(
                    "=== 阶段报告 ===\n实验名称: {}\n当前阶段: v1.4 processed-summary workflow\n关键产出:\n1. processed_summary.json / processed_summary.txt 已生成。\n2. 多资产回测启动前会读取 processed 摘要并打印。\n3. 研究诊断已记录 processed manifest 与 summary 路径。\n4. 数据准备层与回测层的衔接更完整。\n\n下一步建议:\n- 给 processed 层加入异常样本统计。\n- 在 batch 输出里记录数据准备时间戳。\n- 为单资产回测增加 processed 可选模式。\n",
                    cfg.experiment_name,
                )
            };
            write_diagnostics(&format!("{}/stage_report.txt", cfg.output_dir), &stage_report)?;

            println!("=== 批量实验摘要 ===");
            println!("实验数量：{}", rows.len());
            println!("已写入：{}/batch_results.csv", cfg.output_dir);
            println!("已写入：{}/experiment_index.csv", cfg.output_dir);
            println!("已写入：{}/batch_summary.txt", cfg.output_dir);
            println!("已写入：{}/stage_report.txt", cfg.output_dir);
            println!("已写入：{}/config_snapshot.json", cfg.output_dir);
        }
        other => return Err(anyhow!("不支持的策略类型：{}", other)),
    }

    Ok(())
}
