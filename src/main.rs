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
    ensure_output_dir, read_csv_rows, write_contributions, write_diagnostics, write_equity_curve,
    write_csv_rows, write_experiment_index, write_holdings_trace, write_hypothesis_assessments,
    write_rebalance_log, EquityRow, ExecutionLogRow, ExperimentIndexRow, RebalanceInstructionRow,
    TargetPositionRow,
};
use research::{
    apply_manual_override, assessments_to_rows, assess_hypotheses, build_evidence_summary,
    build_sample_split_plan, build_walk_forward_windows, cost_sensitivity_detail_rows,
    decide_research_state, render_governance_summary, render_research_decision,
    render_research_plan, render_walk_forward_plan, summarize_cost_sensitivity,
    summarize_walk_forward_assessments, walk_forward_detail_rows, BatchRowView,
};
use serde::Serialize;
use strategy::runtime::RotationStrategySpec;
use std::cmp::Ordering;
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
    halted_by_risk: bool,
    halt_event_type: String,
    halt_reason: String,
    top_contributor: String,
    worst_contributor: String,
    output_dir: String,
}

#[derive(Debug, Clone)]
struct ProcessedRunSnapshot {
    total_return: f64,
    max_drawdown: f64,
    trade_count: usize,
    total_cost_paid: f64,
    final_equity: f64,
    halted_by_risk: bool,
    halt_reason: String,
    top_contributor: String,
    worst_contributor: String,
    output_dir: String,
}

#[derive(Debug, Serialize)]
struct StrategyComparisonRow {
    rank: usize,
    strategy: String,
    experiment_name: String,
    source_config: String,
    total_return: f64,
    max_drawdown: f64,
    trade_count: usize,
    total_cost_paid: f64,
    final_equity: f64,
    halted_by_risk: bool,
    halt_reason: String,
    top_contributor: String,
    worst_contributor: String,
    output_dir: String,
}

struct ProcessedStrategyContext {
    asset_files: HashMap<String, String>,
    asset_maps: HashMap<String, HashMap<NaiveDate, data::Bar>>,
    dates: Vec<NaiveDate>,
    commission: f64,
    slippage: f64,
}

#[derive(Debug, Clone)]
struct DailySignalDecision {
    model_weights: HashMap<String, f64>,
    final_weights: HashMap<String, f64>,
    model_note: String,
    final_note: String,
    decision_source: String,
    override_reason: String,
    override_owner: String,
    override_decided_at: String,
}

#[derive(Debug, Clone)]
struct ExecutionBackfillResult {
    rows: Vec<ExecutionLogRow>,
    summary: String,
    actual_weights: Option<HashMap<String, f64>>,
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
        halted_by_risk: result.summary.halted_by_risk,
        halt_event_type: last_stop_event_type(&result.risk_events).unwrap_or_default(),
        halt_reason: result
            .summary
            .halt_reason
            .clone()
            .unwrap_or_default(),
        top_contributor,
        worst_contributor,
        output_dir: exp_dir.to_string(),
    });
}

/// 为较长的研究流程打印统一格式的信息日志。
fn log_info(message: &str) {
    println!("[信息] {}", message);
}

fn is_stop_event_type(event_type: &str) -> bool {
    event_type.ends_with("_stop")
}

fn last_stop_event_type(events: &[report::RiskEventRow]) -> Option<String> {
    events
        .iter()
        .rev()
        .find(|event| is_stop_event_type(&event.event_type))
        .map(|event| event.event_type.clone())
}

fn required_asset_count_for_max_weight(max_weight: f64) -> usize {
    engine::backtest::required_asset_count_for_max_weight(max_weight)
}

fn validate_risk_config(
    risk: Option<&config::RiskConfig>,
    asset_count: Option<usize>,
) -> anyhow::Result<()> {
    if let Some(risk_cfg) = risk {
        if let Some(limit) = risk_cfg.max_single_asset_weight {
            if !(0.0..=1.0).contains(&limit) || limit == 0.0 {
                return Err(anyhow!("risk.max_single_asset_weight 必须介于 0 和 1 之间"));
            }
            if let Some(asset_count) = asset_count {
                let required_assets = required_asset_count_for_max_weight(limit);
                if asset_count < required_assets {
                    return Err(anyhow!(
                        "当前资产池只有 {} 个资产，无法满足 risk.max_single_asset_weight={:.2}% 所要求的至少 {} 个资产",
                        asset_count,
                        limit * 100.0,
                        required_assets
                    ));
                }
            }
        }
        if let Some(limit) = risk_cfg.max_daily_loss_limit {
            if !(0.0..=1.0).contains(&limit) || limit == 0.0 {
                return Err(anyhow!("risk.max_daily_loss_limit 必须介于 0 和 1 之间"));
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
        if let Some(days) = risk_cfg.stop_cooldown_days {
            if days == 0 {
                return Err(anyhow!("risk.stop_cooldown_days 必须大于 0"));
            }
        }
    }
    Ok(())
}

fn render_risk_summary(
    risk: Option<&config::RiskConfig>,
    aligned_days: usize,
    halted_count: usize,
    total_runs: usize,
    halt_reason_lines: &[String],
) -> String {
    let mut lines = vec![
        "=== 风控摘要 ===".to_string(),
        format!("对齐交易日数量: {}", aligned_days),
        format!("期末处于风控停机: {} / {}", halted_count, total_runs),
    ];

    if let Some(risk_cfg) = risk {
        if let Some(limit) = risk_cfg.min_aligned_days {
            lines.push(format!("最小样本门槛: {}", limit));
        }
        if let Some(limit) = risk_cfg.max_single_asset_weight {
            lines.push(format!("单资产权重上限: {:.2}%", limit * 100.0));
            lines.push(format!(
                "满足该权重上限所需最少资产数: {}",
                required_asset_count_for_max_weight(limit)
            ));
        }
        if let Some(limit) = risk_cfg.max_daily_loss_limit {
            lines.push(format!("单日亏损上限: {:.2}%", limit * 100.0));
        }
        if let Some(limit) = risk_cfg.max_drawdown_limit {
            lines.push(format!("最大回撤上限: {:.2}%", limit * 100.0));
        }
        if let Some(limit) = risk_cfg.max_rebalance_turnover {
            lines.push(format!("调仓换手上限: {:.2}%", limit * 100.0));
        }
        if let Some(days) = risk_cfg.stop_cooldown_days {
            lines.push(format!("风控冷静期: {} 个交易日", days));
        }
    } else {
        lines.push("风险控制: 未启用".to_string());
    }

    if !halt_reason_lines.is_empty() {
        lines.push("主要停机原因:".to_string());
        for line in halt_reason_lines {
            lines.push(format!("- {}", line));
        }
    }

    lines.join("\n") + "\n"
}

fn summarize_halt_reasons(rows: &[BatchResultRow]) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for row in rows {
        if row.halted_by_risk && !row.halt_reason.is_empty() {
            *counts.entry(row.halt_reason.clone()).or_insert(0) += 1;
        }
    }

    let mut pairs: Vec<(String, usize)> = counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    pairs
        .into_iter()
        .take(3)
        .map(|(reason, count)| format!("{} 次 - {}", count, reason))
        .collect()
}

fn format_low_drawdown_candidate(rows: &[BatchResultRow]) -> String {
    rows.iter()
        .max_by(|a, b| {
            a.max_drawdown
                .partial_cmp(&b.max_drawdown)
                .unwrap_or(Ordering::Equal)
        })
        .map(|row| format!("{} ({:.2}%)", row.experiment_id, row.max_drawdown * 100.0))
        .unwrap_or_else(|| "N/A".to_string())
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

fn load_processed_strategy_context(
    cfg: &config::AppConfig,
    strategy_spec: &RotationStrategySpec,
    emit_logs: bool,
) -> anyhow::Result<ProcessedStrategyContext> {
    let asset_files = cfg
        .asset_files
        .clone()
        .ok_or_else(|| anyhow!("{} 需要提供 asset_files", cfg.strategy))?;
    validate_risk_config(cfg.risk.as_ref(), Some(asset_files.len()))?;
    let commission = cfg
        .commission
        .ok_or_else(|| anyhow!("{} 需要提供 commission", cfg.strategy))?;
    let slippage = cfg
        .slippage
        .ok_or_else(|| anyhow!("{} 需要提供 slippage", cfg.strategy))?;

    if emit_logs {
        log_info(&format!("正在校验 {} 的 processed 输入", cfg.strategy));
    }
    validate_processed_inputs(&asset_files)?;
    if emit_logs {
        if let Some(manifest_path) = infer_manifest_path(&asset_files) {
            log_info(&format!("使用 processed 对齐清单：{}", manifest_path.display()));
        }
        if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
            log_info(&format!("使用 processed 摘要 JSON：{}", summary_json_path.display()));
        }
        log_processed_summary(&asset_files)?;
        log_info(&format!("正在加载 {} 的多资产数据", cfg.strategy));
    }

    let mut asset_maps = HashMap::new();
    for (name, path) in &asset_files {
        if emit_logs {
            log_info(&format!("正在加载资产 {}：{}", name, path));
        }
        asset_maps.insert(
            name.clone(),
            data::read_bars_map(path).with_context(|| format!("读取资产 {} 失败：{}", name, path))?,
        );
    }

    for required_asset in strategy_spec.required_assets() {
        if !asset_maps.contains_key(required_asset) {
            return Err(anyhow!(
                "策略 {} 依赖资产 {}，但 asset_files 未提供该资产",
                cfg.strategy,
                required_asset
            ));
        }
    }

    let dates = data::intersect_dates(&asset_maps);
    let required_lookback = strategy_spec.required_lookback();
    if dates.len() <= required_lookback + 1 {
        return Err(anyhow!(
            "{} 的对齐交易日不足：当前对齐后仅 {} 个交易日，要求至少 > {}",
            cfg.strategy,
            dates.len(),
            required_lookback + 1
        ));
    }
    if let Some(min_days) = cfg.risk.as_ref().and_then(|risk| risk.min_aligned_days) {
        if dates.len() < min_days {
            return Err(anyhow!(
                "{} 的对齐交易日不足：当前 {}，低于风控要求的最小样本 {}",
                cfg.strategy,
                dates.len(),
                min_days
            ));
        }
    }

    Ok(ProcessedStrategyContext {
        asset_files,
        asset_maps,
        dates,
        commission,
        slippage,
    })
}

fn snapshot_weights_for_date(
    rows: &[report::HoldingTraceRow],
    signal_date: NaiveDate,
) -> HashMap<String, f64> {
    rows.iter()
        .filter(|row| row.date == signal_date.to_string())
        .map(|row| (row.asset.clone(), row.weight))
        .collect()
}

fn with_cash_weight(weights: &HashMap<String, f64>) -> HashMap<String, f64> {
    let mut normalized = weights
        .iter()
        .filter(|(_, weight)| **weight > 1e-10)
        .map(|(asset, weight)| (asset.clone(), *weight))
        .collect::<HashMap<_, _>>();
    let used_weight: f64 = normalized.values().sum();
    let cash_weight = (1.0 - used_weight).max(0.0);
    if normalized.is_empty() {
        normalized.insert("CASH".to_string(), 1.0);
    } else if cash_weight > 1e-10 {
        normalized.insert("CASH".to_string(), cash_weight);
    }
    normalized
}

fn format_weight_map(weights: &HashMap<String, f64>) -> String {
    let mut entries: Vec<(&String, &f64)> = weights.iter().collect();
    entries.sort_by(|(asset_a, _), (asset_b, _)| asset_a.cmp(asset_b));
    entries
        .into_iter()
        .map(|(asset, weight)| format!("{}:{:.2}%", asset, weight * 100.0))
        .collect::<Vec<_>>()
        .join(", ")
}

fn normalize_target_weights(weights: &HashMap<String, f64>) -> anyhow::Result<HashMap<String, f64>> {
    let mut cleaned = HashMap::new();
    let mut total_weight = 0.0;

    for (asset, weight) in weights {
        if *weight < -1e-10 {
            return Err(anyhow!("目标权重不能为负数：{}={:.4}", asset, weight));
        }
        if *weight > 1e-10 {
            cleaned.insert(asset.clone(), *weight);
            total_weight += *weight;
        }
    }

    if total_weight > 1.0 + 1e-8 {
        return Err(anyhow!(
            "目标权重合计超过 100%：当前 {:.2}%",
            total_weight * 100.0
        ));
    }

    Ok(with_cash_weight(&cleaned))
}

fn apply_daily_manual_override(
    model_weights: &HashMap<String, f64>,
    model_note: &str,
    override_cfg: Option<&config::ManualOverrideConfig>,
) -> anyhow::Result<DailySignalDecision> {
    let Some(override_cfg) = override_cfg else {
        return Ok(DailySignalDecision {
            model_weights: model_weights.clone(),
            final_weights: model_weights.clone(),
            model_note: model_note.to_string(),
            final_note: model_note.to_string(),
            decision_source: "model".to_string(),
            override_reason: String::new(),
            override_owner: String::new(),
            override_decided_at: String::new(),
        });
    };

    let mode = override_cfg.mode.trim().to_lowercase();
    let final_weights = match mode.as_str() {
        "follow_model" => model_weights.clone(),
        "force_cash" => {
            let mut cash_only = HashMap::new();
            cash_only.insert("CASH".to_string(), 1.0);
            cash_only
        }
        "custom_weights" => {
            let custom_weights = override_cfg
                .target_weights
                .as_ref()
                .ok_or_else(|| anyhow!("manual_override.mode=custom_weights 时必须提供 target_weights"))?;
            normalize_target_weights(custom_weights)?
        }
        other => {
            return Err(anyhow!(
                "manual_override.mode 不支持：{}，当前只支持 follow_model / force_cash / custom_weights",
                other
            ))
        }
    };

    let owner = override_cfg.owner.clone().unwrap_or_default();
    let decided_at = override_cfg.decided_at.clone().unwrap_or_default();
    let final_note = format!(
        "人工覆写已生效（mode={}，reason={}）。模型原始说明：{}",
        mode, override_cfg.reason, model_note
    );

    Ok(DailySignalDecision {
        model_weights: model_weights.clone(),
        final_weights,
        model_note: model_note.to_string(),
        final_note,
        decision_source: "manual_override".to_string(),
        override_reason: override_cfg.reason.clone(),
        override_owner: owner,
        override_decided_at: decided_at,
    })
}

fn build_actual_position_rows(
    signal_date: NaiveDate,
    actual_weights: &HashMap<String, f64>,
    note: &str,
    decision: &DailySignalDecision,
) -> Vec<TargetPositionRow> {
    build_target_position_rows(
        signal_date,
        actual_weights,
        note,
        &decision.decision_source,
        &decision.override_reason,
        &decision.override_owner,
        &decision.override_decided_at,
    )
}

fn equal_weight_target(selected_assets: &[String]) -> HashMap<String, f64> {
    if selected_assets.is_empty() {
        let mut cash_only = HashMap::new();
        cash_only.insert("CASH".to_string(), 1.0);
        return cash_only;
    }

    let target_weight = 1.0 / selected_assets.len() as f64;
    selected_assets
        .iter()
        .map(|asset| (asset.clone(), target_weight))
        .collect()
}

fn apply_signal_rebalance_guards(
    current_weights: &HashMap<String, f64>,
    proposed_target: &HashMap<String, f64>,
    risk: Option<&config::RiskConfig>,
) -> (HashMap<String, f64>, Option<String>) {
    let non_cash_assets = proposed_target
        .keys()
        .filter(|asset| asset.as_str() != "CASH")
        .count();
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = required_asset_count_for_max_weight(max_weight);
        if non_cash_assets > 0 && non_cash_assets < required_assets {
            return (
                current_weights.clone(),
                Some(format!(
                    "本次信号因单资产权重上限 {:.2}% 约束被跳过，维持当前模型仓位",
                    max_weight * 100.0
                )),
            );
        }
    }

    if let Some(limit) = risk.and_then(|cfg| cfg.max_rebalance_turnover) {
        let turnover_ratio =
            engine::portfolio::compute_turnover_amount(current_weights, proposed_target);
        if turnover_ratio > limit {
            return (
                current_weights.clone(),
                Some(format!(
                    "本次信号因换手率 {:.2}% 超过上限 {:.2}% 被跳过，维持当前模型仓位",
                    turnover_ratio * 100.0,
                    limit * 100.0
                )),
            );
        }
    }

    (proposed_target.clone(), None)
}

fn build_target_position_rows(
    signal_date: NaiveDate,
    target_weights: &HashMap<String, f64>,
    note: &str,
    decision_source: &str,
    override_reason: &str,
    override_owner: &str,
    override_decided_at: &str,
) -> Vec<TargetPositionRow> {
    let mut assets: Vec<String> = target_weights.keys().cloned().collect();
    assets.sort();
    assets
        .into_iter()
        .map(|asset| TargetPositionRow {
            signal_date: signal_date.to_string(),
            target_weight: *target_weights.get(&asset).unwrap_or(&0.0),
            asset,
            decision_source: decision_source.to_string(),
            override_reason: override_reason.to_string(),
            override_owner: override_owner.to_string(),
            override_decided_at: override_decided_at.to_string(),
            note: note.to_string(),
        })
        .collect()
}

fn build_rebalance_instruction_rows(
    signal_date: NaiveDate,
    current_weights: &HashMap<String, f64>,
    target_weights: &HashMap<String, f64>,
    note: &str,
    decision_source: &str,
    override_reason: &str,
    override_owner: &str,
    override_decided_at: &str,
) -> Vec<RebalanceInstructionRow> {
    let mut assets: Vec<String> = current_weights
        .keys()
        .chain(target_weights.keys())
        .cloned()
        .collect();
    assets.sort();
    assets.dedup();

    let mut rows = Vec::new();
    for asset in assets {
        let current_weight = *current_weights.get(&asset).unwrap_or(&0.0);
        let target_weight = *target_weights.get(&asset).unwrap_or(&0.0);
        let delta_weight = target_weight - current_weight;
        let action = if delta_weight > 1e-8 {
            "BUY"
        } else if delta_weight < -1e-8 {
            "SELL"
        } else {
            "HOLD"
        };
        rows.push(RebalanceInstructionRow {
            signal_date: signal_date.to_string(),
            asset,
            action: action.to_string(),
            current_weight,
            target_weight,
            delta_weight,
            decision_source: decision_source.to_string(),
            override_reason: override_reason.to_string(),
            override_owner: override_owner.to_string(),
            override_decided_at: override_decided_at.to_string(),
            note: note.to_string(),
        });
    }

    if rows.is_empty() {
        rows.push(RebalanceInstructionRow {
            signal_date: signal_date.to_string(),
            asset: "CASH".to_string(),
            action: "HOLD".to_string(),
            current_weight: 1.0,
            target_weight: 1.0,
            delta_weight: 0.0,
            decision_source: decision_source.to_string(),
            override_reason: override_reason.to_string(),
            override_owner: override_owner.to_string(),
            override_decided_at: override_decided_at.to_string(),
            note: note.to_string(),
        });
    }

    rows
}

fn build_execution_log_rows(rows: &[RebalanceInstructionRow]) -> Vec<ExecutionLogRow> {
    rows.iter()
        .map(|row| ExecutionLogRow {
            signal_date: row.signal_date.clone(),
            asset: row.asset.clone(),
            action: row.action.clone(),
            target_weight: row.target_weight,
            execution_status: "pending".to_string(),
            executed_weight: None,
            executed_at: None,
            decision_source: row.decision_source.clone(),
            override_reason: row.override_reason.clone(),
            override_owner: row.override_owner.clone(),
            override_decided_at: row.override_decided_at.clone(),
            note: row.note.clone(),
        })
        .collect()
}

fn merge_execution_backfill(
    expected_rows: &[ExecutionLogRow],
    imported_rows: &[ExecutionLogRow],
    execution_input_path: &Path,
) -> anyhow::Result<Vec<ExecutionLogRow>> {
    if expected_rows.len() != imported_rows.len() {
        return Err(anyhow!(
            "execution_input 行数与当前信号模板不一致：模板 {} 行，输入 {} 行，文件：{}",
            expected_rows.len(),
            imported_rows.len(),
            execution_input_path.display()
        ));
    }

    let mut imported_map = HashMap::new();
    for row in imported_rows {
        imported_map.insert(
            format!("{}|{}|{}", row.signal_date, row.asset, row.action),
            row.clone(),
        );
    }

    let mut merged = Vec::new();
    for expected in expected_rows {
        let key = format!("{}|{}|{}", expected.signal_date, expected.asset, expected.action);
        let imported = imported_map.get(&key).ok_or_else(|| {
            anyhow!(
                "execution_input 缺少对应执行行：{}，文件：{}",
                key,
                execution_input_path.display()
            )
        })?;

        if (expected.target_weight - imported.target_weight).abs() > 1e-8 {
            return Err(anyhow!(
                "execution_input 的 target_weight 与当前信号不一致：{} 当前 {:.6}，输入 {:.6}",
                key,
                expected.target_weight,
                imported.target_weight
            ));
        }

        merged.push(imported.clone());
    }

    Ok(merged)
}

fn actual_weights_from_execution_rows(rows: &[ExecutionLogRow]) -> anyhow::Result<HashMap<String, f64>> {
    let mut raw_weights = HashMap::new();
    for row in rows {
        let status = row.execution_status.trim().to_lowercase();
        if matches!(status.as_str(), "filled" | "partial") {
            if let Some(weight) = row.executed_weight {
                if weight > 1e-10 {
                    raw_weights.insert(row.asset.clone(), weight);
                }
            }
        }
    }
    normalize_target_weights(&raw_weights)
}

fn render_manual_override_summary(
    signal_date: NaiveDate,
    decision: &DailySignalDecision,
) -> String {
    let applied = decision.decision_source == "manual_override";
    format!(
        "=== 人工覆写摘要 ===\n信号日期: {}\n是否应用人工覆写: {}\n决策来源: {}\n模型目标仓位: {}\n最终目标仓位: {}\n模型说明: {}\n最终说明: {}\n覆写原因: {}\n覆写人: {}\n覆写时间: {}\n",
        signal_date,
        applied,
        decision.decision_source,
        format_weight_map(&decision.model_weights),
        format_weight_map(&decision.final_weights),
        decision.model_note,
        decision.final_note,
        if decision.override_reason.is_empty() {
            "未应用".to_string()
        } else {
            decision.override_reason.clone()
        },
        if decision.override_owner.is_empty() {
            "未填写".to_string()
        } else {
            decision.override_owner.clone()
        },
        if decision.override_decided_at.is_empty() {
            "未填写".to_string()
        } else {
            decision.override_decided_at.clone()
        },
    )
}

fn render_execution_summary(
    signal_date: NaiveDate,
    execution_rows: &[ExecutionLogRow],
    execution_input_path: Option<&Path>,
    actual_weights: Option<&HashMap<String, f64>>,
) -> String {
    let mut pending = 0usize;
    let mut filled = 0usize;
    let mut partial = 0usize;
    let mut skipped = 0usize;
    let mut rejected = 0usize;
    let mut cancelled = 0usize;

    for row in execution_rows {
        match row.execution_status.trim().to_lowercase().as_str() {
            "pending" => pending += 1,
            "filled" => filled += 1,
            "partial" => partial += 1,
            "skipped" => skipped += 1,
            "rejected" => rejected += 1,
            "cancelled" => cancelled += 1,
            _ => pending += 1,
        }
    }

    format!(
        "=== 执行回写摘要 ===\n信号日期: {}\n执行输入文件: {}\n执行记录行数: {}\npending: {}\nfilled: {}\npartial: {}\nskipped: {}\nrejected: {}\ncancelled: {}\n回写后的实际仓位: {}\n",
        signal_date,
        execution_input_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "未提供 execution_input，当前仅生成待执行模板".to_string()),
        execution_rows.len(),
        pending,
        filled,
        partial,
        skipped,
        rejected,
        cancelled,
        actual_weights
            .map(format_weight_map)
            .unwrap_or_else(|| "N/A".to_string()),
    )
}

fn build_execution_backfill_result(
    template_rows: &[ExecutionLogRow],
    execution_input_path: Option<&Path>,
    signal_date: NaiveDate,
) -> anyhow::Result<ExecutionBackfillResult> {
    let Some(execution_input_path) = execution_input_path else {
        return Ok(ExecutionBackfillResult {
            rows: template_rows.to_vec(),
            summary: render_execution_summary(signal_date, template_rows, None, None),
            actual_weights: None,
        });
    };

    let imported_rows: Vec<ExecutionLogRow> = read_csv_rows(
        execution_input_path.to_str().ok_or_else(|| {
            anyhow!("execution_input 路径不是有效 UTF-8：{}", execution_input_path.display())
        })?,
    )?;
    let merged_rows = merge_execution_backfill(template_rows, &imported_rows, execution_input_path)?;
    let actual_weights = actual_weights_from_execution_rows(&merged_rows)?;
    let summary = render_execution_summary(
        signal_date,
        &merged_rows,
        Some(execution_input_path),
        Some(&actual_weights),
    );

    Ok(ExecutionBackfillResult {
        rows: merged_rows,
        summary,
        actual_weights: Some(actual_weights),
    })
}

fn run_processed_rotation_strategy(
    cfg: &config::AppConfig,
    strategy_spec: &RotationStrategySpec,
) -> anyhow::Result<ProcessedRunSnapshot> {
    ensure_output_dir(&cfg.output_dir)?;
    let ctx = load_processed_strategy_context(cfg, strategy_spec, true)?;
    let asset_files = &ctx.asset_files;
    let asset_maps = &ctx.asset_maps;
    let dates = &ctx.dates;
    let commission = ctx.commission;
    let slippage = ctx.slippage;

    println!(
        "对齐区间：{} -> {}（共 {} 个对齐交易日）",
        dates.first().unwrap(),
        dates.last().unwrap(),
        dates.len()
    );
    log_info(&format!("正在运行 {} 回测", cfg.strategy));
    let result = strategy_spec.run(asset_maps, commission, slippage, cfg.risk.as_ref());

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
    write_diagnostics(
        &format!("{}/risk_summary.txt", cfg.output_dir),
        &render_risk_summary(
            cfg.risk.as_ref(),
            dates.len(),
            usize::from(result.summary.halted_by_risk),
            1,
            &result
                .summary
                .halt_reason
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
        ),
    )?;

    let strategy_lines = strategy_spec
        .detail_rows()
        .into_iter()
        .map(|(k, v)| format!("{}: {}", k, v))
        .collect::<Vec<_>>()
        .join("\n");
    let manifest_path = infer_manifest_path(&asset_files).unwrap();
    let summary_json_path = infer_summary_json_path(&asset_files).unwrap();
    let summary_txt_path = infer_summary_txt_path(&asset_files).unwrap();
    let diagnostics = format!(
        "=== 诊断信息 ===\n实验名称: {}\n策略类型: {}\n数据层: processed\nprocessed 清单: {}\nprocessed 摘要 JSON: {}\nprocessed 摘要 TXT: {}\n资产列表: {}\n{}\n手续费: {}\n滑点: {}\n对齐交易日数量: {}\n开始日期: {}\n结束日期: {}\n总收益: {:.2}%\n最大回撤: {:.2}%\n调仓次数: {}\n总成本: {:.6}\n期末净值: {:.4}\n期末是否处于风控停机: {}\n期末停机原因: {}\n贡献最高资产: {:?}\n贡献最低资产: {:?}\n输出文件:\n- equity_curve.csv\n- rebalance_log.csv\n- holdings_trace.csv\n- asset_contribution.csv\n- risk_events.csv（如触发风控）\n",
        cfg.experiment_name,
        cfg.strategy,
        manifest_path.display(),
        summary_json_path.display(),
        summary_txt_path.display(),
        asset_files.keys().cloned().collect::<Vec<_>>().join(","),
        strategy_lines,
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

    println!("=== {} ===", strategy_spec.summary_title());
    println!("总收益：{:.2}%", result.summary.total_return * 100.0);
    println!("最大回撤：{:.2}%", result.summary.max_drawdown * 100.0);
    println!("调仓次数：{}", result.summary.trade_count);
    println!("总成本：{:.6}", result.summary.total_cost_paid);
    println!("期末净值：{:.4}", result.summary.final_equity);
    println!("期末是否处于风控停机：{}", result.summary.halted_by_risk);
    println!("贡献最高资产：{:?}", result.top_contributor);
    println!("贡献最低资产：{:?}", result.worst_contributor);
    Ok(ProcessedRunSnapshot {
        total_return: result.summary.total_return,
        max_drawdown: result.summary.max_drawdown,
        trade_count: result.summary.trade_count,
        total_cost_paid: result.summary.total_cost_paid,
        final_equity: result.summary.final_equity,
        halted_by_risk: result.summary.halted_by_risk,
        halt_reason: result.summary.halt_reason.unwrap_or_else(|| "未触发".to_string()),
        top_contributor: result
            .top_contributor
            .clone()
            .map(|item| item.0)
            .unwrap_or_default(),
        worst_contributor: result
            .worst_contributor
            .clone()
            .map(|item| item.0)
            .unwrap_or_default(),
        output_dir: cfg.output_dir.clone(),
    })
}

fn resolve_child_config_path(base_config_path: &str, child_path: &str) -> PathBuf {
    let path = Path::new(child_path);
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let base_dir = Path::new(base_config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let resolved = base_dir.join(path);
    if resolved.exists() {
        resolved
    } else {
        path.to_path_buf()
    }
}

fn run_strategy_compare(compare_cfg: &config::AppConfig, compare_config_path: &str) -> anyhow::Result<()> {
    ensure_output_dir(&compare_cfg.output_dir)?;
    fs::copy(
        compare_config_path,
        format!("{}/config_snapshot.json", compare_cfg.output_dir),
    )
    .with_context(|| {
        format!(
            "写入对比配置快照失败：{}/config_snapshot.json",
            compare_cfg.output_dir
        )
    })?;

    let compare_configs = compare_cfg
        .compare_configs
        .clone()
        .ok_or_else(|| anyhow!("strategy_compare 需要提供 compare_configs"))?;
    if compare_configs.is_empty() {
        return Err(anyhow!("strategy_compare 的 compare_configs 不能为空"));
    }

    let mut rows: Vec<StrategyComparisonRow> = Vec::new();
    for config_ref in compare_configs {
        let resolved_path = resolve_child_config_path(compare_config_path, &config_ref);
        let resolved_str = resolved_path.to_string_lossy().to_string();
        log_info(&format!("正在加载对比策略配置：{}", resolved_path.display()));
        let sub_cfg = load_config(&resolved_str)
            .with_context(|| format!("读取策略配置失败：{}", resolved_path.display()))?;
        match sub_cfg.strategy.as_str() {
            "buy_hold_single"
            | "buy_hold_equal_weight"
            | "absolute_momentum_breadth"
            | "absolute_momentum_single"
            | "volatility_adjusted_momentum"
            | "reversal_bottomn"
            | "momentum_topn"
            | "dual_momentum"
            | "risk_off_rotation"
            | "ma_timing_single"
            | "breakout_rotation_topn"
            | "relative_strength_pair"
            | "breakout_timing_single" => {}
            other => {
                return Err(anyhow!(
                    "strategy_compare 目前只支持 processed-first 策略配置，当前为：{}",
                    other
                ))
            }
        }
        let strategy_spec = RotationStrategySpec::from_app_config(&sub_cfg)
            .with_context(|| format!("解析策略配置失败：{}", resolved_path.display()))?;
        let snapshot = run_processed_rotation_strategy(&sub_cfg, &strategy_spec)
            .with_context(|| format!("执行策略失败：{}", resolved_path.display()))?;
        rows.push(StrategyComparisonRow {
            rank: 0,
            strategy: sub_cfg.strategy.clone(),
            experiment_name: sub_cfg.experiment_name.clone(),
            source_config: resolved_path.display().to_string(),
            total_return: snapshot.total_return,
            max_drawdown: snapshot.max_drawdown,
            trade_count: snapshot.trade_count,
            total_cost_paid: snapshot.total_cost_paid,
            final_equity: snapshot.final_equity,
            halted_by_risk: snapshot.halted_by_risk,
            halt_reason: snapshot.halt_reason,
            top_contributor: snapshot.top_contributor,
            worst_contributor: snapshot.worst_contributor,
            output_dir: snapshot.output_dir,
        });
    }

    rows.sort_by(|a, b| {
        a.halted_by_risk
            .cmp(&b.halted_by_risk)
            .then_with(|| {
                b.total_return
                    .partial_cmp(&a.total_return)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                b.max_drawdown
                    .partial_cmp(&a.max_drawdown)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| a.strategy.cmp(&b.strategy))
    });
    for (index, row) in rows.iter_mut().enumerate() {
        row.rank = index + 1;
    }

    write_csv_rows(&format!("{}/comparison.csv", compare_cfg.output_dir), &rows)?;
    let halted_count = rows.iter().filter(|row| row.halted_by_risk).count();
    let top_choice = rows
        .first()
        .map(|row| {
            format!(
                "{} / {}（收益 {:.2}%，回撤 {:.2}%，风控停机: {}）",
                row.strategy,
                row.experiment_name,
                row.total_return * 100.0,
                row.max_drawdown * 100.0,
                row.halted_by_risk
            )
        })
        .unwrap_or_else(|| "N/A".to_string());
    let summary = format!(
        "=== 跨策略对比摘要 ===\n实验名称: {}\n策略类型: strategy_compare\n对比策略数量: {}\n期末处于风控停机的策略数: {}\n排序规则: 先看期末是否处于风控停机（未停机优先），再看总收益（高优先），再看最大回撤（高优先，即回撤更小）\n第一优先候选: {}\n输出文件:\n- {}/comparison.csv\n- {}/comparison_summary.txt\n- {}/config_snapshot.json\n",
        compare_cfg.experiment_name,
        rows.len(),
        halted_count,
        top_choice,
        compare_cfg.output_dir,
        compare_cfg.output_dir,
        compare_cfg.output_dir,
    );
    write_diagnostics(
        &format!("{}/comparison_summary.txt", compare_cfg.output_dir),
        &summary,
    )?;

    println!("=== 跨策略统一对比摘要 ===");
    println!("策略数量：{}", rows.len());
    println!("期末处于风控停机的策略数：{}", halted_count);
    println!("第一优先候选：{}", top_choice);
    println!("已写入：{}/comparison.csv", compare_cfg.output_dir);
    println!("已写入：{}/comparison_summary.txt", compare_cfg.output_dir);
    println!("已写入：{}/config_snapshot.json", compare_cfg.output_dir);
    Ok(())
}

fn run_daily_signal(daily_cfg: &config::AppConfig, daily_config_path: &str) -> anyhow::Result<()> {
    ensure_output_dir(&daily_cfg.output_dir)?;
    fs::copy(
        daily_config_path,
        format!("{}/config_snapshot.json", daily_cfg.output_dir),
    )
    .with_context(|| {
        format!(
            "写入 daily_signal 配置快照失败：{}/config_snapshot.json",
            daily_cfg.output_dir
        )
    })?;

    let source_config_ref = daily_cfg
        .source_config
        .clone()
        .ok_or_else(|| anyhow!("daily_signal 需要提供 source_config"))?;
    let source_config_path = resolve_child_config_path(daily_config_path, &source_config_ref);
    let source_config_str = source_config_path.to_string_lossy().to_string();
    log_info(&format!(
        "正在加载 daily_signal 的来源策略配置：{}",
        source_config_path.display()
    ));
    let source_cfg = load_config(&source_config_str)
        .with_context(|| format!("读取来源策略配置失败：{}", source_config_path.display()))?;
    let source_strategy_spec = RotationStrategySpec::from_app_config(&source_cfg)
        .with_context(|| format!("解析来源策略配置失败：{}", source_config_path.display()))?;
    match source_cfg.strategy.as_str() {
        "buy_hold_single"
        | "buy_hold_equal_weight"
        | "absolute_momentum_breadth"
        | "absolute_momentum_single"
        | "volatility_adjusted_momentum"
        | "reversal_bottomn"
        | "dual_momentum"
        | "risk_off_rotation"
        | "ma_timing_single"
        | "breakout_rotation_topn"
        | "relative_strength_pair"
        | "breakout_timing_single"
        | "momentum_topn" => {}
        other => {
            return Err(anyhow!(
                "daily_signal 目前只支持 processed 轮动策略，当前来源策略为：{}",
                other
            ))
        }
    }

    fs::copy(
        &source_config_path,
        format!("{}/source_config_snapshot.json", daily_cfg.output_dir),
    )
    .with_context(|| {
        format!(
            "写入来源策略配置快照失败：{}/source_config_snapshot.json",
            daily_cfg.output_dir
        )
    })?;

    let ctx = load_processed_strategy_context(&source_cfg, &source_strategy_spec, true)?;
    println!(
        "对齐区间：{} -> {}（共 {} 个对齐交易日）",
        ctx.dates.first().unwrap(),
        ctx.dates.last().unwrap(),
        ctx.dates.len()
    );
    log_info(&format!("正在运行 {} 的最新信号计算", source_cfg.strategy));
    let result = source_strategy_spec.run(
        &ctx.asset_maps,
        ctx.commission,
        ctx.slippage,
        source_cfg.risk.as_ref(),
    );

    let signal_date = *ctx.dates.last().unwrap();
    let signal_index = ctx.dates.len() - 1;
    let current_weights =
        with_cash_weight(&snapshot_weights_for_date(&result.holdings_trace, signal_date));
    let rebalance_due = source_strategy_spec.is_rebalance_due(signal_index);
    let mut signal_note = if result.summary.halted_by_risk {
        result
            .summary
            .halt_reason
            .clone()
            .unwrap_or_else(|| "当前处于风控停机，维持空仓".to_string())
    } else if rebalance_due {
        "当前为调仓信号日，已生成下一交易日目标仓位".to_string()
    } else {
        "当前不是调仓信号日，维持当前模型仓位".to_string()
    };

    let model_target_weights = if result.summary.halted_by_risk {
        let mut cash_only = HashMap::new();
        cash_only.insert("CASH".to_string(), 1.0);
        cash_only
    } else if rebalance_due {
        let selected_assets = source_strategy_spec.preview_selected_assets(
            &ctx.asset_maps,
            &ctx.dates,
            signal_index,
            source_cfg.risk.as_ref(),
        );
        let proposed_target = equal_weight_target(&selected_assets);
        let (effective_target, guard_note) = apply_signal_rebalance_guards(
            &current_weights,
            &proposed_target,
            source_cfg.risk.as_ref(),
        );
        if let Some(note) = guard_note {
            signal_note = note;
        }
        effective_target
    } else {
        current_weights.clone()
    };

    let decision = apply_daily_manual_override(
        &model_target_weights,
        &signal_note,
        daily_cfg.manual_override.as_ref(),
    )?;
    let model_target_rows = build_target_position_rows(
        signal_date,
        &decision.model_weights,
        &decision.model_note,
        "model",
        "",
        "",
        "",
    );
    let target_rows = build_target_position_rows(
        signal_date,
        &decision.final_weights,
        &decision.final_note,
        &decision.decision_source,
        &decision.override_reason,
        &decision.override_owner,
        &decision.override_decided_at,
    );
    let instruction_rows = build_rebalance_instruction_rows(
        signal_date,
        &current_weights,
        &decision.final_weights,
        &decision.final_note,
        &decision.decision_source,
        &decision.override_reason,
        &decision.override_owner,
        &decision.override_decided_at,
    );
    let execution_template_rows = build_execution_log_rows(&instruction_rows);
    let execution_input_path = daily_cfg
        .execution_input
        .as_ref()
        .map(|path| resolve_child_config_path(daily_config_path, path));
    let execution_backfill = build_execution_backfill_result(
        &execution_template_rows,
        execution_input_path.as_deref(),
        signal_date,
    )?;

    write_csv_rows(
        &format!("{}/model_target_positions.csv", daily_cfg.output_dir),
        &model_target_rows,
    )?;
    write_csv_rows(
        &format!("{}/target_positions.csv", daily_cfg.output_dir),
        &target_rows,
    )?;
    write_csv_rows(
        &format!("{}/rebalance_instructions.csv", daily_cfg.output_dir),
        &instruction_rows,
    )?;
    write_csv_rows(
        &format!("{}/execution_log.csv", daily_cfg.output_dir),
        &execution_backfill.rows,
    )?;
    if let Some(actual_weights) = &execution_backfill.actual_weights {
        let actual_rows = build_actual_position_rows(
            signal_date,
            actual_weights,
            "来自 execution_input 的执行回写结果",
            &decision,
        );
        write_csv_rows(
            &format!("{}/actual_positions.csv", daily_cfg.output_dir),
            &actual_rows,
        )?;
    }
    write_diagnostics(
        &format!("{}/manual_override_summary.txt", daily_cfg.output_dir),
        &render_manual_override_summary(signal_date, &decision),
    )?;
    write_diagnostics(
        &format!("{}/execution_summary.txt", daily_cfg.output_dir),
        &execution_backfill.summary,
    )?;

    let latest_rebalance = result.rebalances.last();
    let manifest_path = infer_manifest_path(&ctx.asset_files).unwrap();
    let summary_json_path = infer_summary_json_path(&ctx.asset_files).unwrap();
    let summary_txt_path = infer_summary_txt_path(&ctx.asset_files).unwrap();
    let current_positions_text = format_weight_map(&current_weights);
    let model_target_positions_text = format_weight_map(&decision.model_weights);
    let target_positions_text = format_weight_map(&decision.final_weights);
    let summary = format!(
        "=== 每日信号摘要 ===\n实验名称: {}\n运行模式: daily_signal\n来源策略配置: {}\n来源实验名称: {}\n来源策略类型: {}\n信号日期: {}\n是否调仓信号日: {}\n当前模型仓位: {}\n模型目标仓位: {}\n最终目标仓位: {}\n模型信号说明: {}\n最终执行说明: {}\n决策来源: {}\n人工覆写原因: {}\n人工覆写人: {}\n人工覆写时间: {}\n执行回写文件: {}\n期末是否处于风控停机: {}\n期末停机原因: {}\n最近一次调仓日期: {}\n最近一次调仓目标: {}\nprocessed 清单: {}\nprocessed 摘要 JSON: {}\nprocessed 摘要 TXT: {}\n输出文件:\n- {}/signal_summary.txt\n- {}/model_target_positions.csv\n- {}/target_positions.csv\n- {}/rebalance_instructions.csv\n- {}/execution_log.csv\n- {}/manual_override_summary.txt\n- {}/execution_summary.txt\n- {}/actual_positions.csv（如提供 execution_input）\n- {}/config_snapshot.json\n- {}/source_config_snapshot.json\n",
        daily_cfg.experiment_name,
        source_config_path.display(),
        source_cfg.experiment_name,
        source_cfg.strategy,
        signal_date,
        rebalance_due,
        current_positions_text,
        model_target_positions_text,
        target_positions_text,
        decision.model_note,
        decision.final_note,
        decision.decision_source,
        if decision.override_reason.is_empty() {
            "未应用".to_string()
        } else {
            decision.override_reason.clone()
        },
        if decision.override_owner.is_empty() {
            "未填写".to_string()
        } else {
            decision.override_owner.clone()
        },
        if decision.override_decided_at.is_empty() {
            "未填写".to_string()
        } else {
            decision.override_decided_at.clone()
        },
        execution_input_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "未提供".to_string()),
        result.summary.halted_by_risk,
        result
            .summary
            .halt_reason
            .clone()
            .unwrap_or_else(|| "未触发".to_string()),
        latest_rebalance
            .map(|row| row.date.clone())
            .unwrap_or_else(|| "N/A".to_string()),
        latest_rebalance
            .map(|row| row.selected_assets.clone())
            .unwrap_or_else(|| "N/A".to_string()),
        manifest_path.display(),
        summary_json_path.display(),
        summary_txt_path.display(),
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
        daily_cfg.output_dir,
    );
    write_diagnostics(&format!("{}/signal_summary.txt", daily_cfg.output_dir), &summary)?;

    println!("=== 每日信号摘要 ===");
    println!("来源策略：{}", source_cfg.strategy);
    println!("信号日期：{}", signal_date);
    println!("是否调仓信号日：{}", rebalance_due);
    println!("决策来源：{}", decision.decision_source);
    println!("目标仓位：{}", target_positions_text);
    println!("信号说明：{}", decision.final_note);
    println!("已写入：{}/signal_summary.txt", daily_cfg.output_dir);
    println!("已写入：{}/model_target_positions.csv", daily_cfg.output_dir);
    println!("已写入：{}/target_positions.csv", daily_cfg.output_dir);
    println!("已写入：{}/rebalance_instructions.csv", daily_cfg.output_dir);
    println!("已写入：{}/execution_log.csv", daily_cfg.output_dir);
    println!("已写入：{}/manual_override_summary.txt", daily_cfg.output_dir);
    println!("已写入：{}/execution_summary.txt", daily_cfg.output_dir);
    if execution_backfill.actual_weights.is_some() {
        println!("已写入：{}/actual_positions.csv", daily_cfg.output_dir);
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
        "buy_hold_single"
        | "buy_hold_equal_weight"
        | "absolute_momentum_breadth"
        | "absolute_momentum_single"
        | "volatility_adjusted_momentum"
        | "reversal_bottomn"
        | "dual_momentum"
        | "risk_off_rotation"
        | "ma_timing_single"
        | "breakout_rotation_topn"
        | "relative_strength_pair"
        | "breakout_timing_single"
        | "momentum_topn" => {
            let strategy_spec = RotationStrategySpec::from_app_config(&cfg)?;
            let _ = run_processed_rotation_strategy(&cfg, &strategy_spec)?;
        }
        "strategy_compare" => {
            run_strategy_compare(&cfg, &config_path)?;
        }
        "daily_signal" => {
            run_daily_signal(&cfg, &config_path)?;
        }
        "momentum_batch" => {
            let asset_files = cfg
                .asset_files
                .clone()
                .ok_or_else(|| anyhow!("momentum_batch 需要提供 asset_files"))?;
            validate_risk_config(cfg.risk.as_ref(), Some(asset_files.len()))?;
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
                                "实验ID: {}\n数据层: processed\nlookback: {}\nrebalance_freq: {}\ntop_n: {}\n单位成本: {}\n总收益: {:.2}%\n最大回撤: {:.2}%\n交易次数: {}\n总成本: {:.6}\n期末净值: {:.4}\n期末是否处于风控停机: {}\n期末停机原因: {}\n贡献最高资产: {:?}\n贡献最低资产: {:?}\n",
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
                                halted_by_risk: result.summary.halted_by_risk,
                                halt_event_type: last_stop_event_type(&result.risk_events)
                                    .unwrap_or_default(),
                                halt_reason: result
                                    .summary
                                    .halt_reason
                                    .clone()
                                    .unwrap_or_default(),
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
            let halted_count = rows.iter().filter(|row| row.halted_by_risk).count();
            let low_drawdown_candidate = format_low_drawdown_candidate(&rows);
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
                "=== 批量实验摘要 ===\n实验名称: {}\n策略类型: {}\n数据层: processed\nprocessed 清单: {}\nprocessed 摘要 JSON: {}\nprocessed 摘要 TXT: {}\n实验数量: {}\n期末处于风控停机的实验数: {}\n收益前三组合: {}\n最低回撤候选: {}\n配置快照: {}/config_snapshot.json\n结果总表: {}/batch_results.csv\n实验索引: {}/experiment_index.csv\n",
                cfg.experiment_name,
                cfg.strategy,
                manifest_path.display(),
                summary_json_path.display(),
                summary_txt_path.display(),
                rows.len(),
                halted_count,
                top_by_return.join(", "),
                low_drawdown_candidate,
                cfg.output_dir,
                cfg.output_dir,
                cfg.output_dir,
            );
            write_diagnostics(&format!("{}/batch_summary.txt", cfg.output_dir), &summary)?;
            write_diagnostics(
                &format!("{}/risk_summary.txt", cfg.output_dir),
                &render_risk_summary(
                    cfg.risk.as_ref(),
                    aligned_dates.len(),
                    halted_count,
                    rows.len(),
                    &summarize_halt_reasons(&rows),
                ),
            )?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_row(experiment_id: &str, max_drawdown: f64) -> BatchResultRow {
        BatchResultRow {
            experiment_id: experiment_id.to_string(),
            lookback: 20,
            rebalance_freq: 20,
            top_n: 2,
            unit_cost: 0.0004,
            total_return: 0.0,
            max_drawdown,
            trade_count: 0,
            total_cost_paid: 0.0,
            final_equity: 1.0,
            halted_by_risk: false,
            halt_event_type: String::new(),
            halt_reason: String::new(),
            top_contributor: String::new(),
            worst_contributor: String::new(),
            output_dir: String::new(),
        }
    }

    fn weight_map(items: &[(&str, f64)]) -> HashMap<String, f64> {
        items.iter().map(|(asset, weight)| ((*asset).to_string(), *weight)).collect()
    }

    #[test]
    fn validate_risk_config_rejects_insufficient_asset_universe() {
        let risk = config::RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: Some(0.4),
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: None,
            stop_cooldown_days: None,
        };

        let err = validate_risk_config(Some(&risk), Some(2)).unwrap_err();
        assert!(err
            .to_string()
            .contains("无法满足 risk.max_single_asset_weight"));
    }

    #[test]
    fn validate_risk_config_rejects_zero_cooldown_days() {
        let risk = config::RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: None,
            max_drawdown_limit: Some(0.15),
            max_rebalance_turnover: None,
            stop_cooldown_days: Some(0),
        };

        let err = validate_risk_config(Some(&risk), Some(4)).unwrap_err();
        assert!(err.to_string().contains("risk.stop_cooldown_days"));
    }

    #[test]
    fn low_drawdown_candidate_prefers_shallower_drawdown() {
        let rows = vec![batch_row("deep", -0.25), batch_row("shallow", -0.08)];

        let candidate = format_low_drawdown_candidate(&rows);

        assert!(candidate.contains("shallow"));
        assert!(candidate.contains("-8.00%"));
    }

    #[test]
    fn with_cash_weight_adds_cash_for_empty_positions() {
        let weights = with_cash_weight(&HashMap::new());

        assert_eq!(weights.get("CASH"), Some(&1.0));
    }

    #[test]
    fn format_weight_map_orders_assets_stably() {
        let weights = weight_map(&[("zz500", 0.3), ("hs300", 0.5), ("CASH", 0.2)]);

        let rendered = format_weight_map(&weights);

        assert_eq!(rendered, "CASH:20.00%, hs300:50.00%, zz500:30.00%");
    }

    #[test]
    fn apply_signal_rebalance_guards_skips_when_turnover_too_high() {
        let current = weight_map(&[("hs300", 1.0)]);
        let proposed = weight_map(&[("cyb", 1.0)]);
        let risk = config::RiskConfig {
            min_aligned_days: None,
            max_single_asset_weight: None,
            max_daily_loss_limit: None,
            max_drawdown_limit: None,
            max_rebalance_turnover: Some(0.2),
            stop_cooldown_days: None,
        };

        let (effective, note) = apply_signal_rebalance_guards(&current, &proposed, Some(&risk));

        assert_eq!(effective, current);
        assert!(note.unwrap().contains("换手率"));
    }

    #[test]
    fn manual_override_force_cash_replaces_model_target() {
        let model = weight_map(&[("hs300", 1.0)]);
        let override_cfg = config::ManualOverrideConfig {
            mode: "force_cash".to_string(),
            reason: "人工转为空仓".to_string(),
            owner: Some("ops".to_string()),
            decided_at: Some("2026-03-27 09:30:00".to_string()),
            target_weights: None,
        };

        let decision =
            apply_daily_manual_override(&model, "模型建议持有 hs300", Some(&override_cfg)).unwrap();

        assert_eq!(decision.decision_source, "manual_override");
        assert_eq!(decision.final_weights.get("CASH"), Some(&1.0));
        assert!(decision.final_note.contains("人工覆写已生效"));
    }

    #[test]
    fn actual_weights_from_execution_rows_uses_filled_rows() {
        let rows = vec![
            ExecutionLogRow {
                signal_date: "2026-03-27".to_string(),
                asset: "hs300".to_string(),
                action: "BUY".to_string(),
                target_weight: 0.6,
                execution_status: "filled".to_string(),
                executed_weight: Some(0.55),
                executed_at: Some("2026-03-27 10:01:00".to_string()),
                decision_source: "manual_override".to_string(),
                override_reason: "人工降仓".to_string(),
                override_owner: "ops".to_string(),
                override_decided_at: "2026-03-27 09:30:00".to_string(),
                note: "test".to_string(),
            },
            ExecutionLogRow {
                signal_date: "2026-03-27".to_string(),
                asset: "dividend".to_string(),
                action: "BUY".to_string(),
                target_weight: 0.4,
                execution_status: "partial".to_string(),
                executed_weight: Some(0.25),
                executed_at: Some("2026-03-27 10:02:00".to_string()),
                decision_source: "manual_override".to_string(),
                override_reason: "人工降仓".to_string(),
                override_owner: "ops".to_string(),
                override_decided_at: "2026-03-27 09:30:00".to_string(),
                note: "test".to_string(),
            },
        ];

        let actual = actual_weights_from_execution_rows(&rows).unwrap();

        assert_eq!(actual.get("hs300"), Some(&0.55));
        assert_eq!(actual.get("dividend"), Some(&0.25));
        assert!((actual.get("CASH").copied().unwrap_or_default() - 0.20).abs() < 1e-12);
    }

    #[test]
    fn merge_execution_backfill_requires_matching_template_rows() {
        let expected = vec![ExecutionLogRow {
            signal_date: "2026-03-27".to_string(),
            asset: "hs300".to_string(),
            action: "BUY".to_string(),
            target_weight: 1.0,
            execution_status: "pending".to_string(),
            executed_weight: None,
            executed_at: None,
            decision_source: "model".to_string(),
            override_reason: String::new(),
            override_owner: String::new(),
            override_decided_at: String::new(),
            note: "test".to_string(),
        }];
        let imported = vec![ExecutionLogRow {
            signal_date: "2026-03-27".to_string(),
            asset: "cyb".to_string(),
            action: "BUY".to_string(),
            target_weight: 1.0,
            execution_status: "filled".to_string(),
            executed_weight: Some(1.0),
            executed_at: Some("2026-03-27 10:00:00".to_string()),
            decision_source: "model".to_string(),
            override_reason: String::new(),
            override_owner: String::new(),
            override_decided_at: String::new(),
            note: "test".to_string(),
        }];

        let err = merge_execution_backfill(
            &expected,
            &imported,
            std::path::Path::new("output/mock_execution_input.csv"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("缺少对应执行行"));
    }

    #[test]
    fn build_rebalance_instruction_rows_marks_weight_changes() {
        let current = weight_map(&[("hs300", 1.0)]);
        let target = weight_map(&[("cyb", 0.5), ("dividend", 0.5)]);

        let rows = build_rebalance_instruction_rows(
            NaiveDate::parse_from_str("2026-03-27", "%Y-%m-%d").unwrap(),
            &current,
            &target,
            "test",
            "model",
            "",
            "",
            "",
        );

        assert!(rows.iter().any(|row| row.asset == "hs300" && row.action == "SELL"));
        assert!(rows.iter().any(|row| row.asset == "cyb" && row.action == "BUY"));
        assert!(rows.iter().any(|row| row.asset == "dividend" && row.action == "BUY"));
    }
}
