use crate::config;
use crate::data;
use crate::engine;
use crate::report;
use crate::research::BatchRowView;
use crate::strategy::runtime::RotationStrategySpec;
use anyhow::{anyhow, Context};
use chrono::NaiveDate;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;

#[derive(Debug, Serialize)]
pub(super) struct BatchResultRow {
    pub(super) experiment_id: String,
    pub(super) lookback: usize,
    pub(super) rebalance_freq: usize,
    pub(super) top_n: usize,
    pub(super) unit_cost: f64,
    pub(super) total_return: f64,
    pub(super) max_drawdown: f64,
    pub(super) trade_count: usize,
    pub(super) total_cost_paid: f64,
    pub(super) final_equity: f64,
    pub(super) halted_by_risk: bool,
    pub(super) halt_event_type: String,
    pub(super) halt_reason: String,
    pub(super) top_contributor: String,
    pub(super) worst_contributor: String,
    pub(super) output_dir: String,
}

#[derive(Debug, Clone)]
pub(super) struct ProcessedRunSnapshot {
    pub(super) total_return: f64,
    pub(super) max_drawdown: f64,
    pub(super) trade_count: usize,
    pub(super) total_cost_paid: f64,
    pub(super) final_equity: f64,
    pub(super) halted_by_risk: bool,
    pub(super) halt_reason: String,
    pub(super) top_contributor: String,
    pub(super) worst_contributor: String,
    pub(super) output_dir: String,
}

#[derive(Debug, Serialize)]
pub(super) struct StrategyComparisonRow {
    pub(super) rank: usize,
    pub(super) strategy: String,
    pub(super) experiment_name: String,
    pub(super) source_config: String,
    pub(super) total_return: f64,
    pub(super) max_drawdown: f64,
    pub(super) trade_count: usize,
    pub(super) total_cost_paid: f64,
    pub(super) final_equity: f64,
    pub(super) halted_by_risk: bool,
    pub(super) halt_reason: String,
    pub(super) top_contributor: String,
    pub(super) worst_contributor: String,
    pub(super) output_dir: String,
}

pub(super) struct ProcessedStrategyContext {
    pub(super) asset_files: HashMap<String, String>,
    pub(super) asset_maps: HashMap<String, HashMap<NaiveDate, data::Bar>>,
    pub(super) dates: Vec<NaiveDate>,
    pub(super) commission: f64,
    pub(super) slippage: f64,
}

#[derive(Debug, Clone)]
pub(super) struct DailySignalDecision {
    pub(super) model_weights: HashMap<String, f64>,
    pub(super) final_weights: HashMap<String, f64>,
    pub(super) model_note: String,
    pub(super) final_note: String,
    pub(super) decision_source: String,
    pub(super) override_reason: String,
    pub(super) override_owner: String,
    pub(super) override_decided_at: String,
}

#[derive(Debug, Clone)]
pub(super) struct ExecutionBackfillResult {
    pub(super) rows: Vec<report::ExecutionLogRow>,
    pub(super) summary: String,
    pub(super) actual_weights: Option<HashMap<String, f64>>,
}

pub(super) struct BatchRunSpec<'a> {
    pub(super) exp_id: &'a str,
    pub(super) exp_dir: &'a str,
    pub(super) lookback: usize,
    pub(super) rebalance_freq: usize,
    pub(super) top_n: usize,
    pub(super) unit_cost: f64,
}

pub(super) fn write_batch_results_csv(path: &str, rows: &[BatchResultRow]) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

pub(super) fn to_batch_row_views(rows: &[BatchResultRow]) -> Vec<BatchRowView> {
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

pub(super) fn push_batch_result_row(
    rows: &mut Vec<BatchResultRow>,
    spec: &BatchRunSpec<'_>,
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
        experiment_id: spec.exp_id.to_string(),
        lookback: spec.lookback,
        rebalance_freq: spec.rebalance_freq,
        top_n: spec.top_n,
        unit_cost: spec.unit_cost,
        total_return: result.summary.total_return,
        max_drawdown: result.summary.max_drawdown,
        trade_count: result.summary.trade_count,
        total_cost_paid: result.summary.total_cost_paid,
        final_equity: result.summary.final_equity,
        halted_by_risk: result.summary.halted_by_risk,
        halt_event_type: last_stop_event_type(&result.risk_events).unwrap_or_default(),
        halt_reason: result.summary.halt_reason.clone().unwrap_or_default(),
        top_contributor,
        worst_contributor,
        output_dir: spec.exp_dir.to_string(),
    });
}

pub(super) fn log_info(message: &str) {
    println!("[信息] {}", message);
}

pub(super) fn is_stop_event_type(event_type: &str) -> bool {
    event_type.ends_with("_stop")
}

pub(super) fn last_stop_event_type(events: &[report::RiskEventRow]) -> Option<String> {
    events
        .iter()
        .rev()
        .find(|event| is_stop_event_type(&event.event_type))
        .map(|event| event.event_type.clone())
}

pub(super) fn validate_risk_config(
    risk: Option<&config::RiskConfig>,
    asset_count: Option<usize>,
) -> anyhow::Result<()> {
    if let Some(risk_cfg) = risk {
        if let Some(limit) = risk_cfg.max_single_asset_weight {
            if !(0.0..=1.0).contains(&limit) || limit == 0.0 {
                return Err(anyhow!("risk.max_single_asset_weight 必须介于 0 和 1 之间"));
            }
            if let Some(asset_count) = asset_count {
                let required_assets = engine::backtest::required_asset_count_for_max_weight(limit);
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

pub(super) fn render_risk_summary(
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
                engine::backtest::required_asset_count_for_max_weight(limit)
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

pub(super) fn summarize_halt_reasons(rows: &[BatchResultRow]) -> Vec<String> {
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

pub(super) fn format_low_drawdown_candidate(rows: &[BatchResultRow]) -> String {
    rows.iter()
        .max_by(|a, b| {
            a.max_drawdown
                .partial_cmp(&b.max_drawdown)
                .unwrap_or(Ordering::Equal)
        })
        .map(|row| format!("{} ({:.2}%)", row.experiment_id, row.max_drawdown * 100.0))
        .unwrap_or_else(|| "N/A".to_string())
}

pub(super) fn parse_config_path() -> anyhow::Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 || args[1] != "--config" {
        return Err(anyhow!("用法：cargo run -- --config <json配置路径>"));
    }
    Ok(args[2].clone())
}

pub(super) fn load_processed_strategy_context(
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
    super::validate_processed_inputs(&asset_files)?;
    if emit_logs {
        if let Some(manifest_path) = super::infer_manifest_path(&asset_files) {
            log_info(&format!(
                "使用 processed 对齐清单：{}",
                manifest_path.display()
            ));
        }
        if let Some(summary_json_path) = super::infer_summary_json_path(&asset_files) {
            log_info(&format!(
                "使用 processed 摘要 JSON：{}",
                summary_json_path.display()
            ));
        }
        super::log_processed_summary(&asset_files)?;
        log_info(&format!("正在加载 {} 的多资产数据", cfg.strategy));
    }

    let mut asset_maps = HashMap::new();
    for (name, path) in &asset_files {
        if emit_logs {
            log_info(&format!("正在加载资产 {}：{}", name, path));
        }
        asset_maps.insert(
            name.clone(),
            data::read_bars_map(path)
                .with_context(|| format!("读取资产 {} 失败：{}", name, path))?,
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

pub(super) fn snapshot_weights_for_date(
    rows: &[report::HoldingTraceRow],
    signal_date: NaiveDate,
) -> HashMap<String, f64> {
    rows.iter()
        .filter(|row| row.date == signal_date.to_string())
        .map(|row| (row.asset.clone(), row.weight))
        .collect()
}

pub(super) fn with_cash_weight(weights: &HashMap<String, f64>) -> HashMap<String, f64> {
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

pub(super) fn format_weight_map(weights: &HashMap<String, f64>) -> String {
    let mut entries: Vec<(&String, &f64)> = weights.iter().collect();
    entries.sort_by(|(asset_a, _), (asset_b, _)| asset_a.cmp(asset_b));
    entries
        .into_iter()
        .map(|(asset, weight)| format!("{}:{:.2}%", asset, weight * 100.0))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn normalize_target_weights(
    weights: &HashMap<String, f64>,
) -> anyhow::Result<HashMap<String, f64>> {
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

pub(super) fn apply_daily_manual_override(
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

pub(super) fn build_actual_position_rows(
    signal_date: NaiveDate,
    actual_weights: &HashMap<String, f64>,
    note: &str,
    decision: &DailySignalDecision,
) -> Vec<report::TargetPositionRow> {
    super::build_target_position_rows(
        signal_date,
        actual_weights,
        note,
        &decision.decision_source,
        &decision.override_reason,
        &decision.override_owner,
        &decision.override_decided_at,
    )
}

pub(super) fn equal_weight_target(selected_assets: &[String]) -> HashMap<String, f64> {
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

pub(super) fn apply_signal_rebalance_guards(
    current_weights: &HashMap<String, f64>,
    proposed_target: &HashMap<String, f64>,
    risk: Option<&config::RiskConfig>,
) -> (HashMap<String, f64>, Option<String>) {
    let non_cash_assets = proposed_target
        .keys()
        .filter(|asset| asset.as_str() != "CASH")
        .count();
    if let Some(max_weight) = risk.and_then(|cfg| cfg.max_single_asset_weight) {
        let required_assets = engine::backtest::required_asset_count_for_max_weight(max_weight);
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

#[derive(Clone, Copy)]
pub(super) struct DecisionAudit<'a> {
    pub(super) note: &'a str,
    pub(super) decision_source: &'a str,
    pub(super) override_reason: &'a str,
    pub(super) override_owner: &'a str,
    pub(super) override_decided_at: &'a str,
}

pub(super) fn build_execution_log_rows(
    rows: &[report::RebalanceInstructionRow],
) -> Vec<report::ExecutionLogRow> {
    rows.iter()
        .map(|row| report::ExecutionLogRow {
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

pub(super) fn merge_execution_backfill(
    expected_rows: &[report::ExecutionLogRow],
    imported_rows: &[report::ExecutionLogRow],
    execution_input_path: &std::path::Path,
) -> anyhow::Result<Vec<report::ExecutionLogRow>> {
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
        let key = format!(
            "{}|{}|{}",
            expected.signal_date, expected.asset, expected.action
        );
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

pub(super) fn actual_weights_from_execution_rows(
    rows: &[report::ExecutionLogRow],
) -> anyhow::Result<HashMap<String, f64>> {
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

pub(super) fn render_manual_override_summary(
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

pub(super) fn render_execution_summary(
    signal_date: NaiveDate,
    execution_rows: &[report::ExecutionLogRow],
    execution_input_path: Option<&std::path::Path>,
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

pub(super) fn build_execution_backfill_result(
    template_rows: &[report::ExecutionLogRow],
    execution_input_path: Option<&std::path::Path>,
    signal_date: NaiveDate,
) -> anyhow::Result<ExecutionBackfillResult> {
    let Some(execution_input_path) = execution_input_path else {
        return Ok(ExecutionBackfillResult {
            rows: template_rows.to_vec(),
            summary: render_execution_summary(signal_date, template_rows, None, None),
            actual_weights: None,
        });
    };

    let imported_rows: Vec<report::ExecutionLogRow> =
        report::read_csv_rows(execution_input_path.to_str().ok_or_else(|| {
            anyhow!(
                "execution_input 路径不是有效 UTF-8：{}",
                execution_input_path.display()
            )
        })?)?;
    let merged_rows =
        merge_execution_backfill(template_rows, &imported_rows, execution_input_path)?;
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
