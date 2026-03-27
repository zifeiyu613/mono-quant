mod config;
mod data;
mod engine;
mod entry_dispatch;
mod main_support;
mod metrics;
mod modes;
mod report;
mod research;
mod strategy;

use anyhow::{anyhow, Context};
use chrono::NaiveDate;
use config::load_config;
use main_support::*;
#[cfg(test)]
use report::ExecutionLogRow;
use report::{
    ensure_output_dir, write_contributions, write_csv_rows, write_diagnostics, write_equity_curve,
    write_experiment_index, write_holdings_trace, write_hypothesis_assessments,
    write_rebalance_log, EquityRow, ExperimentIndexRow, RebalanceInstructionRow, TargetPositionRow,
};
use research::{
    apply_manual_override, assess_hypotheses, assessments_to_rows, build_evidence_summary,
    build_sample_split_plan, build_walk_forward_windows, cost_sensitivity_detail_rows,
    decide_research_state, render_governance_summary, render_research_decision,
    render_research_plan, render_walk_forward_plan, summarize_cost_sensitivity,
    summarize_walk_forward_assessments, walk_forward_detail_rows, EvidenceSummaryInput,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use strategy::runtime::RotationStrategySpec;

/// 根据第一个 processed 资产文件路径推导对齐清单文件路径。
fn infer_manifest_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent()
            .map(|parent| parent.join("alignment_manifest.json"))
    })
}

/// 根据第一个 processed 资产文件路径推导 processed 摘要 JSON 路径。
fn infer_summary_json_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent()
            .map(|parent| parent.join("processed_summary.json"))
    })
}

/// 根据第一个 processed 资产文件路径推导 processed 摘要 TXT 路径。
fn infer_summary_txt_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent()
            .map(|parent| parent.join("processed_summary.txt"))
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
    audit: DecisionAudit<'_>,
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
            decision_source: audit.decision_source.to_string(),
            override_reason: audit.override_reason.to_string(),
            override_owner: audit.override_owner.to_string(),
            override_decided_at: audit.override_decided_at.to_string(),
            note: audit.note.to_string(),
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
            decision_source: audit.decision_source.to_string(),
            override_reason: audit.override_reason.to_string(),
            override_owner: audit.override_owner.to_string(),
            override_decided_at: audit.override_decided_at.to_string(),
            note: audit.note.to_string(),
        });
    }

    rows
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
    write_equity_curve(
        &format!("{}/equity_curve.csv", cfg.output_dir),
        &equity_rows,
    )?;
    write_rebalance_log(
        &format!("{}/rebalance_log.csv", cfg.output_dir),
        &result.rebalances,
    )?;
    write_holdings_trace(
        &format!("{}/holdings_trace.csv", cfg.output_dir),
        &result.holdings_trace,
    )?;
    write_contributions(
        &format!("{}/asset_contribution.csv", cfg.output_dir),
        &result.contributions,
    )?;
    if !result.risk_events.is_empty() {
        write_csv_rows(
            &format!("{}/risk_events.csv", cfg.output_dir),
            &result.risk_events,
        )?;
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
    let manifest_path = infer_manifest_path(asset_files).unwrap();
    let summary_json_path = infer_summary_json_path(asset_files).unwrap();
    let summary_txt_path = infer_summary_txt_path(asset_files).unwrap();
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
        halt_reason: result
            .summary
            .halt_reason
            .unwrap_or_else(|| "未触发".to_string()),
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

fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path()?;
    log_info(&format!("正在加载配置：{}", config_path));
    let cfg = load_config(&config_path)?;
    entry_dispatch::run_from_config(&cfg, &config_path)
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
        items
            .iter()
            .map(|(asset, weight)| ((*asset).to_string(), *weight))
            .collect()
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
            DecisionAudit {
                note: "test",
                decision_source: "model",
                override_reason: "",
                override_owner: "",
                override_decided_at: "",
            },
        );

        assert!(rows
            .iter()
            .any(|row| row.asset == "hs300" && row.action == "SELL"));
        assert!(rows
            .iter()
            .any(|row| row.asset == "cyb" && row.action == "BUY"));
        assert!(rows
            .iter()
            .any(|row| row.asset == "dividend" && row.action == "BUY"));
    }
}
