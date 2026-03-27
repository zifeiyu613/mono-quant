use super::super::*;
use crate::strategy::runtime::is_processed_rotation_strategy;

pub(super) fn run_daily_signal(
    daily_cfg: &config::AppConfig,
    daily_config_path: &str,
) -> anyhow::Result<()> {
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
    if !is_processed_rotation_strategy(&source_cfg.strategy) {
        return Err(anyhow!(
            "daily_signal 目前只支持 processed 轮动策略，当前来源策略为：{}",
            source_cfg.strategy
        ));
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
    let current_weights = with_cash_weight(&snapshot_weights_for_date(
        &result.holdings_trace,
        signal_date,
    ));
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
        DecisionAudit {
            note: &decision.final_note,
            decision_source: &decision.decision_source,
            override_reason: &decision.override_reason,
            override_owner: &decision.override_owner,
            override_decided_at: &decision.override_decided_at,
        },
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
    write_diagnostics(
        &format!("{}/signal_summary.txt", daily_cfg.output_dir),
        &summary,
    )?;

    println!("=== 每日信号摘要 ===");
    println!("来源策略：{}", source_cfg.strategy);
    println!("信号日期：{}", signal_date);
    println!("是否调仓信号日：{}", rebalance_due);
    println!("决策来源：{}", decision.decision_source);
    println!("目标仓位：{}", target_positions_text);
    println!("信号说明：{}", decision.final_note);
    println!("已写入：{}/signal_summary.txt", daily_cfg.output_dir);
    println!(
        "已写入：{}/model_target_positions.csv",
        daily_cfg.output_dir
    );
    println!("已写入：{}/target_positions.csv", daily_cfg.output_dir);
    println!(
        "已写入：{}/rebalance_instructions.csv",
        daily_cfg.output_dir
    );
    println!("已写入：{}/execution_log.csv", daily_cfg.output_dir);
    println!(
        "已写入：{}/manual_override_summary.txt",
        daily_cfg.output_dir
    );
    println!("已写入：{}/execution_summary.txt", daily_cfg.output_dir);
    if execution_backfill.actual_weights.is_some() {
        println!("已写入：{}/actual_positions.csv", daily_cfg.output_dir);
    }
    Ok(())
}
