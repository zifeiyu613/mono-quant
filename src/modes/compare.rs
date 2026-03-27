use super::super::*;
use crate::strategy::runtime::is_processed_rotation_strategy;

pub(super) fn run_strategy_compare(
    compare_cfg: &config::AppConfig,
    compare_config_path: &str,
) -> anyhow::Result<()> {
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
        log_info(&format!(
            "正在加载对比策略配置：{}",
            resolved_path.display()
        ));
        let sub_cfg = load_config(&resolved_str)
            .with_context(|| format!("读取策略配置失败：{}", resolved_path.display()))?;
        if !is_processed_rotation_strategy(&sub_cfg.strategy) {
            return Err(anyhow!(
                "strategy_compare 目前只支持 processed-first 策略配置，当前为：{}",
                sub_cfg.strategy
            ));
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
