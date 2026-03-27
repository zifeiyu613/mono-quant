use super::super::*;

pub(super) fn run_momentum_batch(cfg: &config::AppConfig, config_path: &str) -> anyhow::Result<()> {
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
    let top_ns = cfg
        .top_ns
        .clone()
        .ok_or_else(|| anyhow!("momentum_batch 需要提供 top_ns"))?;
    let unit_costs = cfg
        .unit_costs
        .clone()
        .ok_or_else(|| anyhow!("momentum_batch 需要提供 unit_costs"))?;

    log_info("正在校验 momentum_batch 的 processed 输入");
    validate_processed_inputs(&asset_files)?;
    if let Some(manifest_path) = infer_manifest_path(&asset_files) {
        log_info(&format!(
            "使用 processed 对齐清单：{}",
            manifest_path.display()
        ));
    }
    if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
        log_info(&format!(
            "使用 processed 摘要 JSON：{}",
            summary_json_path.display()
        ));
    }
    log_processed_summary(&asset_files)?;

    ensure_output_dir(&cfg.output_dir)?;
    fs::create_dir_all(format!("{}/experiments", cfg.output_dir))?;
    fs::copy(
        config_path,
        format!("{}/config_snapshot.json", cfg.output_dir),
    )?;
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
                    write_rebalance_log(
                        &format!("{}/rebalance_log.csv", exp_dir),
                        &result.rebalances,
                    )?;
                    write_holdings_trace(
                        &format!("{}/holdings_trace.csv", exp_dir),
                        &result.holdings_trace,
                    )?;
                    write_contributions(
                        &format!("{}/asset_contribution.csv", exp_dir),
                        &result.contributions,
                    )?;
                    if !result.risk_events.is_empty() {
                        write_csv_rows(
                            &format!("{}/risk_events.csv", exp_dir),
                            &result.risk_events,
                        )?;
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
                        result
                            .summary
                            .halt_reason
                            .clone()
                            .unwrap_or_else(|| "未触发".to_string()),
                        result.top_contributor,
                        result.worst_contributor,
                    );
                    write_diagnostics(&format!("{}/diagnostics.txt", exp_dir), &diag)?;
                    let batch_spec = BatchRunSpec {
                        exp_id: &exp_id,
                        exp_dir: &exp_dir,
                        lookback,
                        rebalance_freq: *rebalance_freq,
                        top_n: *top_n,
                        unit_cost: *unit_cost,
                    };

                    push_batch_result_row(&mut rows, &batch_spec, &result);

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
                            push_batch_result_row(&mut in_sample_rows, &batch_spec, &scoped_result);
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
                                &batch_spec,
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
                                    &batch_spec,
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
                        halt_reason: result.summary.halt_reason.clone().unwrap_or_default(),
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
    write_experiment_index(
        &format!("{}/experiment_index.csv", cfg.output_dir),
        &index_rows,
    )?;
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
            Some(assess_hypotheses(
                research_cfg,
                &to_batch_row_views(&in_sample_rows),
            ))
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
        let cost_sensitivity_detail = cost_sensitivity_detail_rows(research_cfg, &full_row_views);
        let cost_sensitivity_summary =
            summarize_cost_sensitivity(research_cfg, &cost_sensitivity_detail);
        let evidence_summary = build_evidence_summary(
            research_cfg,
            EvidenceSummaryInput {
                full_assessments: &full_assessments,
                in_sample_assessments: in_sample_assessments.as_deref(),
                out_of_sample_assessments: out_sample_assessments.as_deref(),
                walk_forward_summaries: &walk_forward_summary,
                cost_summaries: &cost_sensitivity_summary,
                data_start: aligned_dates.first().copied(),
                data_end: aligned_dates.last().copied(),
            },
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
        let full_assessments = assess_hypotheses(research_cfg, &to_batch_row_views(&rows));
        let in_sample_assessments = if sample_split_plan.is_some() {
            Some(assess_hypotheses(
                research_cfg,
                &to_batch_row_views(&in_sample_rows),
            ))
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
    write_diagnostics(
        &format!("{}/stage_report.txt", cfg.output_dir),
        &stage_report,
    )?;

    println!("=== 批量实验摘要 ===");
    println!("实验数量：{}", rows.len());
    println!("已写入：{}/batch_results.csv", cfg.output_dir);
    println!("已写入：{}/experiment_index.csv", cfg.output_dir);
    println!("已写入：{}/batch_summary.txt", cfg.output_dir);
    println!("已写入：{}/stage_report.txt", cfg.output_dir);
    println!("已写入：{}/config_snapshot.json", cfg.output_dir);

    Ok(())
}
