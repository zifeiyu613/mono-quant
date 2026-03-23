mod config;
mod data;
mod engine;
mod metrics;
mod report;
mod strategy;

use anyhow::{anyhow, Context};
use config::load_config;
use report::{
    ensure_output_dir, write_contributions, write_diagnostics, write_equity_curve,
    write_experiment_index, write_holdings_trace, write_rebalance_log, EquityRow,
    ExperimentIndexRow,
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

/// Print a unified informational log line for long-running research workflows.
fn log_info(message: &str) {
    println!("[INFO] {}", message);
}

/// Parse the `--config` CLI argument and return the JSON config path.
fn parse_config_path() -> anyhow::Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 || args[1] != "--config" {
        return Err(anyhow!("usage: cargo run -- --config <path-to-json>"));
    }
    Ok(args[2].clone())
}

/// Resolve the processed alignment manifest path from the first processed asset path.
fn infer_manifest_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("alignment_manifest.json"))
    })
}

/// Resolve the processed summary JSON path from the first processed asset path.
fn infer_summary_json_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("processed_summary.json"))
    })
}

/// Resolve the processed summary TXT path from the first processed asset path.
fn infer_summary_txt_path(asset_files: &HashMap<String, String>) -> Option<PathBuf> {
    asset_files.values().next().and_then(|path| {
        let p = Path::new(path);
        p.parent().map(|parent| parent.join("processed_summary.txt"))
    })
}

/// Check whether all multi-asset inputs are coming from the processed layer.
fn all_assets_use_processed_layer(asset_files: &HashMap<String, String>) -> bool {
    asset_files
        .values()
        .all(|path| path.contains("data/processed/"))
}

/// Validate that processed-layer files exist before running multi-asset research.
fn validate_processed_inputs(asset_files: &HashMap<String, String>) -> anyhow::Result<()> {
    if !all_assets_use_processed_layer(asset_files) {
        return Err(anyhow!(
            "multi-asset strategies now expect processed-layer inputs under data/processed/. Please update config asset_files first."
        ));
    }

    for (name, path) in asset_files {
        if !Path::new(path).exists() {
            return Err(anyhow!(
                "missing processed asset file for {}: {}\nPlease run: ./scripts/prepare_data.sh scripts/fetch_config.json",
                name,
                path
            ));
        }
    }

    let manifest_path = infer_manifest_path(asset_files)
        .ok_or_else(|| anyhow!("failed to infer processed alignment manifest path from asset_files"))?;
    if !manifest_path.exists() {
        return Err(anyhow!(
            "missing processed alignment manifest: {}\nPlease run: ./scripts/prepare_data.sh scripts/fetch_config.json",
            manifest_path.display()
        ));
    }

    let summary_json_path = infer_summary_json_path(asset_files)
        .ok_or_else(|| anyhow!("failed to infer processed summary json path from asset_files"))?;
    if !summary_json_path.exists() {
        return Err(anyhow!(
            "missing processed summary json: {}\nPlease run: ./scripts/prepare_data.sh scripts/fetch_config.json",
            summary_json_path.display()
        ));
    }

    let summary_txt_path = infer_summary_txt_path(asset_files)
        .ok_or_else(|| anyhow!("failed to infer processed summary txt path from asset_files"))?;
    if !summary_txt_path.exists() {
        return Err(anyhow!(
            "missing processed summary txt: {}\nPlease run: ./scripts/prepare_data.sh scripts/fetch_config.json",
            summary_txt_path.display()
        ));
    }

    Ok(())
}

/// Print a short processed-layer summary snippet before a multi-asset run starts.
fn log_processed_summary(asset_files: &HashMap<String, String>) -> anyhow::Result<()> {
    let summary_txt_path = infer_summary_txt_path(asset_files)
        .ok_or_else(|| anyhow!("failed to infer processed summary txt path from asset_files"))?;
    let content = fs::read_to_string(&summary_txt_path)
        .with_context(|| format!("failed to read processed summary: {}", summary_txt_path.display()))?;

    println!("[INFO] processed data summary: {}", summary_txt_path.display());
    for line in content.lines().take(12) {
        println!("[INFO] {}", line);
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path()?;
    log_info(&format!("loading config from {}", config_path));
    let cfg = load_config(&config_path)?;

    println!("experiment: {}", cfg.experiment_name);
    println!("strategy: {}", cfg.strategy);

    match cfg.strategy.as_str() {
        "ma_single" => {
            ensure_output_dir(&cfg.output_dir)?;
            let data_file = cfg
                .data_file
                .clone()
                .ok_or_else(|| anyhow!("data_file is required for ma_single"))?;
            let fast = cfg.fast.ok_or_else(|| anyhow!("fast is required for ma_single"))?;
            let slow = cfg.slow.ok_or_else(|| anyhow!("slow is required for ma_single"))?;
            let commission = cfg
                .commission
                .ok_or_else(|| anyhow!("commission is required for ma_single"))?;
            let slippage = cfg
                .slippage
                .ok_or_else(|| anyhow!("slippage is required for ma_single"))?;
            let stamp_tax_sell = cfg.stamp_tax_sell.unwrap_or(0.0);

            log_info(&format!("reading single-asset data from {}", data_file));
            let bars = data::read_bars(&data_file)?;
            if bars.len() <= slow {
                return Err(anyhow!(
                    "not enough bars: {} rows, need more than slow window {}",
                    bars.len(),
                    slow
                ));
            }

            println!(
                "data range: {} -> {} ({} bars)",
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
                "=== Diagnostics ===\nexperiment_name: {}\nstrategy: {}\ndata_file: {}\nfast: {}\nslow: {}\ncommission: {}\nslippage: {}\nstamp_tax_sell: {}\nbar_count: {}\nstart_date: {}\nend_date: {}\ntotal_return: {:.2}%\nmax_drawdown: {:.2}%\ntrade_count: {}\ntotal_cost_paid: {:.4}\nfinal_equity: {:.4}\n",
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

            println!("=== backtest summary ===");
            println!("total return: {:.2}%", summary.total_return * 100.0);
            println!("max drawdown: {:.2}%", summary.max_drawdown * 100.0);
            println!("trade count: {}", summary.trade_count);
            println!("total cost paid: {:.4}", summary.total_cost_paid);
            println!("final equity: {:.4}", summary.final_equity);
        }
        "momentum_topn" => {
            ensure_output_dir(&cfg.output_dir)?;
            let asset_files = cfg
                .asset_files
                .clone()
                .ok_or_else(|| anyhow!("asset_files is required for momentum_topn"))?;
            let lookback = cfg
                .lookback
                .ok_or_else(|| anyhow!("lookback is required for momentum_topn"))?;
            let rebalance_freq = cfg
                .rebalance_freq
                .ok_or_else(|| anyhow!("rebalance_freq is required for momentum_topn"))?;
            let top_n = cfg.top_n.ok_or_else(|| anyhow!("top_n is required for momentum_topn"))?;
            let commission = cfg
                .commission
                .ok_or_else(|| anyhow!("commission is required for momentum_topn"))?;
            let slippage = cfg
                .slippage
                .ok_or_else(|| anyhow!("slippage is required for momentum_topn"))?;

            log_info("validating processed-layer inputs for momentum_topn");
            validate_processed_inputs(&asset_files)?;
            if let Some(manifest_path) = infer_manifest_path(&asset_files) {
                log_info(&format!("using processed alignment manifest: {}", manifest_path.display()));
            }
            if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
                log_info(&format!("using processed summary json: {}", summary_json_path.display()));
            }
            log_processed_summary(&asset_files)?;

            log_info("loading multi-asset data for momentum_topn");
            let mut asset_maps = HashMap::new();
            for (name, path) in &asset_files {
                log_info(&format!("loading asset {} from {}", name, path));
                asset_maps.insert(
                    name.clone(),
                    data::read_bars_map(path)
                        .with_context(|| format!("failed reading asset {} from {}", name, path))?,
                );
            }

            let dates = data::intersect_dates(&asset_maps);
            if dates.len() <= lookback + 1 {
                return Err(anyhow!(
                    "not enough aligned dates for momentum_topn, aligned bars = {}",
                    dates.len()
                ));
            }

            println!(
                "aligned date range: {} -> {} ({} aligned bars)",
                dates.first().unwrap(),
                dates.last().unwrap(),
                dates.len()
            );
            log_info("running momentum_topn backtest");
            let result = engine::backtest::run_momentum_topn_backtest(
                &asset_maps,
                lookback,
                rebalance_freq,
                top_n,
                commission,
                slippage,
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

            let manifest_path = infer_manifest_path(&asset_files).unwrap();
            let summary_json_path = infer_summary_json_path(&asset_files).unwrap();
            let summary_txt_path = infer_summary_txt_path(&asset_files).unwrap();
            let diagnostics = format!(
                "=== Diagnostics ===\nexperiment_name: {}\nstrategy: {}\ndata_layer: processed\nprocessed_manifest: {}\nprocessed_summary_json: {}\nprocessed_summary_txt: {}\nassets: {}\nlookback: {}\nrebalance_freq: {}\ntop_n: {}\ncommission: {}\nslippage: {}\naligned_bar_count: {}\nstart_date: {}\nend_date: {}\ntotal_return: {:.2}%\nmax_drawdown: {:.2}%\nrebalance_count: {}\ntotal_cost_paid: {:.6}\nfinal_equity: {:.4}\ntop_contributor: {:?}\nworst_contributor: {:?}\noutput_files:\n- equity_curve.csv\n- rebalance_log.csv\n- holdings_trace.csv\n- asset_contribution.csv\n",
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
                result.top_contributor,
                result.worst_contributor,
            );
            write_diagnostics(&format!("{}/diagnostics.txt", cfg.output_dir), &diagnostics)?;

            println!("=== momentum summary ===");
            println!("total return: {:.2}%", result.summary.total_return * 100.0);
            println!("max drawdown: {:.2}%", result.summary.max_drawdown * 100.0);
            println!("rebalance count: {}", result.summary.trade_count);
            println!("total cost paid: {:.6}", result.summary.total_cost_paid);
            println!("final equity: {:.4}", result.summary.final_equity);
            println!("top contributor: {:?}", result.top_contributor);
            println!("worst contributor: {:?}", result.worst_contributor);
        }
        "momentum_batch" => {
            let asset_files = cfg
                .asset_files
                .clone()
                .ok_or_else(|| anyhow!("asset_files is required for momentum_batch"))?;
            let lookbacks = cfg
                .lookbacks
                .clone()
                .ok_or_else(|| anyhow!("lookbacks is required for momentum_batch"))?;
            let rebalance_freqs = cfg
                .rebalance_freqs
                .clone()
                .ok_or_else(|| anyhow!("rebalance_freqs is required for momentum_batch"))?;
            let top_ns = cfg.top_ns.clone().ok_or_else(|| anyhow!("top_ns is required for momentum_batch"))?;
            let unit_costs = cfg
                .unit_costs
                .clone()
                .ok_or_else(|| anyhow!("unit_costs is required for momentum_batch"))?;

            log_info("validating processed-layer inputs for momentum_batch");
            validate_processed_inputs(&asset_files)?;
            if let Some(manifest_path) = infer_manifest_path(&asset_files) {
                log_info(&format!("using processed alignment manifest: {}", manifest_path.display()));
            }
            if let Some(summary_json_path) = infer_summary_json_path(&asset_files) {
                log_info(&format!("using processed summary json: {}", summary_json_path.display()));
            }
            log_processed_summary(&asset_files)?;

            ensure_output_dir(&cfg.output_dir)?;
            fs::create_dir_all(format!("{}/experiments", cfg.output_dir))?;
            fs::copy(&config_path, format!("{}/config_snapshot.json", cfg.output_dir))?;
            log_info("loading multi-asset data for batch experiments");

            let mut asset_maps = HashMap::new();
            for (name, path) in &asset_files {
                log_info(&format!("loading asset {} from {}", name, path));
                asset_maps.insert(
                    name.clone(),
                    data::read_bars_map(path)
                        .with_context(|| format!("failed reading asset {} from {}", name, path))?,
                );
            }

            let mut rows: Vec<BatchResultRow> = Vec::new();
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
                                "running {} with lookback={}, rebalance_freq={}, top_n={}, unit_cost={}",
                                exp_id, lookback, rebalance_freq, top_n, unit_cost
                            ));

                            let result = engine::backtest::run_momentum_topn_backtest(
                                &asset_maps,
                                lookback,
                                *rebalance_freq,
                                *top_n,
                                unit_cost / 2.0,
                                unit_cost / 2.0,
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

                            let diag = format!(
                                "experiment_id: {}\ndata_layer: processed\nlookback: {}\nrebalance_freq: {}\ntop_n: {}\nunit_cost: {}\ntotal_return: {:.2}%\nmax_drawdown: {:.2}%\ntrade_count: {}\ntotal_cost_paid: {:.6}\nfinal_equity: {:.4}\ntop_contributor: {:?}\nworst_contributor: {:?}\n",
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
                                result.top_contributor,
                                result.worst_contributor,
                            );
                            write_diagnostics(&format!("{}/diagnostics.txt", exp_dir), &diag)?;

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
                                experiment_id: exp_id.clone(),
                                lookback,
                                rebalance_freq: *rebalance_freq,
                                top_n: *top_n,
                                unit_cost: *unit_cost,
                                total_return: result.summary.total_return,
                                max_drawdown: result.summary.max_drawdown,
                                trade_count: result.summary.trade_count,
                                total_cost_paid: result.summary.total_cost_paid,
                                final_equity: result.summary.final_equity,
                                top_contributor: top_contributor.clone(),
                                worst_contributor: worst_contributor.clone(),
                                output_dir: exp_dir.clone(),
                            });

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
            let mut wtr = csv::Writer::from_path(format!("{}/batch_results.csv", cfg.output_dir))?;
            for row in &rows {
                wtr.serialize(row)?;
            }
            wtr.flush()?;
            write_experiment_index(&format!("{}/experiment_index.csv", cfg.output_dir), &index_rows)?;

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

            let summary = format!(
                "=== Batch Summary ===\nexperiment_name: {}\nstrategy: {}\ndata_layer: processed\nprocessed_manifest: {}\nprocessed_summary_json: {}\nprocessed_summary_txt: {}\nexperiment_count: {}\ntop_3_by_return: {}\nlowest_drawdown_candidate: {}\nconfig_snapshot: {}/config_snapshot.json\nresults: {}/batch_results.csv\nindex: {}/experiment_index.csv\n",
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

            let stage_report = format!(
                "=== Stage Report ===\nExperiment Name: {}\nCurrent Stage: v1.4 processed-summary workflow\nKey Deliverables:\n1. processed_summary.json / processed_summary.txt 已生成。\n2. 多资产回测启动前会读取 processed 摘要并打印。\n3. 研究诊断已记录 processed manifest 与 summary 路径。\n4. 数据准备层与回测层的衔接更完整。\n\nSuggested Next Actions:\n- 给 processed 层加入异常样本统计。\n- 在 batch 输出里记录数据准备时间戳。\n- 为单资产回测增加 processed 可选模式。\n",
                cfg.experiment_name,
            );
            write_diagnostics(&format!("{}/stage_report.txt", cfg.output_dir), &stage_report)?;

            println!("=== batch summary ===");
            println!("experiments: {}", rows.len());
            println!("written: {}/batch_results.csv", cfg.output_dir);
            println!("written: {}/experiment_index.csv", cfg.output_dir);
            println!("written: {}/batch_summary.txt", cfg.output_dir);
            println!("written: {}/stage_report.txt", cfg.output_dir);
            println!("written: {}/config_snapshot.json", cfg.output_dir);
        }
        other => return Err(anyhow!("unsupported strategy: {}", other)),
    }

    Ok(())
}
