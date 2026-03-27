use crate::config;
use crate::data;
use crate::engine;
use crate::modes;
use crate::report::{ensure_output_dir, write_diagnostics, write_equity_curve, EquityRow};
use crate::strategy;
use crate::strategy::runtime::{is_processed_rotation_strategy, RotationStrategySpec};
use anyhow::anyhow;

pub(super) fn run_from_config(cfg: &config::AppConfig, config_path: &str) -> anyhow::Result<()> {
    println!("实验名称：{}", cfg.experiment_name);
    println!("策略类型：{}", cfg.strategy);

    match cfg.strategy.as_str() {
        "ma_single" => run_ma_single(cfg),
        strategy_name if is_processed_rotation_strategy(strategy_name) => {
            let strategy_spec = RotationStrategySpec::from_app_config(cfg)?;
            let _ = super::run_processed_rotation_strategy(cfg, &strategy_spec)?;
            Ok(())
        }
        "strategy_compare" => {
            modes::run_strategy_compare(cfg, config_path)?;
            Ok(())
        }
        "daily_signal" => {
            modes::run_daily_signal(cfg, config_path)?;
            Ok(())
        }
        "momentum_batch" => {
            modes::run_momentum_batch(cfg, config_path)?;
            Ok(())
        }
        other => Err(anyhow!("不支持的策略类型：{}", other)),
    }
}

fn run_ma_single(cfg: &config::AppConfig) -> anyhow::Result<()> {
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

    super::log_info(&format!("正在读取单资产数据：{}", data_file));
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
    Ok(())
}
