use super::BacktestSummary;
use crate::data::Bar;
use crate::metrics::max_drawdown;

/// 运行单资产均线交叉回测，使用收盘到收盘收益和简化交易成本模型。
pub fn run_ma_backtest(
    bars: &[Bar],
    signals: &[i8],
    commission: f64,
    slippage: f64,
    stamp_tax_sell: f64,
) -> (BacktestSummary, Vec<f64>) {
    let mut equity = 1.0;
    let mut curve = vec![equity];
    let mut position = 0.0;
    let mut pending_signal = 0_i8;
    let mut trade_count = 0usize;
    let mut total_cost_paid = 0.0;

    for i in 1..bars.len() {
        if pending_signal == 1 && position == 0.0 {
            let cost = commission + slippage;
            equity *= 1.0 - cost;
            total_cost_paid += cost;
            position = 1.0;
            trade_count += 1;
        } else if pending_signal == -1 && position == 1.0 {
            let cost = commission + slippage + stamp_tax_sell;
            equity *= 1.0 - cost;
            total_cost_paid += cost;
            position = 0.0;
            trade_count += 1;
        }
        pending_signal = 0;

        let daily_ret = bars[i].close / bars[i - 1].close - 1.0;
        equity *= 1.0 + position * daily_ret;
        curve.push(equity);

        if signals[i] != 0 {
            pending_signal = signals[i];
        }
    }

    let summary = BacktestSummary {
        total_return: equity - 1.0,
        max_drawdown: max_drawdown(&curve),
        trade_count,
        total_cost_paid,
        final_equity: equity,
        halted_by_risk: false,
        halt_reason: None,
    };
    (summary, curve)
}
