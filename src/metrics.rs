/// 根据净值曲线计算最大回撤。
pub fn max_drawdown(equity_curve: &[f64]) -> f64 {
    if equity_curve.is_empty() {
        return 0.0;
    }
    let mut peak = equity_curve[0];
    let mut mdd = 0.0;
    for &v in equity_curve {
        if v > peak {
            peak = v;
        }
        let dd = v / peak - 1.0;
        if dd < mdd {
            mdd = dd;
        }
    }
    mdd
}
