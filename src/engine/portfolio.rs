use std::collections::{HashMap, HashSet};

/// 按持仓市值计算当前持仓与目标持仓之间的换手金额。
pub fn compute_turnover_amount(
    current_values: &HashMap<String, f64>,
    target_values: &HashMap<String, f64>,
) -> f64 {
    let assets: HashSet<String> = current_values
        .keys()
        .chain(target_values.keys())
        .cloned()
        .collect();

    let mut turnover = 0.0;
    for asset in assets {
        let old_v = *current_values.get(&asset).unwrap_or(&0.0);
        let new_v = *target_values.get(&asset).unwrap_or(&0.0);
        turnover += (old_v - new_v).abs();
    }
    turnover / 2.0
}
