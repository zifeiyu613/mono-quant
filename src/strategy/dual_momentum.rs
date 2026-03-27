use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 双动量选股：先做相对动量排序，再做绝对动量过滤。
pub fn select_dual_momentum_assets(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    top_n: usize,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let mut ranking: Vec<(String, f64)> = asset_maps
        .iter()
        .map(|(name, bars)| {
            let now = bars.get(&dates[i]).unwrap().close;
            let past = bars.get(&dates[i - lookback]).unwrap().close;
            let ret = now / past - 1.0;
            (name.clone(), ret)
        })
        .collect();
    ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let selected: Vec<String> = ranking
        .into_iter()
        .filter(|(_, ret)| *ret >= absolute_momentum_floor)
        .take(top_n.max(1))
        .map(|(name, _)| name)
        .collect();

    if selected.is_empty() {
        if let Some(defensive) = defensive_asset {
            return vec![defensive.to_string()];
        }
    }

    selected
}

