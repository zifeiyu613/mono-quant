use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 在给定对齐日期索引上，按回看收益率对资产进行排名。
pub fn rank_assets_by_lookback(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
) -> Vec<(String, f64)> {
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
    ranking
}
