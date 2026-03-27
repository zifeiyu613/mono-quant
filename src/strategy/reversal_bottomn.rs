use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 在给定对齐日期索引上，按回看收益率从低到高排序，构造反转策略候选列表。
pub fn rank_assets_by_reversal(
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
    ranking.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    ranking
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(date: &str, close: f64) -> Bar {
        Bar {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            open: close,
            close,
        }
    }

    #[test]
    fn ranks_weakest_asset_first() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "weak".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 90.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "strong".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 110.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let ranking = rank_assets_by_reversal(&maps, &dates, 1, 1);

        assert_eq!(ranking.first().map(|item| item.0.as_str()), Some("weak"));
    }
}
