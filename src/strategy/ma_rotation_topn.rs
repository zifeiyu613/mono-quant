use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

fn moving_average_at(
    bars: &HashMap<NaiveDate, Bar>,
    dates: &[NaiveDate],
    i: usize,
    window: usize,
) -> Option<f64> {
    if window == 0 || i + 1 < window {
        return None;
    }
    let mut sum = 0.0;
    let start = i + 1 - window;
    for date in dates.iter().take(i + 1).skip(start) {
        sum += bars.get(date)?.close;
    }
    Some(sum / window as f64)
}

/// 先过滤快线高于慢线的资产，再按回看收益率排序，构造均线过滤后的 TopN 轮动候选。
pub fn rank_assets_by_ma_rotation(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    fast: usize,
    slow: usize,
    lookback: usize,
    defensive_asset: Option<&str>,
) -> Vec<(String, f64)> {
    let defensive_name = defensive_asset.unwrap_or_default();
    let mut ranking: Vec<(String, f64)> = asset_maps
        .iter()
        .filter(|(name, _)| name.as_str() != defensive_name)
        .filter_map(|(name, bars)| {
            let fast_now = moving_average_at(bars, dates, i, fast)?;
            let slow_now = moving_average_at(bars, dates, i, slow)?;
            if fast_now <= slow_now {
                return None;
            }

            let now = bars.get(&dates[i])?.close;
            let past = bars.get(&dates[i - lookback])?.close;
            let ret = now / past - 1.0;
            Some((name.clone(), ret))
        })
        .collect();
    ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
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
    fn only_ranks_assets_above_ma_filter() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-04", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "trend".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 103.0),
                bar("2024-01-04", 105.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "weak".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 99.0),
                bar("2024-01-03", 98.0),
                bar("2024-01-04", 97.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let ranking = rank_assets_by_ma_rotation(&maps, &dates, 3, 2, 3, 3, None);

        assert_eq!(ranking.len(), 1);
        assert_eq!(ranking[0].0, "trend");
        assert!((ranking[0].1 - 0.05).abs() < 1e-9);
    }
}
