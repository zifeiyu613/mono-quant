use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 按回看窗口的日收益波动率从低到高排序，返回低波资产列表。
pub fn rank_assets_by_low_volatility(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    defensive_asset: Option<&str>,
) -> Vec<(String, f64)> {
    let defensive_name = defensive_asset.unwrap_or_default();
    let mut ranking: Vec<(String, f64)> = asset_maps
        .iter()
        .filter(|(name, _)| name.as_str() != defensive_name)
        .filter_map(|(name, bars)| {
            let mut mean = 0.0;
            let mut m2 = 0.0;
            let mut count = 0usize;
            for idx in (i - lookback + 1)..=i {
                let today = bars.get(&dates[idx])?.close;
                let prev = bars.get(&dates[idx - 1])?.close;
                let value = today / prev - 1.0;

                count += 1;
                let delta = value - mean;
                mean += delta / count as f64;
                let delta2 = value - mean;
                m2 += delta * delta2;
            }

            let variance = if count > 0 { m2 / count as f64 } else { 0.0 };
            Some((name.clone(), variance.sqrt()))
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
    fn ranks_lowest_volatility_first() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-04", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "stable".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.5),
                bar("2024-01-03", 101.0),
                bar("2024-01-04", 101.5),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "noisy".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 105.0),
                bar("2024-01-03", 99.0),
                bar("2024-01-04", 104.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let ranking = rank_assets_by_low_volatility(&maps, &dates, 3, 3, None);

        assert_eq!(ranking.first().map(|item| item.0.as_str()), Some("stable"));
    }
}
