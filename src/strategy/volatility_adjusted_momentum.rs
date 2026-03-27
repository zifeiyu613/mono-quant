use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 按“回看收益 / 回看波动”排序，返回分数从高到低的资产列表。
pub fn rank_assets_by_volatility_adjusted_momentum(
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

            let mut daily_returns = Vec::new();
            for idx in (i - lookback + 1)..=i {
                let today = bars.get(&dates[idx]).unwrap().close;
                let prev = bars.get(&dates[idx - 1]).unwrap().close;
                daily_returns.push(today / prev - 1.0);
            }

            let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
            let variance = daily_returns
                .iter()
                .map(|value| {
                    let diff = value - mean;
                    diff * diff
                })
                .sum::<f64>()
                / daily_returns.len() as f64;
            let volatility = variance.sqrt().max(1e-12);
            (name.clone(), ret / volatility)
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
    fn prefers_smoother_trend_when_returns_are_similar() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-04", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "smooth".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 102.0),
                bar("2024-01-03", 103.0),
                bar("2024-01-04", 104.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "noisy".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 110.0),
                bar("2024-01-03", 95.0),
                bar("2024-01-04", 104.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let ranking = rank_assets_by_volatility_adjusted_momentum(&maps, &dates, 3, 3);

        assert_eq!(ranking.first().map(|item| item.0.as_str()), Some("smooth"));
    }
}
