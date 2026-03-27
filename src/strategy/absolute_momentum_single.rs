use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 单资产绝对动量开关：基准资产满足回看收益门槛则持有，否则回退到防守资产或空仓。
pub fn select_absolute_momentum_single(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    benchmark_asset: &str,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let Some(bars) = asset_maps.get(benchmark_asset) else {
        return Vec::new();
    };
    let Some(now_bar) = bars.get(&dates[i]) else {
        return Vec::new();
    };
    let Some(past_bar) = bars.get(&dates[i - lookback]) else {
        return Vec::new();
    };

    let ret = now_bar.close / past_bar.close - 1.0;
    if ret >= absolute_momentum_floor {
        return vec![benchmark_asset.to_string()];
    }

    if let Some(defensive) = defensive_asset {
        if defensive != benchmark_asset && asset_maps.contains_key(defensive) {
            return vec![defensive.to_string()];
        }
    }

    Vec::new()
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

    fn sample_maps() -> HashMap<String, HashMap<NaiveDate, Bar>> {
        let mut maps = HashMap::new();
        let benchmark = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 96.0)];
        let defensive = vec![bar("2024-01-01", 100.0), bar("2024-01-02", 101.0)];
        maps.insert(
            "hs300".to_string(),
            benchmark
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            defensive
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps
    }

    #[test]
    fn holds_benchmark_when_absolute_momentum_passes() {
        let mut maps = sample_maps();
        maps.insert(
            "hs300".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 103.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];

        let selected =
            select_absolute_momentum_single(&maps, &dates, 1, 1, "hs300", 0.0, Some("dividend"));

        assert_eq!(selected, vec!["hs300".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_absolute_momentum_fails() {
        let maps = sample_maps();
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];

        let selected =
            select_absolute_momentum_single(&maps, &dates, 1, 1, "hs300", 0.0, Some("dividend"));

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
