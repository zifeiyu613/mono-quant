use crate::data::Bar;
use crate::strategy::ma_cross::moving_average;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 单资产均线择时：快线高于慢线则持有基准资产，否则回退到防守资产或空仓。
pub fn select_ma_timing_single(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    fast: usize,
    slow: usize,
    benchmark_asset: &str,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let Some(bars) = asset_maps.get(benchmark_asset) else {
        return Vec::new();
    };

    let closes = dates
        .iter()
        .take(i + 1)
        .filter_map(|date| bars.get(date).map(|bar| bar.close))
        .collect::<Vec<_>>();
    let fast_ma = moving_average(&closes, fast);
    let slow_ma = moving_average(&closes, slow);

    let Some(fast_now) = fast_ma.last().and_then(|value| *value) else {
        return Vec::new();
    };
    let Some(slow_now) = slow_ma.last().and_then(|value| *value) else {
        return Vec::new();
    };

    if fast_now > slow_now {
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

    fn sample_dates() -> Vec<NaiveDate> {
        vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-04", "%Y-%m-%d").unwrap(),
        ]
    }

    #[test]
    fn holds_benchmark_when_fast_ma_is_above_slow_ma() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "hs300".to_string(),
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

        let selected = select_ma_timing_single(&maps, &dates, 3, 2, 3, "hs300", None);

        assert_eq!(selected, vec!["hs300".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_fast_ma_is_not_above_slow_ma() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "hs300".to_string(),
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
        maps.insert(
            "dividend".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.2),
                bar("2024-01-03", 100.4),
                bar("2024-01-04", 100.6),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected =
            select_ma_timing_single(&maps, &dates, 3, 2, 3, "hs300", Some("dividend"));

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
