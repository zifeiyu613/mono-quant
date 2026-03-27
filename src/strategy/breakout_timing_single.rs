use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 单资产突破择时：当前收盘价突破过去 lookback 个交易日最高收盘价则持有，否则回退到防守资产或空仓。
pub fn select_breakout_timing_single(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    benchmark_asset: &str,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let Some(bars) = asset_maps.get(benchmark_asset) else {
        return Vec::new();
    };
    let Some(current_bar) = bars.get(&dates[i]) else {
        return Vec::new();
    };

    let breakout_level = ((i - lookback)..i)
        .map(|idx| bars.get(&dates[idx]).unwrap().close)
        .fold(f64::MIN, f64::max);

    if current_bar.close >= breakout_level {
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
    fn holds_benchmark_after_breakout() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "hs300".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 102.0),
                bar("2024-01-04", 105.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected = select_breakout_timing_single(&maps, &dates, 3, 3, "hs300", None);

        assert_eq!(selected, vec!["hs300".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_without_breakout() {
        let dates = sample_dates();
        let mut maps = HashMap::new();
        maps.insert(
            "hs300".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 102.0),
                bar("2024-01-04", 99.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.1),
                bar("2024-01-03", 100.2),
                bar("2024-01-04", 100.3),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected =
            select_breakout_timing_single(&maps, &dates, 3, 3, "hs300", Some("dividend"));

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
