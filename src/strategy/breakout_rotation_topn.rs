use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 多资产突破轮动：从触发突破的资产中按回看收益排序，持有最强的前 N 个。
pub fn select_breakout_rotation_topn(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    top_n: usize,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let defensive_name = defensive_asset.unwrap_or_default();
    let mut ranking: Vec<(String, f64)> = asset_maps
        .iter()
        .filter(|(name, _)| name.as_str() != defensive_name)
        .filter_map(|(name, bars)| {
            let current_close = bars.get(&dates[i])?.close;
            let breakout_level = ((i - lookback)..i)
                .map(|idx| bars.get(&dates[idx]).unwrap().close)
                .fold(f64::MIN, f64::max);
            if current_close < breakout_level {
                return None;
            }
            let past_close = bars.get(&dates[i - lookback]).unwrap().close;
            let lookback_return = current_close / past_close - 1.0;
            Some((name.clone(), lookback_return))
        })
        .collect();
    ranking.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let selected: Vec<String> = ranking
        .into_iter()
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
    fn selects_breakout_assets_ranked_by_return() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 105.0),
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 100.5),
                bar("2024-01-03", 102.0),
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
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected = select_breakout_rotation_topn(&maps, &dates, 2, 2, 1, Some("dividend"));

        assert_eq!(selected, vec!["a".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_no_breakout_asset() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-03", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![
                bar("2024-01-01", 100.0),
                bar("2024-01-02", 101.0),
                bar("2024-01-03", 100.5),
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
            ]
            .into_iter()
            .map(|item| (item.date, item))
            .collect(),
        );

        let selected = select_breakout_rotation_topn(&maps, &dates, 2, 2, 1, Some("dividend"));

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
