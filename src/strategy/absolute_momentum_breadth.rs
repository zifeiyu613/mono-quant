use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 多资产绝对动量广度：持有所有回看收益不低于门槛的风险资产，否则回退到防守资产或空仓。
pub fn select_absolute_momentum_breadth(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    absolute_momentum_floor: f64,
    defensive_asset: Option<&str>,
) -> Vec<String> {
    let defensive_name = defensive_asset.unwrap_or_default();
    let mut selected: Vec<(String, f64)> = asset_maps
        .iter()
        .filter(|(name, _)| name.as_str() != defensive_name)
        .map(|(name, bars)| {
            let now = bars.get(&dates[i]).unwrap().close;
            let past = bars.get(&dates[i - lookback]).unwrap().close;
            let ret = now / past - 1.0;
            (name.clone(), ret)
        })
        .filter(|(_, ret)| *ret >= absolute_momentum_floor)
        .collect();
    selected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    if selected.is_empty() {
        if let Some(defensive) = defensive_asset {
            return vec![defensive.to_string()];
        }
        return Vec::new();
    }

    selected.into_iter().map(|(name, _)| name).collect()
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
    fn selects_all_assets_above_floor() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 103.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "b".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 101.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 100.5)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected =
            select_absolute_momentum_breadth(&maps, &dates, 1, 1, 0.01, Some("dividend"));

        assert_eq!(selected, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn falls_back_to_defensive_when_no_asset_passes_floor() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "a".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 99.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 100.2)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected = select_absolute_momentum_breadth(&maps, &dates, 1, 1, 0.0, Some("dividend"));

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
