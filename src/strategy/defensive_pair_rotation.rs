use crate::data::Bar;
use crate::strategy::relative_strength_pair::select_relative_strength_pair;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 防守资产对轮动：在两类防守资产中比较回看收益，持有更强者。
pub fn select_defensive_pair_rotation_asset(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    primary_defensive_asset: &str,
    secondary_defensive_asset: &str,
) -> Vec<String> {
    select_relative_strength_pair(
        asset_maps,
        dates,
        i,
        lookback,
        primary_defensive_asset,
        secondary_defensive_asset,
    )
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
    fn selects_stronger_defensive_asset() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 102.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "bond".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 100.8)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected =
            select_defensive_pair_rotation_asset(&maps, &dates, 1, 1, "dividend", "bond");

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
