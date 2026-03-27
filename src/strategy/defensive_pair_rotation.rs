use crate::data::Bar;
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
    let Some(primary_bars) = asset_maps.get(primary_defensive_asset) else {
        return Vec::new();
    };
    let Some(secondary_bars) = asset_maps.get(secondary_defensive_asset) else {
        return Vec::new();
    };

    let primary_now = primary_bars.get(&dates[i]).unwrap().close;
    let primary_past = primary_bars.get(&dates[i - lookback]).unwrap().close;
    let secondary_now = secondary_bars.get(&dates[i]).unwrap().close;
    let secondary_past = secondary_bars.get(&dates[i - lookback]).unwrap().close;

    let primary_return = primary_now / primary_past - 1.0;
    let secondary_return = secondary_now / secondary_past - 1.0;

    if primary_return >= secondary_return {
        vec![primary_defensive_asset.to_string()]
    } else {
        vec![secondary_defensive_asset.to_string()]
    }
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

        let selected = select_defensive_pair_rotation_asset(
            &maps,
            &dates,
            1,
            1,
            "dividend",
            "bond",
        );

        assert_eq!(selected, vec!["dividend".to_string()]);
    }
}
