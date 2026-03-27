use crate::data::Bar;
use chrono::NaiveDate;
use std::collections::HashMap;

/// 双资产相对强弱切换：比较两个资产在回看窗口内的收益，持有更强者。
pub fn select_relative_strength_pair(
    asset_maps: &HashMap<String, HashMap<NaiveDate, Bar>>,
    dates: &[NaiveDate],
    i: usize,
    lookback: usize,
    primary_asset: &str,
    alternate_asset: &str,
) -> Vec<String> {
    let Some(primary_bars) = asset_maps.get(primary_asset) else {
        return Vec::new();
    };
    let Some(alternate_bars) = asset_maps.get(alternate_asset) else {
        return Vec::new();
    };

    let primary_now = primary_bars.get(&dates[i]).unwrap().close;
    let primary_past = primary_bars.get(&dates[i - lookback]).unwrap().close;
    let alternate_now = alternate_bars.get(&dates[i]).unwrap().close;
    let alternate_past = alternate_bars.get(&dates[i - lookback]).unwrap().close;

    let primary_return = primary_now / primary_past - 1.0;
    let alternate_return = alternate_now / alternate_past - 1.0;

    if primary_return >= alternate_return {
        vec![primary_asset.to_string()]
    } else {
        vec![alternate_asset.to_string()]
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
    fn selects_stronger_asset() {
        let dates = vec![
            NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
            NaiveDate::parse_from_str("2024-01-02", "%Y-%m-%d").unwrap(),
        ];
        let mut maps = HashMap::new();
        maps.insert(
            "hs300".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 105.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );
        maps.insert(
            "dividend".to_string(),
            vec![bar("2024-01-01", 100.0), bar("2024-01-02", 101.0)]
                .into_iter()
                .map(|item| (item.date, item))
                .collect(),
        );

        let selected = select_relative_strength_pair(&maps, &dates, 1, 1, "hs300", "dividend");

        assert_eq!(selected, vec!["hs300".to_string()]);
    }
}
