use crate::data::Bar;

/// Compute a simple moving average series. Values before the window are None.
pub fn moving_average(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let mut result = vec![None; values.len()];
    if window == 0 || values.len() < window {
        return result;
    }
    let mut sum = 0.0;
    for i in 0..values.len() {
        sum += values[i];
        if i >= window {
            sum -= values[i - window];
        }
        if i + 1 >= window {
            result[i] = Some(sum / window as f64);
        }
    }
    result
}

/// Generate MA cross signals: 1 for buy, -1 for sell, 0 otherwise.
pub fn generate_signals(bars: &[Bar], fast: usize, slow: usize) -> Vec<i8> {
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let fast_ma = moving_average(&closes, fast);
    let slow_ma = moving_average(&closes, slow);
    let mut signals = vec![0_i8; bars.len()];

    for i in 1..bars.len() {
        if let (Some(f_prev), Some(s_prev), Some(f_now), Some(s_now)) =
            (fast_ma[i - 1], slow_ma[i - 1], fast_ma[i], slow_ma[i])
        {
            if f_prev <= s_prev && f_now > s_now {
                signals[i] = 1;
            } else if f_prev >= s_prev && f_now < s_now {
                signals[i] = -1;
            }
        }
    }
    signals
}
