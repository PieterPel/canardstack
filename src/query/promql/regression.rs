//! `deriv` and `predict_linear`: least-squares regression over the samples in a window.

use super::*;

/// Slope and intercept of the regression of value on time, with time in seconds relative to
/// `intercept_ms`. None when the window cannot define a line.
fn linear_regression(samples: Samples, intercept_ms: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }
    let (mut n, mut sum_x, mut sum_y, mut sum_xy, mut sum_x2) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for &(t, v) in samples {
        let x = (t - intercept_ms) as f64 / 1000.0;
        n += 1.0;
        sum_x += x;
        sum_y += v;
        sum_xy += x * v;
        sum_x2 += x * x;
    }
    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;
    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    Some((slope, intercept))
}

impl Ev<'_> {
    pub(super) fn regression(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        let range = args.first().ok_or_else(|| bad(format!("{name}: missing argument")))?;
        if name == "deriv" {
            return self.windowed(range, false, &|s, _, _| {
                linear_regression(s, s.first()?.0).map(|(slope, _)| slope)
            });
        }
        let seconds = self.scalar_arg(
            args.get(1)
                .ok_or_else(|| bad("predict_linear: missing duration argument"))?,
        )?;
        self.windowed_at(range, false, &|s, _, end, step| {
            linear_regression(s, end).map(|(slope, intercept)| slope * seconds[step] + intercept)
        })
    }
}
