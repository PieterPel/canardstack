//! Calendar functions (`year`, `hour`, ...) over unix-second values, and `timestamp()`.

use super::*;
use chrono::{Datelike, TimeZone, Timelike, Utc};

impl Ev<'_> {
    /// `year()`, `month()`, `day_of_week()`, ... of each sample value read as unix seconds (UTC).
    /// With no argument they use the evaluation time.
    pub(super) fn datetime(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        let series = match args.first() {
            Some(arg) => self.vector_arg(arg)?,
            None => vec![Series {
                labels: Labels::new(),
                vals: (0..self.n())
                    .map(|i| Some(self.grid.ts(i) as f64 / 1000.0))
                    .collect(),
            }],
        };
        let part = |seconds: f64| -> f64 {
            if !seconds.is_finite() {
                return f64::NAN;
            }
            let Some(t) = Utc.timestamp_opt(seconds.floor() as i64, 0).single() else {
                return f64::NAN;
            };
            match name {
                "year" => f64::from(t.year()),
                "month" => f64::from(t.month()),
                "day_of_month" => f64::from(t.day()),
                "day_of_week" => f64::from(t.weekday().num_days_from_sunday()),
                "day_of_year" => f64::from(t.ordinal()),
                "hour" => f64::from(t.hour()),
                "minute" => f64::from(t.minute()),
                _ => {
                    let (y, m) = (t.year(), t.month());
                    let next = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
                    let first = Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0).single();
                    let after = Utc.with_ymd_and_hms(next.0, next.1, 1, 0, 0, 0).single();
                    match (first, after) {
                        (Some(a), Some(b)) => (b - a).num_days() as f64,
                        _ => f64::NAN,
                    }
                }
            }
        };
        Ok(Value::Vector(
            series
                .into_iter()
                .map(|mut s| {
                    s.labels.remove(METRIC_NAME);
                    s.vals = s.vals.into_iter().map(|v| v.map(part)).collect();
                    s
                })
                .collect(),
        ))
    }

    /// `timestamp(v)`: the time of the sample behind each value, or the step time for anything
    /// that is not a plain selector.
    pub(super) fn timestamp(&self, arg: &Expr) -> ApiResult<Value> {
        let mut inner = arg;
        while let Expr::Paren(p) = inner {
            inner = &p.expr;
        }
        if let Expr::VectorSelector(vs) = inner {
            return Ok(Value::Vector(self.instant_selector(vs, true)?));
        }
        Ok(Value::Vector(
            self.vector_arg(arg)?
                .into_iter()
                .map(|mut s| {
                    s.labels.remove(METRIC_NAME);
                    for (i, v) in s.vals.iter_mut().enumerate() {
                        if v.is_some() {
                            *v = Some(self.grid.ts(i) as f64 / 1000.0);
                        }
                    }
                    s
                })
                .collect(),
        ))
    }
}
