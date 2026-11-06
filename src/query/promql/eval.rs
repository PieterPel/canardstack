//! Expression walk: literals, selectors at every step, windowed range functions.

use super::*;

pub(super) struct Ev<'a> {
    pub(super) grid: &'a Grid,
    pub(super) data: &'a HashMap<String, Vec<Raw>>,
}

pub(super) type Samples<'a> = &'a [(i64, f64)];

impl Ev<'_> {
    pub(super) fn n(&self) -> usize {
        self.grid.steps
    }

    pub(super) fn eval(&self, expr: &Expr) -> ApiResult<Value> {
        match expr {
            Expr::NumberLiteral(n) => Ok(Value::Scalar(vec![n.val; self.n()])),
            Expr::StringLiteral(s) => Ok(Value::Str(s.val.clone())),
            Expr::Paren(p) => self.eval(&p.expr),
            Expr::Unary(u) => Ok(match self.eval(&u.expr)? {
                Value::Scalar(v) => Value::Scalar(v.into_iter().map(|x| -x).collect()),
                Value::Vector(series) => Value::Vector(
                    series
                        .into_iter()
                        .map(|mut s| {
                            s.labels.remove(METRIC_NAME);
                            s.vals = s.vals.into_iter().map(|v| v.map(|x| -x)).collect();
                            s
                        })
                        .collect(),
                ),
                Value::Str(_) => return Err(bad("unary minus on a string")),
            }),
            Expr::VectorSelector(vs) => Ok(Value::Vector(self.instant_selector(vs)?)),
            Expr::MatrixSelector(_) => Err(unsupported(
                "range vectors are only supported as function arguments",
            )),
            Expr::Call(call) => self.call(call.func.name, &call.args.args),
            Expr::Aggregate(agg) => self.aggregate(agg),
            Expr::Binary(b) => self.binary(b),
            _ => Err(unsupported("unsupported expression")),
        }
    }

    pub(super) fn raw(&self, vs: &VectorSelector, window_ms: i64) -> ApiResult<&[Raw]> {
        self.data
            .get(&selector_key(vs, window_ms))
            .map(Vec::as_slice)
            .ok_or_else(|| exec_err("selector was not fetched"))
    }

    pub(super) fn instant_selector(&self, vs: &VectorSelector) -> ApiResult<Vec<Series>> {
        let off = offset_ms(vs);
        let mut out = Vec::new();
        for raw in self.raw(vs, LOOKBACK_MS)? {
            let mut vals = vec![None; self.n()];
            for (i, slot) in vals.iter_mut().enumerate() {
                let t = self.grid.ts(i) - off;
                let idx = raw.samples.partition_point(|s| s.0 <= t);
                if idx > 0 && raw.samples[idx - 1].0 > t - LOOKBACK_MS {
                    *slot = Some(raw.samples[idx - 1].1);
                }
            }
            if vals.iter().any(Option::is_some) {
                out.push(Series {
                    labels: raw.labels.clone(),
                    vals,
                });
            }
        }
        Ok(out)
    }

    /// Applies `f` to the samples in the left-open window `(t - range, t]` at every step.

    pub(super) fn windowed(
        &self,
        arg: &Expr,
        keep_name: bool,
        f: &dyn Fn(Samples, i64, i64) -> Option<f64>,
    ) -> ApiResult<Value> {
        let Expr::MatrixSelector(ms) = arg else {
            return Err(unsupported("expected a range vector argument"));
        };
        let range = ms.range.as_millis() as i64;
        let off = offset_ms(&ms.vs);
        let mut out = Vec::new();
        for raw in self.raw(&ms.vs, range)? {
            let mut vals = vec![None; self.n()];
            for (i, slot) in vals.iter_mut().enumerate() {
                let end = self.grid.ts(i) - off;
                let start = end - range;
                let lo = raw.samples.partition_point(|s| s.0 <= start);
                let hi = raw.samples.partition_point(|s| s.0 <= end);
                if lo < hi {
                    *slot = f(&raw.samples[lo..hi], start, end);
                }
            }
            if vals.iter().any(Option::is_some) {
                let mut labels = raw.labels.clone();
                if !keep_name {
                    labels.remove(METRIC_NAME);
                }
                out.push(Series { labels, vals });
            }
        }
        Ok(Value::Vector(out))
    }

    pub(super) fn vector_arg(&self, expr: &Expr) -> ApiResult<Vec<Series>> {
        match self.eval(expr)? {
            Value::Vector(v) => Ok(v),
            _ => Err(bad("expected an instant vector")),
        }
    }

    pub(super) fn scalar_arg(&self, expr: &Expr) -> ApiResult<Vec<f64>> {
        match self.eval(expr)? {
            Value::Scalar(v) => Ok(v),
            _ => Err(bad("expected a scalar")),
        }
    }

    pub(super) fn string_arg(&self, expr: &Expr) -> ApiResult<String> {
        match self.eval(expr)? {
            Value::Str(s) => Ok(s),
            _ => Err(bad("expected a string")),
        }
    }

    pub(super) fn map_vector(&self, expr: &Expr, f: impl Fn(f64) -> f64) -> ApiResult<Value> {
        Ok(Value::Vector(
            self.vector_arg(expr)?
                .into_iter()
                .map(|mut s| {
                    s.labels.remove(METRIC_NAME);
                    s.vals = s.vals.into_iter().map(|v| v.map(&f)).collect();
                    s
                })
                .collect(),
        ))
    }
}
