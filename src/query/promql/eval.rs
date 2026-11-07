//! Expression walk: literals, selectors at every step, windowed range functions.

use super::*;

pub(super) struct Ev<'a> {
    pub(super) grid: &'a Grid,
    pub(super) data: &'a HashMap<String, Vec<Raw>>,
    /// Extra look-back baked into the keys of selectors inside enclosing subqueries.
    pub(super) extra_ms: i64,
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
                Value::Samples(samples) => Value::Samples(super::native::negate(samples)),
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
            Expr::VectorSelector(vs) => {
                let floats = self.instant_selector(vs, false)?;
                let hists = self.hist_instant(vs)?;
                Ok(if hists.is_empty() { Value::Vector(floats) } else { Value::Samples(super::native::combine(floats, hists)) })
            }
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
            .get(&selector_key(vs, window_ms + self.extra_ms))
            .map(Vec::as_slice)
            .ok_or_else(|| exec_err("selector was not fetched"))
    }

    /// The newest sample at or before each step within the lookback; with `timestamps`, the
    /// value is that sample's time in seconds (for `timestamp()`).
    pub(super) fn instant_selector(
        &self,
        vs: &VectorSelector,
        timestamps: bool,
    ) -> ApiResult<Vec<Series>> {
        let off = offset_ms(vs);
        let mut out = Vec::new();
        for raw in self.raw(vs, LOOKBACK_MS)? {
            let mut vals = vec![None; self.n()];
            for (i, slot) in vals.iter_mut().enumerate() {
                let t = self.grid.ts(i) - off;
                let idx = raw.samples.partition_point(|s| s.0 <= t);
                if idx > 0 && raw.samples[idx - 1].0 > t - LOOKBACK_MS {
                    let (sample_ms, value) = raw.samples[idx - 1];
                    *slot = Some(if timestamps { sample_ms as f64 / 1000.0 } else { value });
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
        self.windowed_at(arg, keep_name, &|s, start, end, _| f(s, start, end))
    }

    /// Like [`Self::windowed`], and also passes the step index to `f`.
    pub(super) fn windowed_at(
        &self,
        arg: &Expr,
        keep_name: bool,
        f: &dyn Fn(Samples, i64, i64, usize) -> Option<f64>,
    ) -> ApiResult<Value> {
        let ms = match arg {
            Expr::MatrixSelector(ms) => ms,
            Expr::Subquery(sq) => return self.windowed_subquery(sq, keep_name, f),
            _ => return Err(unsupported("expected a range vector argument")),
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
                    *slot = f(&raw.samples[lo..hi], start, end, i);
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
            // Functions that know nothing of histograms see only the float samples.
            Value::Samples(samples) => Ok(super::native::split(samples).0),
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
