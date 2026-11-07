//! Subqueries: `expr[range:step]` evaluates `expr` at every `step` inside each outer window and
//! hands those points to the enclosing range function.

use super::fetch::{subquery_extra_ms, DEFAULT_SUBQUERY_STEP_MS};
use super::*;

impl Ev<'_> {
    pub(super) fn windowed_subquery(
        &self,
        sq: &SubqueryExpr,
        keep_name: bool,
        f: &dyn Fn(Samples, i64, i64, usize) -> Option<f64>,
    ) -> ApiResult<Value> {
        let range = sq.range.as_millis() as i64;
        let step = sq.step.map_or(DEFAULT_SUBQUERY_STEP_MS, |d| d.as_millis() as i64).max(1);
        let off = match &sq.offset {
            Some(Offset::Pos(d)) => d.as_millis() as i64,
            Some(Offset::Neg(d)) => -(d.as_millis() as i64),
            None => 0,
        };
        let inner_extra = self.extra_ms + subquery_extra_ms(sq);
        let mut out: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
        for i in 0..self.n() {
            let end = self.grid.ts(i) - off;
            let start = end - range;
            // Subquery evaluation times are multiples of the step inside (start, end].
            let mut first = start.div_euclid(step) * step;
            if first <= start {
                first += step;
            }
            if first > end {
                continue;
            }
            let grid = Grid {
                start_ms: first,
                step_ms: step,
                steps: ((end - first) / step + 1) as usize,
            };
            let inner = Ev {
                grid: &grid,
                data: self.data,
                extra_ms: inner_extra,
            };
            let series = match inner.eval(&sq.expr)? {
                Value::Vector(series) => series,
                // Subqueries over native histograms are not supported; their float part is used.
                Value::Samples(samples) => super::native::split(samples).0,
                Value::Scalar(values) => vec![Series {
                    labels: Labels::new(),
                    vals: values.into_iter().map(Some).collect(),
                }],
                Value::Str(_) => return Err(bad("a subquery over a string")),
            };
            for s in series {
                let samples: Vec<(i64, f64)> = s
                    .vals
                    .iter()
                    .enumerate()
                    .filter_map(|(j, v)| Some((grid.ts(j), (*v)?)))
                    .collect();
                if samples.is_empty() {
                    continue;
                }
                let mut labels = s.labels;
                if !keep_name {
                    labels.remove(METRIC_NAME);
                }
                let vals = out.entry(labels).or_insert_with(|| vec![None; self.n()]);
                vals[i] = f(&samples, start, end, i);
            }
        }
        Ok(Value::Vector(
            out.into_iter()
                .filter(|(_, vals)| vals.iter().any(Option::is_some))
                .map(|(labels, vals)| Series { labels, vals })
                .collect(),
        ))
    }
}
