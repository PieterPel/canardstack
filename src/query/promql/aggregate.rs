//! Aggregations: `by`/`without` grouping, `topk`/`bottomk`, `quantile`, `count_values`. The numeric
//! work lives in `crate::query::vector`.

use super::*;
use crate::query::vector::Grouping;

fn grouping(modifier: &Option<LabelModifier>) -> Option<Grouping> {
    modifier.as_ref().map(|m| match m {
        LabelModifier::Include(l) => Grouping { by: true, labels: l.labels.iter().map(|n| unescape(n)).collect() },
        LabelModifier::Exclude(l) => Grouping { by: false, labels: l.labels.iter().map(|n| unescape(n)).collect() },
    })
}

impl Ev<'_> {
    pub(super) fn aggregate(&self, agg: &parser::AggregateExpr) -> ApiResult<Value> {
        let op = agg.op.to_string();
        let (input, hists) = match self.eval(&agg.expr)? {
            Value::Vector(v) => (v, Vec::new()),
            Value::Samples(samples) => super::native::split(samples),
            _ => return Err(bad("expected an instant vector")),
        };
        let grouping = grouping(&agg.modifier);
        if op == "count_values" {
            let label = match &agg.param {
                Some(p) => self.string_arg(p)?,
                None => return Err(bad("count_values needs a label name")),
            };
            let groups = vector::group(&input, &grouping);
            return Ok(Value::Vector(self.count_values(&label, &input, &groups)));
        }
        let param = match &agg.param {
            Some(p) => Some(self.scalar_arg(p)?),
            None => None,
        };
        if !matches!(
            op.as_str(),
            "sum" | "avg" | "min" | "max" | "count" | "group" | "stddev" | "stdvar" | "quantile" | "topk" | "bottomk"
        ) {
            return Err(unsupported(format!("aggregation `{op}` is not supported")));
        }
        let floats = vector::aggregate(&op, self.n(), input, &grouping, param.as_deref())
            .map(Value::Vector)
            .map_err(bad)?;
        self.aggregate_samples(&op, floats, hists, &grouping)
    }

    /// `count_values("label", v)`: per group and step, how many series hold each distinct value.
    fn count_values(
        &self,
        label: &str,
        input: &[Series],
        groups: &BTreeMap<Labels, Vec<usize>>,
    ) -> Vec<Series> {
        let mut out: BTreeMap<Labels, Vec<Option<f64>>> = BTreeMap::new();
        for (group, members) in groups {
            for step in 0..self.n() {
                let mut counts: BTreeMap<String, f64> = BTreeMap::new();
                for &i in members {
                    if let Some(v) = input[i].vals[step] {
                        *counts.entry(v.to_string()).or_default() += 1.0;
                    }
                }
                for (value, count) in counts {
                    let mut labels = group.clone();
                    labels.insert(label.to_string(), value);
                    out.entry(labels).or_insert_with(|| vec![None; self.n()])[step] = Some(count);
                }
            }
        }
        out.into_iter().map(|(labels, vals)| Series { labels, vals }).collect()
    }
}
