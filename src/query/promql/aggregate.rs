//! Aggregations: `by`/`without` grouping, `topk`/`bottomk`, `quantile`.

use super::*;

impl Ev<'_> {
    pub(super) fn group_key(&self, labels: &Labels, modifier: &Option<LabelModifier>) -> Labels {
        match modifier {
            None => Labels::new(),
            Some(LabelModifier::Include(l)) => l
                .labels
                .iter()
                .filter_map(|name| {
                    let name = canon(&unescape(name));
                    labels.get(&name).map(|v| (name, v.clone()))
                })
                .collect(),
            Some(LabelModifier::Exclude(l)) => {
                let drop: Vec<String> = l.labels.iter().map(|n| canon(&unescape(n))).collect();
                labels
                    .iter()
                    .filter(|(k, _)| k.as_str() != METRIC_NAME && !drop.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
        }
    }

    pub(super) fn aggregate(&self, agg: &parser::AggregateExpr) -> ApiResult<Value> {
        let op = agg.op.to_string();
        let input = self.vector_arg(&agg.expr)?;
        let param = match &agg.param {
            Some(p) => Some(self.scalar_arg(p)?),
            None => None,
        };
        let mut groups: BTreeMap<Labels, Vec<usize>> = BTreeMap::new();
        for (i, s) in input.iter().enumerate() {
            groups
                .entry(self.group_key(&s.labels, &agg.modifier))
                .or_default()
                .push(i);
        }
        let n = self.n();
        let mut out = Vec::new();
        if op == "topk" || op == "bottomk" {
            let k = param.ok_or_else(|| bad("missing k"))?;
            for members in groups.values() {
                let mut kept: HashMap<usize, Vec<Option<f64>>> = HashMap::new();
                for step in 0..n {
                    let mut present: Vec<(usize, f64)> = members
                        .iter()
                        .filter_map(|&i| input[i].vals[step].map(|v| (i, v)))
                        .collect();
                    present.sort_by(|a, b| {
                        if op == "topk" {
                            b.1.total_cmp(&a.1)
                        } else {
                            a.1.total_cmp(&b.1)
                        }
                    });
                    for (i, v) in present.into_iter().take(k[step].max(0.0) as usize) {
                        kept.entry(i).or_insert_with(|| vec![None; n])[step] = Some(v);
                    }
                }
                let mut idx: Vec<_> = kept.into_iter().collect();
                idx.sort_by_key(|(i, _)| *i);
                out.extend(idx.into_iter().map(|(i, vals)| Series {
                    labels: input[i].labels.clone(),
                    vals,
                }));
            }
            return Ok(Value::Vector(out));
        }
        if !matches!(
            op.as_str(),
            "sum" | "avg" | "min" | "max" | "count" | "group" | "stddev" | "stdvar" | "quantile"
        ) {
            return Err(unsupported(format!("aggregation `{op}` is not supported")));
        }
        for (labels, members) in groups {
            let vals = (0..n)
                .map(|step| {
                    let vs: Vec<f64> = members
                        .iter()
                        .filter_map(|&i| input[i].vals[step])
                        .collect();
                    if vs.is_empty() {
                        return None;
                    }
                    Some(match op.as_str() {
                        "sum" => vs.iter().sum(),
                        "avg" => vs.iter().sum::<f64>() / vs.len() as f64,
                        "min" => vs.iter().copied().fold(f64::INFINITY, f64::min),
                        "max" => vs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                        "count" => vs.len() as f64,
                        "group" => 1.0,
                        "stddev" => variance(vs.into_iter()).sqrt(),
                        "stdvar" => variance(vs.into_iter()),
                        _ => quantile(param.as_ref().map_or(f64::NAN, |p| p[step]), vs),
                    })
                })
                .collect();
            out.push(Series { labels, vals });
        }
        Ok(Value::Vector(out))
    }
}
