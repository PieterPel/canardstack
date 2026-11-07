//! `histogram_quantile` over classic histograms: cumulative `_bucket` series with an `le` label.

use super::*;

/// Prometheus' bucketQuantile: linear interpolation inside the bucket holding the rank.
fn bucket_quantile(q: f64, mut buckets: Vec<(f64, f64)>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.last().is_none_or(|b| !b.0.is_infinite() || b.0 < 0.0) {
        return f64::NAN;
    }
    // Merge duplicate bounds, then make the cumulative counts monotonic.
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for (bound, count) in buckets {
        match merged.last_mut() {
            Some(last) if last.0 == bound => last.1 += count,
            _ => merged.push((bound, count)),
        }
    }
    let mut max = f64::NEG_INFINITY;
    for b in &mut merged {
        max = max.max(b.1);
        b.1 = max;
    }
    if merged.len() < 2 {
        return f64::NAN;
    }
    let observations = merged[merged.len() - 1].1;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let idx = merged.partition_point(|b| b.1 < rank);
    if idx == merged.len() - 1 {
        return merged[merged.len() - 2].0;
    }
    if idx == 0 && merged[0].0 <= 0.0 {
        return merged[0].0;
    }
    let (mut start, end, mut count) = (0.0, merged[idx].0, merged[idx].1);
    if idx > 0 {
        start = merged[idx - 1].0;
        count -= merged[idx - 1].1;
        rank -= merged[idx - 1].1;
    }
    start + (end - start) * (rank / count)
}

impl Ev<'_> {
    pub(super) fn histogram_quantile(&self, args: &[Box<Expr>]) -> ApiResult<Value> {
        let [phi, buckets] = args else {
            return Err(bad("histogram_quantile takes a quantile and a vector"));
        };
        let phi = self.scalar_arg(phi)?;
        let input = self.vector_arg(buckets)?;
        let mut groups: BTreeMap<Labels, Vec<(f64, usize)>> = BTreeMap::new();
        for (i, s) in input.iter().enumerate() {
            let Some(bound) = s.labels.get("le").and_then(|le| match le.as_str() {
                "+Inf" | "Inf" => Some(f64::INFINITY),
                other => other.parse::<f64>().ok(),
            }) else {
                continue;
            };
            let mut key = s.labels.clone();
            key.remove("le");
            key.remove(METRIC_NAME);
            groups.entry(key).or_default().push((bound, i));
        }
        Ok(Value::Vector(
            groups
                .into_iter()
                .map(|(labels, members)| {
                    let vals = (0..self.n())
                        .map(|step| {
                            let buckets: Vec<(f64, f64)> = members
                                .iter()
                                .filter_map(|&(bound, i)| input[i].vals[step].map(|v| (bound, v)))
                                .collect();
                            (!buckets.is_empty()).then(|| bucket_quantile(phi[step], buckets))
                        })
                        .collect();
                    Series { labels, vals }
                })
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_interpolates_inside_the_bucket() {
        // 10 observations <= 1, 30 <= 2, 40 total.
        let buckets = vec![(1.0, 10.0), (2.0, 30.0), (f64::INFINITY, 40.0)];
        assert_eq!(bucket_quantile(0.5, buckets.clone()), 1.5);
        // Rank 4 of 40 lies in the first bucket, which interpolates from 0.
        assert_eq!(bucket_quantile(0.1, buckets.clone()), 0.4);
        // The rank falls in the +Inf bucket: the highest finite bound.
        assert_eq!(bucket_quantile(0.99, buckets), 2.0);
        assert!(bucket_quantile(0.5, vec![(1.0, 1.0)]).is_nan());
    }
}
