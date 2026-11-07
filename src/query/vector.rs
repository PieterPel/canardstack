//! Instant-vector operations shared by the PromQL and LogQL evaluators: the series and grid types,
//! `by`/`without` aggregation, vector matching, set operators and the numeric helpers. They work on
//! labelled series with one optional value per step and know nothing about either query language.

use crate::query::prom_labels::Labels;
use std::collections::{BTreeMap, HashMap};

const METRIC_NAME: &str = "__name__";

/// The evaluation steps: `steps` timestamps `step_ms` apart from `start_ms`.
#[derive(Clone, Copy)]
pub struct Grid {
    pub start_ms: i64,
    pub step_ms: i64,
    pub steps: usize,
}

impl Grid {
    pub fn ts(&self, i: usize) -> i64 {
        self.start_ms + self.step_ms * i as i64
    }

    pub fn end_ms(&self) -> i64 {
        self.ts(self.steps.saturating_sub(1))
    }
}

pub struct Series {
    pub labels: Labels,
    pub vals: Vec<Option<f64>>,
}

/// `by (labels)` keeps those labels; `without (labels)` drops them (and `__name__`).
#[derive(Clone, Debug)]
pub struct Grouping {
    pub by: bool,
    pub labels: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Card {
    OneToOne,
    /// `group_left`: many series on the left match one on the right.
    ManyToOne,
    /// `group_right`.
    OneToMany,
}

/// How the two sides of a binary operator pair up.
#[derive(Clone, Debug)]
pub struct Matching {
    /// `on(labels)` (true) or `ignoring(labels)` (false).
    pub on: Option<(bool, Vec<String>)>,
    pub card: Card,
    /// Labels copied from the "one" side with `group_left(...)` / `group_right(...)`.
    pub include: Vec<String>,
}

impl Matching {
    pub fn one_to_one() -> Self {
        Matching {
            on: None,
            card: Card::OneToOne,
            include: Vec::new(),
        }
    }
}

pub enum Applied {
    Value(f64),
    Cmp(bool),
}

pub fn arith(op: &str, a: f64, b: f64) -> Applied {
    match op {
        "+" => Applied::Value(a + b),
        "-" => Applied::Value(a - b),
        "*" => Applied::Value(a * b),
        "/" => Applied::Value(a / b),
        "%" => Applied::Value(a % b),
        "^" => Applied::Value(a.powf(b)),
        "atan2" => Applied::Value(a.atan2(b)),
        "==" => Applied::Cmp(a == b),
        "!=" => Applied::Cmp(a != b),
        ">" => Applied::Cmp(a > b),
        "<" => Applied::Cmp(a < b),
        ">=" => Applied::Cmp(a >= b),
        "<=" => Applied::Cmp(a <= b),
        _ => Applied::Value(f64::NAN),
    }
}

/// Builds result series keyed by label set while evaluating step by step.
pub struct Acc {
    steps: usize,
    index: HashMap<Labels, usize>,
    series: Vec<Series>,
}

impl Acc {
    pub fn new(steps: usize) -> Self {
        Acc {
            steps,
            index: HashMap::new(),
            series: Vec::new(),
        }
    }

    pub fn set(&mut self, labels: Labels, step: usize, value: f64) {
        let next = self.series.len();
        let idx = *self.index.entry(labels.clone()).or_insert(next);
        if idx == next {
            self.series.push(Series {
                labels,
                vals: vec![None; self.steps],
            });
        }
        let slot = &mut self.series[idx].vals[step];
        if slot.is_none() {
            *slot = Some(value);
        }
    }

    pub fn finish(mut self) -> Vec<Series> {
        self.series.sort_by(|a, b| a.labels.cmp(&b.labels));
        self.series
    }
}

pub fn vector_scalar(
    series: Vec<Series>,
    scalar: &[f64],
    op: &str,
    comparison: bool,
    as_bool: bool,
    scalar_left: bool,
) -> Vec<Series> {
    let mut out = Vec::new();
    for mut s in series {
        if !comparison || as_bool {
            s.labels.remove(METRIC_NAME);
        }
        for (i, slot) in s.vals.iter_mut().enumerate() {
            let Some(v) = *slot else { continue };
            let (a, c) = if scalar_left { (scalar[i], v) } else { (v, scalar[i]) };
            *slot = match arith(op, a, c) {
                Applied::Value(x) => Some(x),
                Applied::Cmp(t) if as_bool => Some(f64::from(t)),
                Applied::Cmp(true) => Some(v),
                Applied::Cmp(false) => None,
            };
        }
        if s.vals.iter().any(Option::is_some) {
            out.push(s);
        }
    }
    out
}

/// The labels two series are matched on.
pub fn signature(labels: &Labels, on: &Option<(bool, Vec<String>)>) -> Labels {
    let base = labels.iter().filter(|(k, _)| k.as_str() != METRIC_NAME);
    match on {
        None => base.map(|(k, v)| (k.clone(), v.clone())).collect(),
        Some((true, keep)) => base
            .filter(|(k, _)| keep.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Some((false, drop)) => base
            .filter(|(k, _)| !drop.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

pub fn vector_vector(
    steps: usize,
    op: &str,
    comparison: bool,
    as_bool: bool,
    lhs: Vec<Series>,
    rhs: Vec<Series>,
    m: &Matching,
) -> Result<Vec<Series>, String> {
    // `one` is the side that must match at most one series per signature.
    let (many, one, flipped) = match m.card {
        Card::OneToMany => (&rhs, &lhs, true),
        _ => (&lhs, &rhs, false),
    };
    let mut acc = Acc::new(steps);
    for step in 0..steps {
        let mut table: HashMap<Labels, usize> = HashMap::new();
        for (i, s) in one.iter().enumerate() {
            if s.vals[step].is_some() && table.insert(signature(&s.labels, &m.on), i).is_some() {
                return Err("found duplicate series for the match group on the right hand side; many-to-one matching must be explicit (group_left/group_right)".to_string());
            }
        }
        let mut seen: HashMap<Labels, ()> = HashMap::new();
        for many_series in many {
            let Some(mv) = many_series.vals[step] else { continue };
            let sig = signature(&many_series.labels, &m.on);
            let Some(&oi) = table.get(&sig) else { continue };
            let ov = one[oi].vals[step].unwrap_or(f64::NAN);
            let (a, c) = if flipped { (ov, mv) } else { (mv, ov) };
            let value = match arith(op, a, c) {
                Applied::Value(x) => x,
                Applied::Cmp(t) if as_bool => f64::from(t),
                Applied::Cmp(true) => a,
                Applied::Cmp(false) => continue,
            };
            let mut labels = many_series.labels.clone();
            if !comparison || as_bool {
                labels.remove(METRIC_NAME);
            }
            if m.card == Card::OneToOne {
                match &m.on {
                    Some((true, _)) => labels.retain(|k, _| sig.contains_key(k)),
                    Some((false, drop)) => {
                        for name in drop {
                            labels.remove(name);
                        }
                    }
                    None => {}
                }
            } else {
                for name in &m.include {
                    match one[oi].labels.get(name) {
                        Some(v) => labels.insert(name.clone(), v.clone()),
                        None => labels.remove(name),
                    };
                }
            }
            if m.card == Card::OneToOne && seen.insert(labels.clone(), ()).is_some() {
                return Err("multiple matches for labels: grouping labels must ensure unique matches".to_string());
            }
            acc.set(labels, step, value);
        }
    }
    Ok(acc.finish())
}

/// `and`, `or` and `unless`.
pub fn set_op(
    steps: usize,
    op: &str,
    lhs: Vec<Series>,
    rhs: Vec<Series>,
    on: &Option<(bool, Vec<String>)>,
) -> Vec<Series> {
    let mut acc = Acc::new(steps);
    for step in 0..steps {
        let rsigs: HashMap<Labels, ()> = rhs
            .iter()
            .filter(|s| s.vals[step].is_some())
            .map(|s| (signature(&s.labels, on), ()))
            .collect();
        let mut lsigs: HashMap<Labels, ()> = HashMap::new();
        for s in &lhs {
            let Some(v) = s.vals[step] else { continue };
            let sig = signature(&s.labels, on);
            lsigs.insert(sig.clone(), ());
            let hit = rsigs.contains_key(&sig);
            if (op == "and" && hit) || (op == "unless" && !hit) || op == "or" {
                acc.set(s.labels.clone(), step, v);
            }
        }
        if op == "or" {
            for s in &rhs {
                let Some(v) = s.vals[step] else { continue };
                if !lsigs.contains_key(&signature(&s.labels, on)) {
                    acc.set(s.labels.clone(), step, v);
                }
            }
        }
    }
    acc.finish()
}

pub fn group_key(labels: &Labels, grouping: &Option<Grouping>) -> Labels {
    match grouping {
        None => Labels::new(),
        Some(Grouping { by: true, labels: keep }) => keep
            .iter()
            .filter_map(|name| labels.get(name).map(|v| (name.clone(), v.clone())))
            .collect(),
        Some(Grouping { by: false, labels: drop }) => labels
            .iter()
            .filter(|(k, _)| k.as_str() != METRIC_NAME && !drop.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// The grouping labels, or every label when no grouping is given (a range aggregation keeps its
/// series apart by default).
pub fn group_key_or_all(labels: &Labels, grouping: &Option<Grouping>) -> Labels {
    match grouping {
        None => labels.clone(),
        some => group_key(labels, some),
    }
}

/// Series indexes per group.
pub fn group(input: &[Series], grouping: &Option<Grouping>) -> BTreeMap<Labels, Vec<usize>> {
    let mut groups: BTreeMap<Labels, Vec<usize>> = BTreeMap::new();
    for (i, s) in input.iter().enumerate() {
        groups.entry(group_key(&s.labels, grouping)).or_default().push(i);
    }
    groups
}

/// `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`, `quantile` (parameter: the
/// quantile per step) and `topk` / `bottomk` (parameter: k per step).
pub fn aggregate(
    op: &str,
    steps: usize,
    input: Vec<Series>,
    grouping: &Option<Grouping>,
    param: Option<&[f64]>,
) -> Result<Vec<Series>, String> {
    let groups = group(&input, grouping);
    let mut out = Vec::new();
    if op == "topk" || op == "bottomk" {
        let k = param.ok_or("topk and bottomk need a k")?;
        for members in groups.values() {
            let mut kept: HashMap<usize, Vec<Option<f64>>> = HashMap::new();
            for step in 0..steps {
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
                    kept.entry(i).or_insert_with(|| vec![None; steps])[step] = Some(v);
                }
            }
            let mut idx: Vec<_> = kept.into_iter().collect();
            idx.sort_by_key(|(i, _)| *i);
            out.extend(idx.into_iter().map(|(i, vals)| Series {
                labels: input[i].labels.clone(),
                vals,
            }));
        }
        return Ok(out);
    }
    if !matches!(
        op,
        "sum" | "avg" | "min" | "max" | "count" | "group" | "stddev" | "stdvar" | "quantile"
    ) {
        return Err(format!("aggregation `{op}` is not supported"));
    }
    for (labels, members) in groups {
        let vals = (0..steps)
            .map(|step| {
                let vs: Vec<f64> = members.iter().filter_map(|&i| input[i].vals[step]).collect();
                if vs.is_empty() {
                    return None;
                }
                Some(match op {
                    "sum" => vs.iter().sum(),
                    "avg" => vs.iter().sum::<f64>() / vs.len() as f64,
                    "min" => vs.iter().copied().fold(f64::INFINITY, f64::min),
                    "max" => vs.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                    "count" => vs.len() as f64,
                    "group" => 1.0,
                    "stddev" => variance(vs.into_iter()).sqrt(),
                    "stdvar" => variance(vs.into_iter()),
                    _ => quantile(param.map_or(f64::NAN, |p| p[step]), vs),
                })
            })
            .collect();
        out.push(Series { labels, vals });
    }
    Ok(out)
}

/// Population variance.
pub fn variance(values: impl Iterator<Item = f64>) -> f64 {
    let (mut count, mut mean, mut m2) = (0.0, 0.0, 0.0);
    for v in values {
        count += 1.0;
        let delta = v - mean;
        mean += delta / count;
        m2 += delta * (v - mean);
    }
    if count == 0.0 {
        f64::NAN
    } else {
        m2 / count
    }
}

/// Prometheus' quantile: linear interpolation between the two closest ranks.
pub fn quantile(q: f64, mut values: Vec<f64>) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let rank = q * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let weight = rank - lower as f64;
    values[lower] * (1.0 - weight) + values[upper] * weight
}

/// Prometheus' `extrapolatedRate`: slope of the first and last sample in the window, extended to
/// the window edges unless the samples stop short of them.
pub fn extrapolated(s: &[(i64, f64)], start: i64, end: i64, counter: bool, rate: bool) -> Option<f64> {
    if s.len() < 2 {
        return None;
    }
    let (first, last) = (s[0], s[s.len() - 1]);
    let mut result = last.1 - first.1;
    if counter {
        let mut prev = first.1;
        for sample in &s[1..] {
            if sample.1 < prev {
                result += prev;
            }
            prev = sample.1;
        }
    }
    let mut to_start = (first.0 - start) as f64 / 1000.0;
    let to_end = (end - last.0) as f64 / 1000.0;
    let sampled = (last.0 - first.0) as f64 / 1000.0;
    let avg_gap = sampled / (s.len() - 1) as f64;
    if counter && result > 0.0 && first.1 >= 0.0 {
        let to_zero = sampled * (first.1 / result);
        if to_zero < to_start {
            to_start = to_zero;
        }
    }
    let threshold = avg_gap * 1.1;
    let mut interval = sampled;
    interval += if to_start < threshold {
        to_start
    } else {
        avg_gap / 2.0
    };
    interval += if to_end < threshold {
        to_end
    } else {
        avg_gap / 2.0
    };
    let mut factor = interval / sampled;
    if rate {
        factor /= (end - start) as f64 / 1000.0;
    }
    Some(result * factor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(labels: &[(&str, &str)], vals: &[Option<f64>]) -> Series {
        Series {
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            vals: vals.to_vec(),
        }
    }

    fn values(s: &[Series]) -> Vec<(String, Vec<Option<f64>>)> {
        s.iter()
            .map(|s| (s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(","), s.vals.clone()))
            .collect()
    }

    #[test]
    fn aggregation_groups_by_and_without() {
        let input = || {
            vec![
                series(&[("a", "1"), ("b", "x")], &[Some(1.0), Some(2.0)]),
                series(&[("a", "1"), ("b", "y")], &[Some(3.0), None]),
                series(&[("a", "2"), ("b", "x")], &[Some(5.0), Some(7.0)]),
            ]
        };
        let by_a = Some(Grouping { by: true, labels: vec!["a".into()] });
        let got = aggregate("sum", 2, input(), &by_a, None).unwrap();
        assert_eq!(values(&got), [("a=1".into(), vec![Some(4.0), Some(2.0)]), ("a=2".into(), vec![Some(5.0), Some(7.0)])]);
        let without_b = Some(Grouping { by: false, labels: vec!["b".into()] });
        let got = aggregate("count", 2, input(), &without_b, None).unwrap();
        assert_eq!(values(&got)[0].1, vec![Some(2.0), Some(1.0)]);
        let got = aggregate("topk", 2, input(), &None, Some(&[1.0, 2.0])).unwrap();
        assert_eq!(got.len(), 2); // step 0 keeps the 5; step 1 keeps the 7 and the 2
        assert!(aggregate("nope", 2, input(), &None, None).is_err());
    }

    #[test]
    fn matching_pairs_series_and_set_ops_filter_them() {
        let lhs = || vec![series(&[("a", "1"), ("x", "l")], &[Some(10.0)]), series(&[("a", "2"), ("x", "l")], &[Some(20.0)])];
        let rhs = || vec![series(&[("a", "1")], &[Some(2.0)])];
        let on_a = Matching { on: Some((true, vec!["a".into()])), card: Card::ManyToOne, include: vec![] };
        let got = vector_vector(1, "/", false, false, lhs(), rhs(), &on_a).unwrap();
        assert_eq!(values(&got), [("a=1,x=l".into(), vec![Some(5.0)])]);
        // one-to-one on `a` trims the result to the matching label
        let one = Matching { on: Some((true, vec!["a".into()])), card: Card::OneToOne, include: vec![] };
        let got = vector_vector(1, "+", false, false, lhs(), rhs(), &one).unwrap();
        assert_eq!(values(&got), [("a=1".into(), vec![Some(12.0)])]);
        // two left series with the same signature on the "one" side is an error
        let dup = vec![series(&[("a", "1"), ("y", "1")], &[Some(1.0)]), series(&[("a", "1"), ("y", "2")], &[Some(1.0)])];
        assert!(vector_vector(1, "+", false, false, lhs(), dup, &one).is_err());
        let on = Some((true, vec!["a".to_string()]));
        assert_eq!(set_op(1, "and", lhs(), rhs(), &on).len(), 1);
        assert_eq!(set_op(1, "unless", lhs(), rhs(), &on).len(), 1);
        assert_eq!(set_op(1, "or", lhs(), rhs(), &on).len(), 2);
    }

    #[test]
    fn quantile_interpolates() {
        assert_eq!(quantile(0.5, vec![3.0, 1.0, 2.0]), 2.0);
        assert_eq!(quantile(0.5, vec![1.0, 2.0]), 1.5);
        assert_eq!(quantile(1.5, vec![1.0]), f64::INFINITY);
        assert_eq!(variance([1.0, 3.0].into_iter()), 1.0);
    }
}
