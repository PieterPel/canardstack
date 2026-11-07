//! TraceQL metrics: `{ ... } | rate() by (...)` and the other `*_over_time` functions, evaluated
//! over the spans a pipeline selects and bucketed into steps.
//!
//! Like Tempo, a step covers `(t - step, t]` and is labelled with its end `t`; the range is aligned
//! outward to multiples of the step (with one extra leading step), and a span counts at its start
//! time. Unlike Tempo's frontend, the end of the range is not cut back to 30 seconds before now.

use super::eval::Eval;
use super::model::{self, Trace};
use super::value::{Regexes, Val};
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{TimeZone, Utc};
use std::collections::BTreeMap;
use traceql_syntax::ast::*;

/// Most steps one query returns.
pub const MAX_STEPS: i64 = 11_000;

#[derive(Clone, Copy)]
pub struct Range {
    pub start_ns: i64,
    pub step_ns: i64,
    pub steps: usize,
}

impl Range {
    /// The aligned range for a request: the start rounds down and the end up to the step.
    pub fn new(start_ns: i64, end_ns: i64, step_ns: i64) -> ApiResult<Range> {
        if step_ns <= 0 || end_ns <= start_ns {
            return Err(ApiError::new(400, "bad_data", "end must be after start and the step positive"));
        }
        // Like Tempo: the start rounds down and then moves back one more step, so the first
        // sample is labelled with the aligned start.
        let start = start_ns - start_ns.rem_euclid(step_ns) - step_ns;
        let end = end_ns + (step_ns - end_ns.rem_euclid(step_ns)) % step_ns;
        let steps = (end - start) / step_ns;
        if steps > MAX_STEPS {
            return Err(ApiError::new(
                400,
                "bad_data",
                format!("exceeded maximum resolution of {MAX_STEPS} points per series; try increasing the step"),
            ));
        }
        Ok(Range { start_ns: start, step_ns, steps: steps as usize })
    }

    pub fn end_ns(&self) -> i64 {
        self.start_ns + self.steps as i64 * self.step_ns
    }

    /// The step a timestamp falls in, `(start + i * step, start + (i + 1) * step]`.
    fn interval(&self, ts: i64) -> Option<usize> {
        if ts <= self.start_ns || ts > self.end_ns() {
            return None;
        }
        Some(((ts - self.start_ns - 1) / self.step_ns) as usize)
    }

    /// The label (end) of step `i`, in milliseconds.
    pub fn timestamp_ms(&self, i: usize) -> i64 {
        (self.start_ns + (i as i64 + 1) * self.step_ns) / 1_000_000
    }
}

pub struct Series {
    pub labels: Vec<(String, Val)>,
    /// One value per step; `None` where there is nothing to report.
    pub samples: Vec<Option<f64>>,
}

fn attribute_name(attr: &Attribute) -> String {
    let base = match attr.intrinsic {
        Some(i) => match i {
            Intrinsic::Duration => "duration",
            Intrinsic::Name => "name",
            Intrinsic::Status => "status",
            Intrinsic::StatusMessage => "statusMessage",
            Intrinsic::Kind => "kind",
            Intrinsic::Parent => "parent",
            Intrinsic::TraceRootSpan => "rootName",
            Intrinsic::TraceRootService => "rootServiceName",
            Intrinsic::TraceDuration => "traceDuration",
            Intrinsic::TraceId => "trace:id",
            Intrinsic::SpanId => "span:id",
            Intrinsic::ParentId => "span:parentID",
            Intrinsic::ChildCount => "childCount",
            Intrinsic::NestedSetLeft => "nestedSetLeft",
            Intrinsic::NestedSetRight => "nestedSetRight",
            Intrinsic::NestedSetParent => "nestedSetParent",
            Intrinsic::EventName => "event:name",
            Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
            Intrinsic::LinkTraceId => "link:traceID",
            Intrinsic::LinkSpanId => "link:spanID",
            Intrinsic::InstrumentationName => "instrumentation:name",
            Intrinsic::InstrumentationVersion => "instrumentation:version",
        }
        .to_string(),
        None => match attr.scope {
            Scope::None => format!(".{}", attr.name),
            Scope::Span => format!("span.{}", attr.name),
            Scope::Resource => format!("resource.{}", attr.name),
            Scope::Event => format!("event.{}", attr.name),
            Scope::Link => format!("link.{}", attr.name),
            Scope::Instrumentation => format!("instrumentation.{}", attr.name),
        },
    };
    if attr.parent {
        format!("parent.{base}")
    } else {
        base
    }
}

/// A key that tells label values apart.
fn value_key(v: &Option<Val>) -> String {
    format!("{v:?}")
}

#[derive(Clone)]
struct Cell {
    count: f64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Cell {
    fn new() -> Cell {
        Cell { count: 0.0, sum: 0.0, min: f64::NAN, max: f64::NAN }
    }

    fn observe(&mut self, v: f64) {
        self.count += 1.0;
        self.sum += v;
        self.min = if self.min.is_nan() { v } else { self.min.min(v) };
        self.max = if self.max.is_nan() { v } else { self.max.max(v) };
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Rate,
    Count,
    Min,
    Max,
    Sum,
    Avg,
    Histogram,
    Quantile,
}

/// The observations of one series: per step, and for histograms per bucket.
struct Bucketed {
    labels: Vec<Option<Val>>,
    /// Bucket upper bound (as bits, ascending for positive floats) to the cells per step.
    cells: BTreeMap<u64, Vec<Cell>>,
}

fn log2_bucket(v: u64) -> Option<f64> {
    if v < 2 {
        return None;
    }
    Some((1u64 << (64 - (v - 1).leading_zeros())) as f64)
}

/// Tempo's quantile over power-of-two buckets, interpolating exponentially inside a bucket.
fn log2_quantile(p: f64, buckets: &[(f64, f64)]) -> Option<f64> {
    let total: f64 = buckets.iter().map(|b| b.1).sum();
    if !(0.0..=1.0).contains(&p) || total == 0.0 {
        return None;
    }
    let max_samples = (p * total).ceil().max(1.0);
    let mut seen = 0.0;
    let mut at = 0;
    for (i, (max, count)) in buckets.iter().enumerate() {
        at = i;
        if seen + count > max_samples {
            break;
        }
        seen += count;
        if seen == max_samples {
            return Some(*max);
        }
    }
    let interp = (max_samples - seen) / buckets[at].1;
    let max_v = buckets[at].0.log2();
    let min_v = if at > 0 { buckets[at - 1].0.log2() } else { max_v - 1.0 };
    Some(2f64.powf(min_v + (max_v - min_v) * interp))
}

fn first_number(values: Vec<Val>) -> Option<f64> {
    values.into_iter().find_map(|v| match v {
        Val::Int(x) | Val::Dur(x) => Some(x as f64),
        Val::Float(x) => Some(x),
        _ => None,
    })
}

fn evaluate_first_stage(
    engine: &QueryEngine,
    storage: &Storage,
    query: &MetricsPipeline,
    range: Range,
) -> ApiResult<Vec<Series>> {
    let (kind, attr, quantiles) = match &query.agg.func {
        MetricsFn::Rate => (Kind::Rate, None, Vec::new()),
        MetricsFn::CountOverTime => (Kind::Count, None, Vec::new()),
        MetricsFn::MinOverTime(a) => (Kind::Min, Some(a), Vec::new()),
        MetricsFn::MaxOverTime(a) => (Kind::Max, Some(a), Vec::new()),
        MetricsFn::SumOverTime(a) => (Kind::Sum, Some(a), Vec::new()),
        MetricsFn::AvgOverTime(a) => (Kind::Avg, Some(a), Vec::new()),
        MetricsFn::HistogramOverTime(a) => (Kind::Histogram, Some(a), Vec::new()),
        MetricsFn::QuantileOverTime(a, qs) => (Kind::Quantile, Some(a), qs.clone()),
        MetricsFn::Compare { .. } => return Err(ApiError::new(400, "bad_data", "compare() is not supported")),
    };
    let name = match &query.agg.func {
        MetricsFn::Rate => "rate",
        MetricsFn::CountOverTime => "count_over_time",
        MetricsFn::MinOverTime(_) => "min_over_time",
        MetricsFn::MaxOverTime(_) => "max_over_time",
        MetricsFn::SumOverTime(_) => "sum_over_time",
        MetricsFn::AvgOverTime(_) => "avg_over_time",
        MetricsFn::HistogramOverTime(_) => "histogram_over_time",
        _ => "quantile_over_time",
    };
    let bucketed = matches!(kind, Kind::Histogram | Kind::Quantile);
    let from = Utc.timestamp_nanos(range.start_ns);
    let to = Utc.timestamp_nanos(range.end_ns() + 1_000_000);
    let traces = model::load(engine, storage, from, to)?;
    let regexes = Regexes::default();
    let value_field = attr.map(|a| Field::Attribute(a.clone()));
    let is_duration = attr.is_some_and(|a| a.intrinsic == Some(Intrinsic::Duration));
    let mut series: BTreeMap<String, Bucketed> = BTreeMap::new();
    for trace in &traces {
        let eval = Eval { trace, regexes: &regexes };
        let groups = eval.pipeline(&query.pipeline, eval.input())?;
        let mut spans: Vec<usize> = groups.into_iter().flatten().collect();
        spans.sort_unstable();
        spans.dedup();
        for i in spans {
            observe(&eval, trace, i, &range, &query.agg.by, value_field.as_ref(), is_duration, bucketed, kind, &mut series);
        }
    }

    let mut out = Vec::new();
    if series.is_empty() && query.agg.by.is_empty() && !bucketed {
        series.insert(String::new(), Bucketed { labels: Vec::new(), cells: BTreeMap::new() });
    }
    let by_names: Vec<String> = query.agg.by.iter().map(attribute_name).collect();
    for (_, s) in series {
        let mut labels: Vec<(String, Val)> = s
            .labels
            .iter()
            .zip(&by_names)
            .filter_map(|(v, n)| Some((n.clone(), v.clone()?)))
            .collect();
        if labels.is_empty() {
            match by_names.first() {
                Some(first) if !bucketed => labels.push((first.clone(), Val::Str("nil".into()))),
                None if !bucketed => labels.push(("__name__".into(), Val::Str(name.into()))),
                _ => {}
            }
        }
        let steps = range.steps;
        let cell_of = |cells: &Vec<Cell>, i: usize| cells[i].clone();
        match kind {
            Kind::Histogram => {
                for (bits, cells) in &s.cells {
                    let mut l = labels.clone();
                    l.push(("__bucket".into(), Val::Float(f64::from_bits(*bits))));
                    out.push(Series { labels: l, samples: (0..steps).map(|i| Some(cell_of(cells, i).count)).collect() });
                }
            }
            Kind::Quantile => {
                for &p in &quantiles {
                    let samples = (0..steps)
                        .map(|i| {
                            let buckets: Vec<(f64, f64)> = s
                                .cells
                                .iter()
                                .map(|(bits, cells)| (f64::from_bits(*bits), cells[i].count))
                                .filter(|b| b.1 > 0.0)
                                .collect();
                            log2_quantile(p, &buckets)
                        })
                        .collect();
                    let mut l = labels.clone();
                    l.push(("p".into(), Val::Float(p)));
                    out.push(Series { labels: l, samples });
                }
            }
            _ => {
                let cells = s.cells.get(&0);
                let samples = (0..steps)
                    .map(|i| {
                        let Some(cells) = cells else {
                            return matches!(kind, Kind::Rate | Kind::Count).then_some(0.0);
                        };
                        let c = &cells[i];
                        match kind {
                            Kind::Rate => Some(c.count * 1e9 / range.step_ns as f64),
                            Kind::Count => Some(c.count),
                            _ if c.count == 0.0 => None,
                            Kind::Min => Some(c.min),
                            Kind::Max => Some(c.max),
                            Kind::Sum => Some(c.sum),
                            _ => Some(c.sum / c.count),
                        }
                    })
                    .collect();
                out.push(Series { labels, samples });
            }
        }
    }
    out.sort_by(|a, b| format!("{:?}", a.labels).cmp(&format!("{:?}", b.labels)));
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn observe(
    eval: &Eval,
    trace: &Trace,
    i: usize,
    range: &Range,
    by: &[Attribute],
    value_field: Option<&Field>,
    is_duration: bool,
    bucketed: bool,
    kind: Kind,
    series: &mut BTreeMap<String, Bucketed>,
) {
    let span = &trace.spans[i];
    let Some(step) = range.interval(span.start_ns) else { return };
    // The value of the aggregated attribute (durations in seconds); `None` skips the span.
    let value = match value_field {
        None => Some(0.0),
        Some(_) if is_duration => Some(span.duration_ns as f64 / 1e9),
        Some(f) => first_number(eval.field(f, i)),
    };
    let bucket = if bucketed {
        let raw = match value_field {
            Some(_) if is_duration => span.duration_ns as f64,
            _ => value.unwrap_or(f64::NAN),
        };
        if raw.is_nan() || raw < 2.0 {
            return;
        }
        let Some(upper) = log2_bucket(raw as u64) else { return };
        // A duration's bucket is reported in seconds.
        Some(if is_duration { upper / 1e9 } else { upper })
    } else {
        None
    };
    if matches!(kind, Kind::Min | Kind::Max | Kind::Sum | Kind::Avg) && value.is_none() {
        return;
    }
    let labels: Vec<Option<Val>> = by.iter().map(|a| eval.field(&Field::Attribute(a.clone()), i).into_iter().next()).collect();
    let key = labels.iter().map(value_key).collect::<Vec<_>>().join("\u{1f}");
    let entry = series.entry(key).or_insert_with(|| Bucketed { labels, cells: BTreeMap::new() });
    let cells = entry.cells.entry(bucket.map_or(0, f64::to_bits)).or_insert_with(|| vec![Cell::new(); range.steps]);
    cells[step].observe(value.unwrap_or(0.0));
}

/// Evaluates a metrics query: one function with its second stages, or arithmetic over several.
pub fn evaluate(engine: &QueryEngine, storage: &Storage, query: &QueryKind, range: Range) -> ApiResult<Vec<Series>> {
    match query {
        QueryKind::Metrics(mp) => leaf(engine, storage, mp, range),
        QueryKind::MetricsMath { expr, second } => {
            let series = expression(engine, storage, expr, range)?;
            Ok(second_stages(series, second, range.steps))
        }
        _ => Err(ApiError::new(400, "bad_data", "not a metrics query")),
    }
}

fn leaf(engine: &QueryEngine, storage: &Storage, mp: &MetricsPipeline, range: Range) -> ApiResult<Vec<Series>> {
    let series = evaluate_first_stage(engine, storage, mp, range)?;
    Ok(second_stages(series, &mp.second, range.steps))
}

fn expression(engine: &QueryEngine, storage: &Storage, expr: &MetricsExpr, range: Range) -> ApiResult<Vec<Series>> {
    match expr {
        MetricsExpr::Pipeline(mp) => leaf(engine, storage, mp, range),
        MetricsExpr::Scalar { op, value, expr, scalar_on_left } => {
            let mut series = expression(engine, storage, expr, range)?;
            for s in &mut series {
                for v in &mut s.samples {
                    *v = v.map(|x| if *scalar_on_left { arithmetic(*op, *value, x) } else { arithmetic(*op, x, *value) }).filter(|x| !x.is_nan());
                }
            }
            Ok(series)
        }
        MetricsExpr::Binary { op, lhs, rhs } => {
            let l = expression(engine, storage, lhs, range)?;
            let r = expression(engine, storage, rhs, range)?;
            Ok(binary(*op, l, r))
        }
    }
}

fn arithmetic(op: Op, l: f64, r: f64) -> f64 {
    match op {
        Op::Add => l + r,
        Op::Sub => l - r,
        Op::Mul => l * r,
        Op::Div if r != 0.0 => l / r,
        _ => f64::NAN,
    }
}

fn op_text(op: Op) -> &'static str {
    match op {
        Op::Add => "+",
        Op::Sub => "-",
        Op::Mul => "*",
        _ => "/",
    }
}

/// What tells two series apart when combining: their labels without the metric name.
fn match_key(s: &Series) -> Vec<(String, String)> {
    s.labels.iter().filter(|(k, _)| k != "__name__").map(|(k, v)| (k.clone(), format!("{v:?}"))).collect()
}

fn name_of(s: &Series) -> Option<&str> {
    s.labels.iter().find_map(|(k, v)| match (k.as_str(), v) {
        ("__name__", Val::Str(name)) => Some(name.as_str()),
        _ => None,
    })
}

/// Combines two sets like Tempo: series pair up by labels, and a series without labels pairs with
/// every series of the other side.
fn binary(op: Op, lhs: Vec<Series>, rhs: Vec<Series>) -> Vec<Series> {
    let keyed = |set: &[Series]| -> BTreeMap<Vec<(String, String)>, usize> { set.iter().enumerate().map(|(i, s)| (match_key(s), i)).collect() };
    let (lk, rk) = (keyed(&lhs), keyed(&rhs));
    let none = Vec::new();
    let target: Vec<Vec<(String, String)>> = if rk.contains_key(&none) { lk.keys().cloned().collect() } else { rk.keys().cloned().collect() };
    let mut out = Vec::new();
    for key in target {
        let (Some(&li), Some(&ri)) = (lk.get(&key).or_else(|| lk.get(&none)), rk.get(&key).or_else(|| rk.get(&none))) else {
            continue;
        };
        let (l, r) = (&lhs[li], &rhs[ri]);
        let n = l.samples.len().min(r.samples.len());
        let samples = (0..n)
            .map(|i| Some(arithmetic(op, l.samples[i].unwrap_or(f64::NAN), r.samples[i].unwrap_or(f64::NAN))).filter(|x| !x.is_nan()))
            .collect();
        let mut labels: Vec<(String, Val)> = l.labels.iter().filter(|(k, _)| k != "__name__").cloned().collect();
        for (k, v) in &r.labels {
            if k != "__name__" && !labels.iter().any(|(name, _)| name == k) {
                labels.push((k.clone(), v.clone()));
            }
        }
        if let (Some(ln), Some(rn)) = (name_of(l), name_of(r)) {
            labels.push(("__name__".into(), Val::Str(format!("({ln} {} {rn})", op_text(op)))));
        }
        out.push(Series { labels, samples });
    }
    out
}

fn second_stages(mut series: Vec<Series>, stages: &[SecondStage], steps: usize) -> Vec<Series> {
    for stage in stages {
        series = match stage {
            SecondStage::TopK(n) => top(series, *n, steps, true),
            SecondStage::BottomK(n) => top(series, *n, steps, false),
            SecondStage::Filter { op, value } => filter(series, *op, value),
        };
    }
    series
}

fn filter(series: Vec<Series>, op: Op, value: &Static) -> Vec<Series> {
    let limit = match value {
        Static::Int(v) => *v as f64,
        Static::Float(v) => *v,
        Static::Duration(v) => *v as f64 / 1e9,
        _ => return series,
    };
    series
        .into_iter()
        .filter_map(|mut s| {
            let mut any = false;
            for v in &mut s.samples {
                let keep = v.is_some_and(|x| match op {
                    Op::Gt => x > limit,
                    Op::Ge => x >= limit,
                    Op::Lt => x < limit,
                    Op::Le => x <= limit,
                    Op::Eq => x == limit,
                    _ => x != limit,
                });
                if !keep {
                    *v = None;
                }
                any |= keep;
            }
            any.then_some(s)
        })
        .collect()
}

fn label_text(v: &Val) -> String {
    match v {
        Val::Str(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// `topk` / `bottomk`: at every step keep the `n` highest (or lowest) series, ties going to the
/// alphabetically earlier labels. With at most `n` series nothing changes.
fn top(series: Vec<Series>, n: i64, steps: usize, highest: bool) -> Vec<Series> {
    let n = n.max(0) as usize;
    if series.len() <= n {
        return series;
    }
    let keys: Vec<Vec<String>> = series.iter().map(|s| s.labels.iter().filter(|(k, _)| k != "__name__").map(|(_, v)| label_text(v)).collect()).collect();
    let mut kept: Vec<Vec<Option<f64>>> = vec![vec![None; steps]; series.len()];
    for step in 0..steps {
        let mut at: Vec<(usize, f64)> = series.iter().enumerate().filter_map(|(i, s)| Some((i, s.samples.get(step).copied().flatten().filter(|x| !x.is_nan())?))).collect();
        at.sort_by(|a, b| {
            let by_value = if highest { b.1.partial_cmp(&a.1) } else { a.1.partial_cmp(&b.1) };
            by_value.unwrap_or(std::cmp::Ordering::Equal).then_with(|| keys[a.0].cmp(&keys[b.0]))
        });
        for (i, v) in at.into_iter().take(n) {
            kept[i][step] = Some(v);
        }
    }
    series
        .into_iter()
        .zip(kept)
        .filter(|(_, samples)| samples.iter().any(Option::is_some))
        .map(|(s, samples)| Series { labels: s.labels, samples })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    fn series(name: &str, samples: &[Option<f64>]) -> Series {
        Series { labels: vec![("svc".into(), Val::Str(name.into()))], samples: samples.to_vec() }
    }

    #[test]
    fn range_aligns_outward_with_an_extra_leading_step() {
        let r = Range::new(105 * SEC, 131 * SEC, 10 * SEC).unwrap();
        assert_eq!(r.start_ns, 90 * SEC);
        assert_eq!(r.end_ns(), 140 * SEC);
        assert_eq!(r.steps, 5);
        assert_eq!(r.timestamp_ms(0), 100_000);
        // `(start, end]`: a span on a boundary belongs to the step that ends there.
        assert_eq!(r.interval(100 * SEC), Some(0));
        assert_eq!(r.interval(100 * SEC + 1), Some(1));
        assert_eq!(r.interval(90 * SEC), None);
        assert_eq!(r.interval(140 * SEC), Some(4));
        assert_eq!(r.interval(140 * SEC + 1), None);
        assert!(Range::new(0, 11_000 * 10 * SEC + 1, 10 * SEC).is_err());
    }

    #[test]
    fn log2_buckets_round_up_to_powers_of_two() {
        assert_eq!(log2_bucket(1), None);
        assert_eq!(log2_bucket(2), Some(2.0));
        assert_eq!(log2_bucket(3), Some(4.0));
        assert_eq!(log2_bucket(1024), Some(1024.0));
        assert_eq!(log2_bucket(1025), Some(2048.0));
    }

    #[test]
    fn quantiles_interpolate_inside_a_bucket() {
        let buckets = [(2.0, 1.0), (4.0, 1.0), (8.0, 2.0)];
        assert_eq!(log2_quantile(1.0, &buckets), Some(8.0));
        assert_eq!(log2_quantile(0.5, &buckets), Some(4.0));
        let q = log2_quantile(0.75, &buckets).unwrap();
        assert!(q > 4.0 && q < 8.0, "{q}");
        assert_eq!(log2_quantile(1.5, &buckets), None);
        assert_eq!(log2_quantile(0.5, &[]), None);
    }

    #[test]
    fn topk_picks_per_step_and_breaks_ties_alphabetically() {
        let input = vec![
            series("a", &[Some(1.0), Some(5.0)]),
            series("b", &[Some(2.0), Some(1.0)]),
            series("c", &[Some(2.0), Some(1.0)]),
        ];
        let out = top(input, 1, 2, true);
        // Step 0: b and c tie at 2, b is alphabetically earlier. Step 1: a. Series c never wins.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].samples, vec![None, Some(5.0)]);
        assert_eq!(out[1].samples, vec![Some(2.0), None]);
    }

    #[test]
    fn filters_drop_samples_and_empty_series() {
        let out = filter(vec![series("a", &[Some(1.0), Some(5.0)]), series("b", &[Some(1.0), None])], Op::Gt, &Static::Int(3));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].samples, vec![None, Some(5.0)]);
    }

    #[test]
    fn binary_pairs_series_by_labels_and_fans_out_unlabelled_ones() {
        let named = |n: &str, v: f64| Series { labels: vec![("__name__".into(), Val::Str(n.into()))], samples: vec![Some(v)] };
        let out = binary(Op::Add, vec![named("rate", 2.0)], vec![named("count_over_time", 3.0)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].samples, vec![Some(5.0)]);
        assert_eq!(name_of(&out[0]), Some("(rate + count_over_time)"));
        let grouped = vec![series("a", &[Some(10.0)]), series("b", &[Some(20.0)])];
        let out = binary(Op::Div, grouped, vec![named("rate", 5.0)]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].samples, vec![Some(2.0)]);
        assert_eq!(out[1].samples, vec![Some(4.0)]);
        assert!(binary(Op::Div, vec![series("a", &[Some(1.0)])], vec![series("b", &[Some(1.0)])]).is_empty());
        assert!(arithmetic(Op::Div, 1.0, 0.0).is_nan());
    }
}
