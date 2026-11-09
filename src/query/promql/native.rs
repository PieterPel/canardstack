//! Native histograms in the PromQL evaluator: selectors, `rate` and the other range functions,
//! aggregation, arithmetic and the `histogram_*` functions over samples that are histograms. The
//! float code is untouched and is used whenever no histogram is involved; a value holds histograms
//! only as `Value::Samples`.
//!
//! The behaviour follows Prometheus (`promql/functions.go`, `promql/engine.go`).

use super::*;
use crate::query::native_histogram::Hist;
use crate::query::vector::{arith, signature, Applied, Card, Grouping, Matching};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone)]
pub enum Sample {
    F(f64),
    H(Arc<Hist>),
}

pub struct SSeries {
    pub labels: Labels,
    pub vals: Vec<Option<Sample>>,
}

type Points<'a> = &'a [(i64, Arc<Hist>)];

pub(super) fn to_samples(series: Vec<Series>) -> Vec<SSeries> {
    series
        .into_iter()
        .map(|s| SSeries {
            labels: s.labels,
            vals: s.vals.into_iter().map(|v| v.map(Sample::F)).collect(),
        })
        .collect()
}

/// The float series and the histogram series of a set of samples, each with only its own samples.
pub(super) fn split(samples: Vec<SSeries>) -> (Vec<Series>, Vec<SSeries>) {
    let (mut floats, mut hists) = (Vec::new(), Vec::new());
    for s in samples {
        let f: Vec<Option<f64>> = s
            .vals
            .iter()
            .map(|v| {
                if let Some(Sample::F(x)) = v {
                    Some(*x)
                } else {
                    None
                }
            })
            .collect();
        let h: Vec<Option<Sample>> = s
            .vals
            .iter()
            .map(|v| {
                if let Some(Sample::H(_)) = v {
                    v.clone()
                } else {
                    None
                }
            })
            .collect();
        if f.iter().any(Option::is_some) {
            floats.push(Series {
                labels: s.labels.clone(),
                vals: f,
            });
        }
        if h.iter().any(Option::is_some) {
            hists.push(SSeries {
                labels: s.labels,
                vals: h,
            });
        }
    }
    (floats, hists)
}

pub(super) fn combine(floats: Vec<Series>, hists: Vec<SSeries>) -> Vec<SSeries> {
    let mut out = to_samples(floats);
    out.extend(hists);
    out
}

pub(super) fn negate(samples: Vec<SSeries>) -> Vec<SSeries> {
    samples
        .into_iter()
        .map(|mut s| {
            s.labels.remove(METRIC_NAME);
            s.vals = s
                .vals
                .into_iter()
                .map(|v| {
                    v.map(|v| match v {
                        Sample::F(x) => Sample::F(-x),
                        Sample::H(h) => {
                            let mut h = (*h).clone();
                            h.mul(-1.0);
                            Sample::H(Arc::new(h))
                        }
                    })
                })
                .collect();
            s
        })
        .collect()
}

/// A rate (or increase, or delta) over histogram samples, extrapolated to the ends of the window
/// like a float one. `None` with fewer than two samples.
fn hist_rate(
    points: Points,
    range_start: i64,
    range_end: i64,
    is_counter: bool,
    is_rate: bool,
    range_secs: f64,
) -> Option<Hist> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() - 1;
    let (first_t, last_t) = (points[0].0, points[n].0);
    let last = &points[n].1;
    let mut prev: Hist = (*points[0].1).clone();
    if is_counter && points[1].1.detect_reset(&prev) {
        prev = Hist::empty(points[1].1.schema);
    }
    let mut min_schema = last.schema.min(prev.schema);
    if is_counter {
        for (_, h) in &points[1..n] {
            min_schema = min_schema.min(h.schema);
        }
    }
    let mut h = last.at_schema(min_schema);
    h.sub(&prev);
    if is_counter {
        for (_, curr) in &points[1..] {
            if curr.detect_reset(&prev) {
                h.add(&prev);
            }
            prev = (**curr).clone();
        }
    }

    let mut duration_to_start = (first_t - range_start) as f64 / 1000.0;
    let mut duration_to_end = (range_end - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_between = sampled_interval / n as f64;
    let threshold = average_between * 1.1;
    if duration_to_start >= threshold {
        duration_to_start = average_between / 2.0;
    }
    if is_counter {
        let mut duration_to_zero = duration_to_start;
        if h.count > 0.0 && points[0].1.count >= 0.0 {
            duration_to_zero = sampled_interval * (points[0].1.count / h.count);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= threshold {
        duration_to_end = average_between / 2.0;
    }
    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if is_rate {
        factor /= range_secs;
    }
    h.mul(factor);
    Some(h)
}

/// `irate` / `idelta` over the last two histogram samples.
fn hist_instant_value(points: Points, is_rate: bool) -> Option<Hist> {
    let [.., prev, last] = points else {
        return None;
    };
    let interval = last.0 - prev.0;
    if interval == 0 {
        return None;
    }
    let mut result = (*last.1).clone();
    if !is_rate || !last.1.detect_reset(&prev.1) {
        result.sub(&prev.1);
    }
    if is_rate {
        result.div(interval as f64 / 1000.0);
    }
    Some(result)
}

fn hist_sum(points: Points) -> Hist {
    let mut sum = (*points[0].1).clone();
    for (_, h) in &points[1..] {
        sum.add(h);
    }
    sum
}

impl Ev<'_> {
    /// The newest histogram at or before each step within the lookback.
    pub(super) fn hist_instant(&self, vs: &VectorSelector) -> ApiResult<Vec<SSeries>> {
        let off = offset_ms(vs);
        let mut out = Vec::new();
        for raw in self
            .raw(vs, LOOKBACK_MS)?
            .iter()
            .filter(|r| !r.hists.is_empty())
        {
            let mut vals: Vec<Option<Sample>> = vec![None; self.n()];
            for (i, slot) in vals.iter_mut().enumerate() {
                let t = self.grid.ts(i) - off;
                let idx = raw.hists.partition_point(|s| s.0 <= t);
                if idx > 0 && raw.hists[idx - 1].0 > t - LOOKBACK_MS {
                    *slot = Some(Sample::H(raw.hists[idx - 1].1.clone()));
                }
            }
            if vals.iter().any(Option::is_some) {
                out.push(SSeries {
                    labels: raw.labels.clone(),
                    vals,
                });
            }
        }
        Ok(out)
    }

    /// Applies `f` to the histogram samples in `(t - range, t]` at every step. `f` gets the samples,
    /// the window start and end (offset applied) and the range in seconds.
    fn hist_windowed(
        &self,
        arg: &Expr,
        keep_name: bool,
        f: &dyn Fn(Points, i64, i64, f64) -> Option<Sample>,
    ) -> ApiResult<Vec<SSeries>> {
        let Expr::MatrixSelector(ms) = arg else {
            return Ok(Vec::new());
        };
        let range = ms.range.as_millis() as i64;
        let off = offset_ms(&ms.vs);
        let mut out = Vec::new();
        for raw in self
            .raw(&ms.vs, range)?
            .iter()
            .filter(|r| !r.hists.is_empty())
        {
            let mut vals: Vec<Option<Sample>> = vec![None; self.n()];
            for (i, slot) in vals.iter_mut().enumerate() {
                let end = self.grid.ts(i) - off;
                let start = end - range;
                let lo = raw.hists.partition_point(|s| s.0 <= start);
                let hi = raw.hists.partition_point(|s| s.0 <= end);
                if lo < hi {
                    *slot = f(&raw.hists[lo..hi], start, end, range as f64 / 1000.0);
                }
            }
            if vals.iter().any(Option::is_some) {
                let mut labels = raw.labels.clone();
                if !keep_name {
                    labels.remove(METRIC_NAME);
                }
                out.push(SSeries { labels, vals });
            }
        }
        Ok(out)
    }

    /// The histogram side of a range function: nothing unless the argument is a range selector
    /// over histograms.
    pub(super) fn native_range(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Vec<SSeries>> {
        let Some(arg) = args.first() else {
            return Ok(Vec::new());
        };
        let hist = |h: Hist| Some(Sample::H(Arc::new(h)));
        match name {
            "rate" | "increase" | "delta" => {
                self.hist_windowed(arg, false, &|p, start, end, secs| {
                    hist_rate(p, start, end, name != "delta", name == "rate", secs).and_then(hist)
                })
            }
            "irate" | "idelta" => self.hist_windowed(arg, false, &|p, _, _, _| {
                hist_instant_value(p, name == "irate").and_then(hist)
            }),
            "resets" => self.hist_windowed(arg, false, &|p, _, _, _| {
                Some(Sample::F(
                    p.windows(2)
                        .filter(|w| w[1].1.detect_reset(&w[0].1))
                        .count() as f64,
                ))
            }),
            "changes" => self.hist_windowed(arg, false, &|p, _, _, _| {
                Some(Sample::F(
                    p.windows(2).filter(|w| !w[1].1.equals(&w[0].1)).count() as f64,
                ))
            }),
            "count_over_time" => {
                self.hist_windowed(arg, false, &|p, _, _, _| Some(Sample::F(p.len() as f64)))
            }
            "present_over_time" => {
                self.hist_windowed(arg, false, &|_, _, _, _| Some(Sample::F(1.0)))
            }
            "last_over_time" => self.hist_windowed(arg, true, &|p, _, _, _| {
                p.last().map(|s| Sample::H(s.1.clone()))
            }),
            "sum_over_time" => self.hist_windowed(arg, false, &|p, _, _, _| hist(hist_sum(p))),
            "avg_over_time" => self.hist_windowed(arg, false, &|p, _, _, _| {
                let mut avg = hist_sum(p);
                avg.div(p.len() as f64);
                hist(avg)
            }),
            _ => Ok(Vec::new()),
        }
    }

    /// Calls a function, adding what it does to histogram samples to what it does to floats.
    pub(super) fn call(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        match name {
            "rate" | "increase" | "delta" | "irate" | "idelta" | "resets" | "changes"
            | "count_over_time" | "present_over_time" | "last_over_time" | "sum_over_time"
            | "avg_over_time" => {
                let base = self.call_floats(name, args)?;
                let extra = self.native_range(name, args)?;
                Ok(match (base, extra.is_empty()) {
                    (base, true) => base,
                    (Value::Vector(floats), false) => Value::Samples(combine(floats, extra)),
                    (other, false) => other,
                })
            }
            "absent_over_time" | "absent" => self.absent_call(name, args),
            "histogram_count" | "histogram_sum" | "histogram_avg" | "histogram_stddev"
            | "histogram_stdvar" | "histogram_fraction" => self.native_instant(name, args),
            "histogram_quantile" => {
                let base = self.call_floats(name, args)?;
                let (Some(q), Some(v)) = (args.first(), args.get(1)) else {
                    return Ok(base);
                };
                let q = self.scalar_arg(q)?;
                let hists = self.hist_part(v)?;
                let Value::Vector(mut out) = base else {
                    return Ok(base);
                };
                out.extend(map_hists(hists, |h, i| h.quantile(q[i])));
                Ok(Value::Vector(out))
            }
            _ => self.call_floats(name, args),
        }
    }

    /// The histogram series an instant vector expression produces.
    fn hist_part(&self, expr: &Expr) -> ApiResult<Vec<SSeries>> {
        Ok(match self.eval(expr)? {
            Value::Samples(samples) => split(samples).1,
            _ => Vec::new(),
        })
    }

    /// `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_stddev`, `histogram_stdvar`
    /// and `histogram_fraction`: floats computed from each histogram sample.
    fn native_instant(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        let input = args
            .last()
            .ok_or_else(|| bad(format!("{name}: missing argument")))?;
        let hists = self.hist_part(input)?;
        let out = match name {
            "histogram_count" => map_hists(hists, |h, _| h.count),
            "histogram_sum" => map_hists(hists, |h, _| h.sum),
            "histogram_avg" => map_hists(hists, |h, _| h.avg()),
            "histogram_stdvar" => map_hists(hists, |h, _| h.variance()),
            "histogram_stddev" => map_hists(hists, |h, _| h.variance().sqrt()),
            _ => {
                let [lower, upper, _] = args else {
                    return Err(bad(
                        "histogram_fraction takes a lower and an upper bound and a vector",
                    ));
                };
                let (lower, upper) = (self.scalar_arg(lower)?, self.scalar_arg(upper)?);
                map_hists(hists, |h, i| h.fraction(lower[i], upper[i]))
            }
        };
        Ok(Value::Vector(out))
    }

    /// `absent()` and `absent_over_time()`: one series with value 1 where nothing, of either kind,
    /// is present.
    fn absent_call(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        let arg = args
            .first()
            .ok_or_else(|| bad(format!("{name}: missing argument")))?;
        let mut present = if name == "absent" {
            match self.eval(arg)? {
                Value::Vector(v) => v,
                Value::Samples(s) => presence(s),
                _ => return Err(bad("expected an instant vector")),
            }
        } else {
            match self.windowed(arg, false, &|_, _, _| Some(1.0))? {
                Value::Vector(v) => v,
                _ => Vec::new(),
            }
        };
        if name == "absent_over_time" {
            present.extend(presence(self.native_range("present_over_time", args)?));
        }
        let selector = match &**arg {
            Expr::MatrixSelector(ms) => Expr::VectorSelector(ms.vs.clone()),
            other => other.clone(),
        };
        Ok(self.absent(&selector, &present))
    }

    /// `sum`, `avg`, `count` and `group` including histogram samples; other aggregations ignore them.
    /// A group with both kinds of sample at a step has no result for `sum` and `avg`.
    pub(super) fn aggregate_samples(
        &self,
        op: &str,
        floats: Value,
        hists: Vec<SSeries>,
        grouping: &Option<Grouping>,
    ) -> ApiResult<Value> {
        let Value::Vector(floats) = floats else {
            return Ok(floats);
        };
        if !matches!(op, "sum" | "avg" | "count" | "group") || hists.is_empty() {
            return Ok(Value::Vector(floats));
        }
        let stubs: Vec<Series> = hists
            .iter()
            .map(|s| Series {
                labels: s.labels.clone(),
                vals: Vec::new(),
            })
            .collect();
        let mut out: BTreeMap<Labels, Vec<Option<Sample>>> = floats
            .into_iter()
            .map(|s| {
                (
                    s.labels,
                    s.vals.into_iter().map(|v| v.map(Sample::F)).collect(),
                )
            })
            .collect();
        for (labels, members) in vector::group(&stubs, grouping) {
            let slot = out.entry(labels).or_insert_with(|| vec![None; self.n()]);
            for (step, cell) in slot.iter_mut().enumerate() {
                let present: Vec<&Arc<Hist>> = members
                    .iter()
                    .filter_map(|&i| match &hists[i].vals[step] {
                        Some(Sample::H(h)) => Some(h),
                        _ => None,
                    })
                    .collect();
                if present.is_empty() {
                    continue;
                }
                *cell = match (op, cell.take()) {
                    ("count", Some(Sample::F(n))) => Some(Sample::F(n + present.len() as f64)),
                    ("count", None) => Some(Sample::F(present.len() as f64)),
                    ("group", _) => Some(Sample::F(1.0)),
                    // Floats and histograms cannot be summed or averaged together.
                    (_, Some(_)) => None,
                    ("sum" | "avg", None) => {
                        let mut sum = (**present[0]).clone();
                        for h in &present[1..] {
                            sum.add(h);
                        }
                        if op == "avg" {
                            sum.div(present.len() as f64);
                        }
                        Some(Sample::H(Arc::new(sum)))
                    }
                    _ => None,
                };
            }
        }
        Ok(Value::Samples(
            out.into_iter()
                .filter(|(_, v)| v.iter().any(Option::is_some))
                .map(|(labels, vals)| SSeries { labels, vals })
                .collect(),
        ))
    }

    /// A vector and a scalar; `scalar_left` says which side of the operator the scalar is on.
    fn samples_scalar(
        &self,
        series: Vec<SSeries>,
        scalar: &[f64],
        op: &str,
        comparison: bool,
        as_bool: bool,
        scalar_left: bool,
    ) -> Vec<SSeries> {
        series
            .into_iter()
            .filter_map(|mut s| {
                if !comparison || as_bool {
                    s.labels.remove(METRIC_NAME);
                }
                for (i, slot) in s.vals.iter_mut().enumerate() {
                    *slot = slot.take().and_then(|v| {
                        let c = Sample::F(scalar[i]);
                        if scalar_left {
                            elem(op, as_bool, &c, &v)
                        } else {
                            elem(op, as_bool, &v, &c)
                        }
                    });
                }
                s.vals.iter().any(Option::is_some).then_some(s)
            })
            .collect()
    }

    /// Binary operators where at least one side holds histogram samples.
    pub(super) fn binary_samples(
        &self,
        b: &parser::BinaryExpr,
        lhs: Value,
        rhs: Value,
    ) -> ApiResult<Value> {
        let op = b.op.to_string();
        let comparison = b.op.is_comparison_operator();
        let as_bool = b.modifier.as_ref().is_some_and(|m| m.return_bool);
        if b.op.is_set_operator() {
            return Err(unsupported(
                "set operators on native histograms are not supported",
            ));
        }
        let n = self.n();
        let as_samples = |v: Value| -> ApiResult<Vec<SSeries>> {
            match v {
                Value::Vector(s) => Ok(to_samples(s)),
                Value::Samples(s) => Ok(s),
                _ => Err(bad("binary operator on a scalar and a string")),
            }
        };
        match (lhs, rhs) {
            (Value::Scalar(c), other @ (Value::Vector(_) | Value::Samples(_))) => {
                Ok(Value::Samples(self.samples_scalar(
                    as_samples(other)?,
                    &c,
                    &op,
                    comparison,
                    as_bool,
                    true,
                )))
            }
            (other @ (Value::Vector(_) | Value::Samples(_)), Value::Scalar(c)) => {
                Ok(Value::Samples(self.samples_scalar(
                    as_samples(other)?,
                    &c,
                    &op,
                    comparison,
                    as_bool,
                    false,
                )))
            }
            (l, r) => {
                let (l, r) = (as_samples(l)?, as_samples(r)?);
                let m = matching(&b.modifier);
                let (many, one, flipped) = match m.card {
                    Card::OneToMany => (&r, &l, true),
                    _ => (&l, &r, false),
                };
                let mut out: BTreeMap<Labels, Vec<Option<Sample>>> = BTreeMap::new();
                for step in 0..n {
                    let mut table: HashMap<Labels, usize> = HashMap::new();
                    for (i, s) in one.iter().enumerate() {
                        if s.vals[step].is_some()
                            && table.insert(signature(&s.labels, &m.on), i).is_some()
                        {
                            return Err(exec_err("found duplicate series for the match group on the right hand side; many-to-one matching must be explicit (group_left/group_right)"));
                        }
                    }
                    let mut seen: HashSet<Labels> = HashSet::new();
                    for many_series in many {
                        let Some(mv) = &many_series.vals[step] else {
                            continue;
                        };
                        let sig = signature(&many_series.labels, &m.on);
                        let Some(&oi) = table.get(&sig) else { continue };
                        let Some(ov) = &one[oi].vals[step] else {
                            continue;
                        };
                        let (a, c) = if flipped { (ov, mv) } else { (mv, ov) };
                        let Some(value) = elem(&op, as_bool, a, c) else {
                            continue;
                        };
                        let mut labels = many_series.labels.clone();
                        if !comparison || as_bool {
                            labels.remove(METRIC_NAME);
                        }
                        if m.card == Card::OneToOne {
                            match &m.on {
                                Some((true, _)) => labels.retain(|k, _| sig.contains_key(k)),
                                Some((false, drop)) => drop.iter().for_each(|name| {
                                    labels.remove(name);
                                }),
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
                        if m.card == Card::OneToOne && !seen.insert(labels.clone()) {
                            return Err(exec_err("multiple matches for labels: grouping labels must ensure unique matches"));
                        }
                        out.entry(labels).or_insert_with(|| vec![None; n])[step] = Some(value);
                    }
                }
                Ok(Value::Samples(
                    out.into_iter()
                        .map(|(labels, vals)| SSeries { labels, vals })
                        .collect(),
                ))
            }
        }
    }
}

fn matching(modifier: &Option<parser::BinModifier>) -> Matching {
    let Some(m) = modifier else {
        return Matching::one_to_one();
    };
    let on = m.matching.as_ref().map(|l| match l {
        LabelModifier::Include(labels) => {
            (true, labels.labels.iter().map(|n| unescape(n)).collect())
        }
        LabelModifier::Exclude(labels) => {
            (false, labels.labels.iter().map(|n| unescape(n)).collect())
        }
    });
    let (card, include) = match &m.card {
        VectorMatchCardinality::ManyToOne(l) => (
            Card::ManyToOne,
            l.labels.iter().map(|n| unescape(n)).collect(),
        ),
        VectorMatchCardinality::OneToMany(l) => (
            Card::OneToMany,
            l.labels.iter().map(|n| unescape(n)).collect(),
        ),
        _ => (Card::OneToOne, Vec::new()),
    };
    Matching { on, card, include }
}

/// One binary operation between two samples; `None` drops the pair.
fn elem(op: &str, as_bool: bool, a: &Sample, b: &Sample) -> Option<Sample> {
    match (a, b) {
        (Sample::F(x), Sample::F(y)) => match arith(op, *x, *y) {
            Applied::Value(v) => Some(Sample::F(v)),
            Applied::Cmp(t) if as_bool => Some(Sample::F(f64::from(t))),
            Applied::Cmp(true) => Some(Sample::F(*x)),
            Applied::Cmp(false) => None,
        },
        (Sample::F(f), Sample::H(h)) if op == "*" => {
            let mut h = (**h).clone();
            h.mul(*f);
            Some(Sample::H(Arc::new(h)))
        }
        (Sample::H(h), Sample::F(f)) if op == "*" || op == "/" => {
            let mut h = (**h).clone();
            if op == "*" {
                h.mul(*f);
            } else {
                h.div(*f);
            }
            Some(Sample::H(Arc::new(h)))
        }
        (Sample::H(x), Sample::H(y)) => match op {
            "+" | "-" => {
                let mut h = (**x).clone();
                if op == "+" {
                    h.add(y);
                } else {
                    h.sub(y);
                }
                Some(Sample::H(Arc::new(h)))
            }
            "==" | "!=" => {
                let keep = x.equals(y) == (op == "==");
                if as_bool {
                    Some(Sample::F(f64::from(keep)))
                } else {
                    keep.then(|| a.clone())
                }
            }
            _ => None,
        },
        _ => None,
    }
}

/// Float series that mark where the given samples exist.
fn presence(samples: Vec<SSeries>) -> Vec<Series> {
    samples
        .into_iter()
        .map(|s| Series {
            labels: s.labels,
            vals: s.vals.into_iter().map(|v| v.map(|_| 1.0)).collect(),
        })
        .collect()
}

/// A float series per histogram series, computed from each histogram sample.
fn map_hists(hists: Vec<SSeries>, f: impl Fn(&Hist, usize) -> f64) -> Vec<Series> {
    hists
        .into_iter()
        .map(|mut s| {
            s.labels.remove(METRIC_NAME);
            let vals = s
                .vals
                .iter()
                .enumerate()
                .map(|(i, v)| match v {
                    Some(Sample::H(h)) => Some(f(h, i)),
                    _ => None,
                })
                .collect();
            Series {
                labels: s.labels,
                vals,
            }
        })
        .collect()
}
