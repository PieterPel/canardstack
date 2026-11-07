//! LogQL metric queries: range aggregations over the lines a log query selects, then vector
//! aggregations, binary operators and `label_replace` on the resulting series.
//!
//! The lines are fetched once for the whole evaluation window and grouped into series; each step
//! then looks at the samples in `(t - range - offset, t - offset]`.

use super::fetch::fetch;
use super::pipeline::{anchored, Pipeline};
use super::values::{parse_bytes, parse_duration_secs};
use crate::query::prom_labels::Labels;
use crate::query::vector::{self, extrapolated, quantile, variance, Card, Grid, Grouping, Matching, Series};
use crate::query::QueryEngine;
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use chrono::{TimeZone, Utc};
use logql_syntax::ast::{self, BinOp, Conv, Expr, MatchOp, RangeAgg, RangeOp, VecOp};
use std::collections::BTreeMap;

pub enum Value {
    Scalar(Vec<f64>),
    Vector(Vec<Series>),
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "bad_data", msg)
}

pub fn evaluate(engine: &QueryEngine, storage: &Storage, expr: &Expr, grid: Grid) -> ApiResult<Value> {
    Ev { engine, storage, grid }.eval(expr)
}

struct Ev<'a> {
    engine: &'a QueryEngine,
    storage: &'a Storage,
    grid: Grid,
}

struct Sample {
    ts_ns: i64,
    value: f64,
    bytes: f64,
}

fn grouping(g: &Option<ast::Grouping>) -> Option<Grouping> {
    g.as_ref().map(|g| Grouping { by: g.by, labels: g.labels.clone() })
}

fn op_name(op: BinOp) -> &'static str {
    match op {
        BinOp::Or => "or",
        BinOp::And => "and",
        BinOp::Unless => "unless",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Pow => "^",
        BinOp::Eq => "==",
        BinOp::Ne => "!=",
        BinOp::Gt => ">",
        BinOp::Ge => ">=",
        BinOp::Lt => "<",
        BinOp::Le => "<=",
    }
}

impl Ev<'_> {
    fn n(&self) -> usize {
        self.grid.steps
    }

    fn eval(&self, expr: &Expr) -> ApiResult<Value> {
        match expr {
            Expr::Number(n) => Ok(Value::Scalar(vec![*n; self.n()])),
            Expr::VectorLit(n) => Ok(Value::Vector(vec![Series { labels: Labels::new(), vals: vec![Some(*n); self.n()] }])),
            Expr::Log(_) => Err(bad("a log query is not a metric query: wrap it in a range aggregation such as count_over_time")),
            Expr::Range(r) => Ok(Value::Vector(self.range(r)?)),
            Expr::Vector(v) => {
                let input = self.vector(&v.expr)?;
                if matches!(v.op, VecOp::Sort | VecOp::SortDesc) {
                    return Ok(Value::Vector(sorted(input, v.op == VecOp::SortDesc)));
                }
                let name = match v.op {
                    VecOp::Sum => "sum",
                    VecOp::Avg => "avg",
                    VecOp::Min => "min",
                    VecOp::Max => "max",
                    VecOp::Count => "count",
                    VecOp::Stddev => "stddev",
                    VecOp::Stdvar => "stdvar",
                    VecOp::Topk => "topk",
                    _ => "bottomk",
                };
                let k = v.param.map(|k| vec![k; self.n()]);
                vector::aggregate(name, self.n(), input, &grouping(&v.grouping), k.as_deref())
                    .map(Value::Vector)
                    .map_err(bad)
            }
            Expr::LabelReplace(l) => {
                let mut series = self.vector(&l.expr)?;
                let rx = anchored(&l.regex)?;
                for s in &mut series {
                    let value = s.labels.get(&l.src).cloned().unwrap_or_default();
                    if let Some(caps) = rx.captures(&value) {
                        let mut out = String::new();
                        caps.expand(&l.replacement, &mut out);
                        if out.is_empty() {
                            s.labels.remove(&l.dst);
                        } else {
                            s.labels.insert(l.dst.clone(), out);
                        }
                    }
                }
                Ok(Value::Vector(series))
            }
            Expr::Binary(b) => self.binary(b),
        }
    }

    fn vector(&self, expr: &Expr) -> ApiResult<Vec<Series>> {
        match self.eval(expr)? {
            Value::Vector(v) => Ok(v),
            Value::Scalar(_) => Err(bad("expected a vector, found a scalar")),
        }
    }

    fn binary(&self, b: &ast::Binary) -> ApiResult<Value> {
        let op = op_name(b.op);
        let comparison = b.op.is_comparison();
        let n = self.n();
        let lhs = self.eval(&b.lhs)?;
        let rhs = self.eval(&b.rhs)?;
        match (lhs, rhs) {
            (Value::Scalar(a), Value::Scalar(c)) => {
                if comparison && !b.bool_modifier {
                    return Err(bad("comparisons between scalars must use the bool modifier"));
                }
                Ok(Value::Scalar(
                    (0..n)
                        .map(|i| match vector::arith(op, a[i], c[i]) {
                            vector::Applied::Value(v) => v,
                            vector::Applied::Cmp(t) => f64::from(t),
                        })
                        .collect(),
                ))
            }
            (Value::Vector(s), Value::Scalar(c)) => {
                Ok(Value::Vector(vector::vector_scalar(s, &c, op, comparison, b.bool_modifier, false)))
            }
            (Value::Scalar(c), Value::Vector(s)) => {
                Ok(Value::Vector(vector::vector_scalar(s, &c, op, comparison, b.bool_modifier, true)))
            }
            (Value::Vector(l), Value::Vector(r)) => {
                let matching = Matching {
                    on: b.matching.clone(),
                    card: match &b.group {
                        Some((true, _)) => Card::ManyToOne,
                        Some((false, _)) => Card::OneToMany,
                        None => Card::OneToOne,
                    },
                    include: b.group.as_ref().map(|(_, labels)| labels.clone()).unwrap_or_default(),
                };
                if b.op.is_set() {
                    Ok(Value::Vector(vector::set_op(n, op, l, r, &matching.on)))
                } else {
                    vector::vector_vector(n, op, comparison, b.bool_modifier, l, r, &matching)
                        .map(Value::Vector)
                        .map_err(|e| ApiError::new(422, "execution", e))
                }
            }
        }
    }

    fn range(&self, r: &RangeAgg) -> ApiResult<Vec<Series>> {
        let pipeline = Pipeline::compile(&r.log.stages)?;
        let at = |ms: i64| Utc.timestamp_millis_opt(ms).single().ok_or_else(|| bad("time out of range"));
        let entries = fetch(
            self.engine,
            self.storage,
            &r.log.matchers,
            &r.log.stages,
            at(self.grid.start_ms - r.offset_ms - r.range_ms)?,
            at(self.grid.end_ms() - r.offset_ms + 1)?,
        )?;
        let group = grouping(&r.grouping);
        let mut by_series: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
        for entry in &entries {
            let Some((line, mut labels)) = pipeline.process(entry.ts_ns, &entry.line, &entry.labels) else { continue };
            if let Some(kind) = labels.get("__error__") {
                let detail = labels.get("__error_details__").map(String::as_str).unwrap_or("");
                return Err(bad(format!(
                    "pipeline error: '{kind}' ({detail}). Use a label filter to intentionally skip this error. (e.g | __error__!=\"{kind}\")"
                )));
            }
            // Loki groups `by (...)` on the labels as they are before the unwrapped one is
            // removed, so `by (status)` can keep the label that was unwrapped.
            let before_unwrap = matches!(group, Some(Grouping { by: true, .. })).then(|| labels.clone());
            let value = match &r.unwrap {
                Some(u) => unwrap_value(u, &mut labels)?,
                None => 1.0,
            };
            let key = match &before_unwrap {
                Some(before) => vector::group_key(before, &group),
                None => vector::group_key_or_all(&labels, &group),
            };
            by_series.entry(key).or_default().push(Sample { ts_ns: entry.ts_ns, value, bytes: line.len() as f64 });
        }
        if r.op == RangeOp::AbsentOverTime {
            return Ok(self.absent(r, &by_series));
        }
        let range_secs = r.range_ms as f64 / 1000.0;
        let mut out = Vec::new();
        for (labels, samples) in by_series {
            let vals: Vec<Option<f64>> = (0..self.n())
                .map(|i| {
                    let end_ms = self.grid.ts(i) - r.offset_ms;
                    let window = window(&samples, end_ms - r.range_ms, end_ms);
                    if window.is_empty() {
                        return None;
                    }
                    apply(r, window, range_secs, end_ms)
                })
                .collect();
            if vals.iter().any(Option::is_some) {
                out.push(Series { labels, vals });
            }
        }
        Ok(out)
    }

    /// `absent_over_time`: 1 at every step where no line at all is in the window.
    fn absent(&self, r: &RangeAgg, by_series: &BTreeMap<Labels, Vec<Sample>>) -> Vec<Series> {
        let labels: Labels = r
            .log
            .matchers
            .iter()
            .filter(|m| m.op == MatchOp::Eq)
            .map(|m| (m.name.clone(), m.value.clone()))
            .collect();
        let vals = (0..self.n())
            .map(|i| {
                let end_ms = self.grid.ts(i) - r.offset_ms;
                by_series
                    .values()
                    .all(|s| window(s, end_ms - r.range_ms, end_ms).is_empty())
                    .then_some(1.0)
            })
            .collect();
        vec![Series { labels, vals }]
    }
}

/// The samples in `(start_ms, end_ms]`.
fn window(samples: &[Sample], start_ms: i64, end_ms: i64) -> &[Sample] {
    let lo = samples.partition_point(|s| s.ts_ns <= start_ms * 1_000_000);
    let hi = samples.partition_point(|s| s.ts_ns <= end_ms * 1_000_000);
    &samples[lo..hi]
}

fn unwrap_value(u: &ast::Unwrap, labels: &mut Labels) -> ApiResult<f64> {
    let err = |detail: String| bad(format!("pipeline error: 'SampleExtractionErr' ({detail}). Use a label filter to intentionally skip this error."));
    let raw = labels.remove(&u.label).ok_or_else(|| err(format!("label {} is missing", u.label)))?;
    let parsed = match u.conv {
        Conv::None => raw.parse::<f64>().ok(),
        Conv::Bytes => parse_bytes(&raw),
        Conv::Duration | Conv::DurationSeconds => parse_duration_secs(&raw),
    };
    parsed.ok_or_else(|| err(format!("label {} has the value {raw:?}, which cannot be converted", u.label)))
}

fn apply(r: &RangeAgg, win: &[Sample], range_secs: f64, end_ms: i64) -> Option<f64> {
    let n = win.len() as f64;
    let values = || win.iter().map(|s| s.value);
    let sum_bytes: f64 = win.iter().map(|s| s.bytes).sum();
    Some(match r.op {
        RangeOp::CountOverTime => n,
        RangeOp::Rate if r.unwrap.is_some() => values().sum::<f64>() / range_secs,
        RangeOp::Rate => n / range_secs,
        RangeOp::BytesOverTime => sum_bytes,
        RangeOp::BytesRate => sum_bytes / range_secs,
        RangeOp::RateCounter => {
            let points: Vec<(i64, f64)> = win.iter().map(|s| (s.ts_ns / 1_000_000, s.value)).collect();
            // Loki gives a window with fewer than two samples the rate 0.
            extrapolated(&points, end_ms - r.range_ms, end_ms, true, true).unwrap_or(0.0)
        }
        RangeOp::SumOverTime => values().sum(),
        RangeOp::AvgOverTime => values().sum::<f64>() / n,
        RangeOp::MinOverTime => values().fold(f64::INFINITY, f64::min),
        RangeOp::MaxOverTime => values().fold(f64::NEG_INFINITY, f64::max),
        RangeOp::StddevOverTime => variance(values()).sqrt(),
        RangeOp::StdvarOverTime => variance(values()),
        RangeOp::QuantileOverTime => quantile(r.param.unwrap_or(f64::NAN), values().collect()),
        RangeOp::FirstOverTime => win.first()?.value,
        RangeOp::LastOverTime => win.last()?.value,
        RangeOp::AbsentOverTime => return None,
    })
}

fn sorted(mut series: Vec<Series>, descending: bool) -> Vec<Series> {
    let key = |s: &Series| s.vals.iter().rev().find_map(|v| *v).unwrap_or(f64::NAN);
    series.sort_by(|a, b| key(a).total_cmp(&key(b)));
    if descending {
        series.reverse();
    }
    series
}
