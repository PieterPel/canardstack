//! Selector collection and bounded sample fetching.

use super::*;
use crate::db::sql::{quote, time_predicate};
use crate::query::prom_labels::{labels_from_row, INSTANCE_SQL, JOB_SQL, SERIES_COLUMNS};
use crate::signal::StorageSignal;
use chrono::{TimeZone, Utc};

pub(super) struct Req {
    pub(super) key: String,
    vs: VectorSelector,
    window_ms: i64,
}

pub(super) struct Raw {
    pub(super) labels: Labels,
    pub(super) samples: Vec<(i64, f64)>,
}

pub(super) fn offset_ms(vs: &VectorSelector) -> i64 {
    match &vs.offset {
        Some(Offset::Pos(d)) => d.as_millis() as i64,
        Some(Offset::Neg(d)) => -(d.as_millis() as i64),
        None => 0,
    }
}

pub(super) fn selector_key(vs: &VectorSelector, window_ms: i64) -> String {
    format!("{vs}#{window_ms}")
}

pub(super) fn collect(expr: &Expr, out: &mut Vec<Req>) -> ApiResult<()> {
    match expr {
        Expr::VectorSelector(vs) => push_req(vs, LOOKBACK_MS, out),
        Expr::MatrixSelector(ms) => push_req(&ms.vs, ms.range.as_millis() as i64, out),
        Expr::Call(call) => call.args.args.iter().try_for_each(|a| collect(a, out)),
        Expr::Aggregate(agg) => {
            collect(&agg.expr, out)?;
            agg.param.as_ref().map_or(Ok(()), |p| collect(p, out))
        }
        Expr::Binary(b) => {
            collect(&b.lhs, out)?;
            collect(&b.rhs, out)
        }
        Expr::Paren(p) => collect(&p.expr, out),
        Expr::Unary(u) => collect(&u.expr, out),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => Ok(()),
        Expr::Subquery(_) => Err(unsupported("subqueries are not supported")),
        _ => Err(unsupported("unsupported expression")),
    }
}

fn push_req(vs: &VectorSelector, window_ms: i64, out: &mut Vec<Req>) -> ApiResult<()> {
    if vs.at.is_some() {
        return Err(unsupported("the @ modifier is not supported"));
    }
    if !vs.matchers.or_matchers.is_empty() {
        return Err(unsupported("`or` between label matchers is not supported"));
    }
    let key = selector_key(vs, window_ms);
    if !out.iter().any(|r| r.key == key) {
        out.push(Req {
            key,
            vs: vs.clone(),
            window_ms,
        });
    }
    Ok(())
}

enum Test {
    Eq(String),
    Ne(String),
    Re(Regex),
    NotRe(Regex),
}

struct Matcher {
    /// Canonical label name, or `__name__`.
    label: String,
    test: Test,
}

impl Matcher {
    fn ok(&self, value: &str) -> bool {
        match &self.test {
            Test::Eq(v) => value == v,
            Test::Ne(v) => value != v,
            Test::Re(r) => r.is_match(value),
            Test::NotRe(r) => !r.is_match(value),
        }
    }
}

pub(super) fn anchored(pattern: &str) -> ApiResult<Regex> {
    Regex::new(&format!("^(?:{pattern})$")).map_err(|e| bad(format!("invalid regex: {e}")))
}

struct Compiled {
    matchers: Vec<Matcher>,
    /// Stored metric names the selector can match, when it names the metric by equality.
    names: Vec<String>,
    display_name: Option<String>,
}

fn compile(vs: &VectorSelector) -> ApiResult<Compiled> {
    let mut raw: Vec<(String, MatchOp, String)> = vs
        .matchers
        .matchers
        .iter()
        .map(|m| (unescape(&m.name), m.op.clone(), m.value.clone()))
        .collect();
    if let Some(name) = &vs.name {
        let name = unescape(name);
        if !raw
            .iter()
            .any(|(l, op, v)| l == METRIC_NAME && matches!(op, MatchOp::Equal) && *v == name)
        {
            raw.push((METRIC_NAME.to_string(), MatchOp::Equal, name));
        }
    }
    let mut compiled = Compiled {
        matchers: Vec::new(),
        names: Vec::new(),
        display_name: None,
    };
    for (label, op, value) in raw {
        if label == METRIC_NAME {
            if let MatchOp::Equal = op {
                if compiled.names.is_empty() {
                    compiled.names.push(value.clone());
                    compiled.display_name = Some(value);
                }
                continue;
            }
        }
        let test = match op {
            MatchOp::Equal => Test::Eq(value),
            MatchOp::NotEqual => Test::Ne(value),
            MatchOp::Re(_) => Test::Re(anchored(&value)?),
            MatchOp::NotRe(_) => Test::NotRe(anchored(&value)?),
        };
        compiled.matchers.push(Matcher { label, test });
    }
    Ok(compiled)
}

pub(super) fn fetch(
    engine: &QueryEngine,
    storage: &Storage,
    reqs: &[Req],
    grid: &Grid,
) -> ApiResult<HashMap<String, Vec<Raw>>> {
    struct Plan {
        key: String,
        sql: String,
        compiled: Compiled,
    }
    let mut plans = Vec::new();
    for req in reqs {
        let compiled = compile(&req.vs)?;
        let off = offset_ms(&req.vs);
        let from = Utc
            .timestamp_millis_opt(grid.start_ms - off - req.window_ms)
            .single()
            .ok_or_else(|| bad("time out of range"))?;
        let to = Utc
            .timestamp_millis_opt(grid.end_ms() - off + 1)
            .single()
            .ok_or_else(|| bad("time out of range"))?;
        let mut filters = vec![time_predicate("time_unix_nano", from, to)];
        if !compiled.names.is_empty() {
            let names = compiled
                .names
                .iter()
                .map(|n| quote(n))
                .collect::<Vec<_>>()
                .join(", ");
            filters.push(format!("name IN ({names})"));
        }
        for m in &compiled.matchers {
            if let Test::Eq(value) = &m.test {
                if value.is_empty() {
                    continue;
                }
                // job and instance are columns; other labels come out of a JSON attribute blob whose
                // keys are sanitised, so those are matched after the fetch.
                match m.label.as_str() {
                    "job" => filters.push(format!("{JOB_SQL} = {}", quote(value))),
                    "instance" => filters.push(format!("{INSTANCE_SQL} = {}", quote(value))),
                    _ => {}
                }
            }
        }
        let select = |signal: StorageSignal| {
            format!(
                "SELECT name, concat_ws(chr(31), coalesce(service_name, ''), coalesce(service_namespace, ''), coalesce(service_instance_id, ''), coalesce(CAST(resource_attributes AS VARCHAR), ''), coalesce(scope_name, ''), coalesce(CAST(metric_attributes AS VARCHAR), '')) AS sid, epoch_ms(time_unix_nano) AS t, coalesce(double_value, int_value::DOUBLE) AS v, {SERIES_COLUMNS} FROM {{prefix}}{} WHERE {}",
                signal.as_str(),
                filters.join(" AND ")
            )
        };
        let sql = format!(
            "{} UNION ALL {} LIMIT {}",
            select(StorageSignal::MetricGauge),
            select(StorageSignal::MetricSum),
            MAX_SAMPLES + 1
        );
        plans.push(Plan {
            key: req.key.clone(),
            sql,
            compiled,
        });
    }
    let fetched = engine.run_interactive(storage, |conn, prefix| {
        let mut out: HashMap<String, Vec<Raw>> = HashMap::new();
        let mut total = 0usize;
        for plan in &plans {
            let mut stmt = conn.prepare(&plan.sql.replace("{prefix}", prefix))?;
            let mut rows = stmt.query([])?;
            let mut by_series: HashMap<(String, String), Raw> = HashMap::new();
            while let Some(row) = rows.next()? {
                total += 1;
                if total > MAX_SAMPLES {
                    return Ok(Err(exec_err(format!(
                        "query selects more than {MAX_SAMPLES} samples; narrow the selector or the time range"
                    ))));
                }
                let name: String = row.get(0)?;
                let sid: String = row.get(1)?;
                let t: i64 = row.get(2)?;
                let Some(v) = row.get::<_, Option<f64>>(3)? else {
                    continue;
                };
                let entry = by_series.entry((name.clone(), sid));
                let raw = match entry {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let col = |i: usize| row.get::<_, Option<String>>(4 + i);
                        let mut labels = labels_from_row(
                            col(0)?.as_deref(),
                            col(1)?.as_deref(),
                            col(2)?.as_deref(),
                            col(3)?.as_deref(),
                            col(4)?.as_deref(),
                            col(5)?.as_deref(),
                        );
                        let shown = plan.compiled.display_name.clone().unwrap_or(name);
                        labels.insert(METRIC_NAME.to_string(), shown);
                        e.insert(Raw {
                            labels,
                            samples: Vec::new(),
                        })
                    }
                };
                raw.samples.push((t, v));
            }
            let mut series: Vec<Raw> = by_series
                .into_values()
                .filter(|raw| {
                    plan.compiled.matchers.iter().all(|m| {
                        let value = raw.labels.get(&m.label).map(String::as_str).unwrap_or("");
                        m.ok(value)
                    })
                })
                .collect();
            for raw in &mut series {
                raw.samples.sort_by_key(|s| s.0);
            }
            series.sort_by(|a, b| a.labels.cmp(&b.labels));
            out.insert(plan.key.clone(), series);
        }
        Ok(Ok(out))
    })?;
    fetched
}
