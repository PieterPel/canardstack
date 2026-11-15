//! Conditions on a single span that every matching trace must have, as SQL over the spans table.
//!
//! A search or metrics query only needs the traces that have a span passing its filters, and the
//! database can find those without loading and parsing every span. The conditions here are
//! necessary, never sufficient: they may let through traces the evaluator then rejects, but must
//! not exclude a trace it would accept, so anything not understood is simply left out.

use traceql_syntax::ast::*;

/// Each returned condition must hold for at least one span of a trace for the query to match it.
pub fn requirements(kind: &QueryKind) -> Vec<String> {
    let mut out = Vec::new();
    match kind {
        QueryKind::Pipeline(p) => pipeline(p, &mut out),
        QueryKind::Spansets(s) => spansets(s, &mut out),
        QueryKind::Metrics(m) => pipeline(&m.pipeline, &mut out),
        _ => {}
    }
    out
}

pub fn pipeline_requirements(p: &Pipeline) -> Vec<String> {
    let mut out = Vec::new();
    pipeline(p, &mut out);
    out
}

fn pipeline(p: &Pipeline, out: &mut Vec<String>) {
    for element in &p.elements {
        if let Element::Spanset(s) = element {
            spansets(s, out);
        }
    }
}

fn spansets(s: &Spansets, out: &mut Vec<String>) {
    match s {
        Spansets::Filter(f) => {
            let mut parts = Vec::new();
            conjuncts(f, &mut parts);
            if !parts.is_empty() {
                out.push(parts.join(" AND "));
            }
        }
        Spansets::Pipeline(p) => pipeline(p, out),
        Spansets::Op { op, lhs, rhs } => match op {
            // Both sides must produce spans.
            SpansetOp::And
            | SpansetOp::Child
            | SpansetOp::Parent
            | SpansetOp::Descendant
            | SpansetOp::Ancestor
            | SpansetOp::Sibling
            | SpansetOp::UnionChild
            | SpansetOp::UnionParent
            | SpansetOp::UnionDescendant
            | SpansetOp::UnionAncestor
            | SpansetOp::UnionSibling => {
                spansets(lhs, out);
                spansets(rhs, out);
            }
            // The result is made of right-hand spans only.
            SpansetOp::NotChild
            | SpansetOp::NotParent
            | SpansetOp::NotDescendant
            | SpansetOp::NotAncestor
            | SpansetOp::NotSibling => {
                spansets(rhs, out);
            }
            SpansetOp::Union => {}
        },
    }
}

fn conjuncts(f: &Field, out: &mut Vec<String>) {
    match f {
        Field::Binary {
            op: Op::And,
            lhs,
            rhs,
        } => {
            conjuncts(lhs, out);
            conjuncts(rhs, out);
        }
        other => out.extend(condition(other)),
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn flip(op: Op) -> Op {
    match op {
        Op::Gt => Op::Lt,
        Op::Ge => Op::Le,
        Op::Lt => Op::Gt,
        Op::Le => Op::Ge,
        other => other,
    }
}

fn sql_op(op: Op) -> Option<&'static str> {
    Some(match op {
        Op::Eq => "=",
        Op::Ne => "<>",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Lt => "<",
        Op::Le => "<=",
        _ => return None,
    })
}

/// A JSON object key as a DuckDB JSON path, or `None` when it cannot be quoted safely.
fn json_path(name: &str) -> Option<String> {
    (!name.contains(['"', '\\', '\'']) && !name.is_empty()).then(|| format!("'$.\"{name}\"'"))
}

fn attribute_expressions(a: &Attribute) -> Option<Vec<String>> {
    if a.parent || a.intrinsic.is_some() {
        return None;
    }
    let path = json_path(&a.name)?;
    let span = format!("json_extract_string(span_attributes, {path})");
    let service_column = match a.name.as_str() {
        "service.name" => Some("service_name"),
        "service.namespace" => Some("service_namespace"),
        "service.instance.id" => Some("service_instance_id"),
        _ => None,
    };
    let resource = match service_column {
        Some(column) => format!(
            "coalesce(json_extract_string(resource_attributes, {path}), NULLIF({column}, ''))"
        ),
        None => format!("json_extract_string(resource_attributes, {path})"),
    };
    match a.scope {
        Scope::Span => Some(vec![span]),
        Scope::Resource => Some(vec![resource]),
        Scope::None => Some(vec![span, resource]),
        _ => None,
    }
}

fn condition(f: &Field) -> Option<String> {
    match f {
        Field::Unary {
            op: Op::Exists,
            operand,
        } => {
            let Field::Attribute(a) = operand.as_ref() else {
                return None;
            };
            let exprs = attribute_expressions(a)?;
            Some(format!(
                "({})",
                exprs
                    .iter()
                    .map(|e| format!("{e} IS NOT NULL"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ))
        }
        Field::Binary { op, lhs, rhs } => {
            let (op, attr, value) = match (lhs.as_ref(), rhs.as_ref()) {
                (Field::Attribute(a), Field::Static(v)) => (*op, a, v),
                (Field::Static(v), Field::Attribute(a)) => (flip(*op), a, v),
                _ => return None,
            };
            compare(op, attr, value)
        }
        _ => None,
    }
}

fn compare(op: Op, a: &Attribute, value: &Static) -> Option<String> {
    let symbol = sql_op(op)?;
    if let Some(intrinsic) = a.intrinsic {
        if a.parent {
            return None;
        }
        return match (intrinsic, value) {
            (Intrinsic::Name, Static::Str(s)) => Some(format!("name {symbol} {}", quote(s))),
            (Intrinsic::Duration, Static::Duration(n)) => {
                Some(format!("duration_time_unix_nano {symbol} {n}"))
            }
            (Intrinsic::Status, Static::Status(s)) if matches!(op, Op::Eq | Op::Ne) => {
                let code = match s {
                    Status::Unset => 0,
                    Status::Ok => 1,
                    Status::Error => 2,
                };
                Some(format!("coalesce(status_code, 0) {symbol} {code}"))
            }
            (Intrinsic::Kind, Static::Kind(k)) if matches!(op, Op::Eq | Op::Ne) => {
                let code = match k {
                    Kind::Unspecified => 0,
                    Kind::Internal => 1,
                    Kind::Server => 2,
                    Kind::Client => 3,
                    Kind::Producer => 4,
                    Kind::Consumer => 5,
                };
                Some(format!("coalesce(kind, 0) {symbol} {code}"))
            }
            _ => None,
        };
    }
    let exprs = attribute_expressions(a)?;
    let parts: Vec<String> = match value {
        Static::Str(s) if matches!(op, Op::Eq | Op::Ne) => exprs
            .iter()
            .map(|e| format!("{e} {symbol} {}", quote(s)))
            .collect(),
        Static::Bool(b) if matches!(op, Op::Eq | Op::Ne) => exprs
            .iter()
            .map(|e| format!("{e} {symbol} '{b}'"))
            .collect(),
        Static::Int(n) => exprs
            .iter()
            .map(|e| format!("TRY_CAST({e} AS DOUBLE) {symbol} {n}"))
            .collect(),
        Static::Float(n) if n.is_finite() => exprs
            .iter()
            .map(|e| format!("TRY_CAST({e} AS DOUBLE) {symbol} {n:?}"))
            .collect(),
        _ => return None,
    };
    Some(format!("({})", parts.join(" OR ")))
}

/// Which of the wide columns a query can read. A query that only filters on names and durations
/// has no use for the attribute JSON of every span, and reading it is most of the cost of loading.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Needs {
    pub resource: bool,
    pub span_attrs: bool,
    pub scope_attrs: bool,
    pub scope_names: bool,
    pub events: bool,
    pub links: bool,
    pub status_message: bool,
}

impl Needs {
    fn all() -> Needs {
        Needs {
            resource: true,
            span_attrs: true,
            scope_attrs: true,
            scope_names: true,
            events: true,
            links: true,
            status_message: true,
        }
    }

    fn attribute(&mut self, a: &Attribute) {
        match a.intrinsic {
            Some(Intrinsic::StatusMessage) => self.status_message = true,
            Some(Intrinsic::EventName | Intrinsic::EventTimeSinceStart) => self.events = true,
            Some(Intrinsic::LinkTraceId | Intrinsic::LinkSpanId) => self.links = true,
            Some(Intrinsic::InstrumentationName | Intrinsic::InstrumentationVersion) => {
                self.scope_names = true
            }
            Some(_) => {}
            None => match a.scope {
                Scope::Span => self.span_attrs = true,
                Scope::Resource => self.resource = true,
                Scope::None => {
                    self.span_attrs = true;
                    self.resource = true;
                }
                Scope::Event => self.events = true,
                Scope::Link => self.links = true,
                Scope::Instrumentation => self.scope_attrs = true,
            },
        }
    }

    fn field(&mut self, f: &Field) {
        match f {
            Field::Static(_) => {}
            Field::Attribute(a) => self.attribute(a),
            Field::Binary { lhs, rhs, .. } => {
                self.field(lhs);
                self.field(rhs);
            }
            Field::Unary { operand, .. } => self.field(operand),
        }
    }

    fn scalar(&mut self, s: &Scalar) {
        match s {
            Scalar::Aggregate(a) => a.expr.iter().for_each(|f| self.field(f)),
            Scalar::Static(_) => {}
            Scalar::Binary { lhs, rhs, .. } => {
                self.scalar(lhs);
                self.scalar(rhs);
            }
            Scalar::Pipeline(p) => self.pipeline(p),
        }
    }

    fn spansets(&mut self, s: &Spansets) {
        match s {
            Spansets::Filter(f) => self.field(f),
            Spansets::Op { lhs, rhs, .. } => {
                self.spansets(lhs);
                self.spansets(rhs);
            }
            Spansets::Pipeline(p) => self.pipeline(p),
        }
    }

    fn pipeline(&mut self, p: &Pipeline) {
        for element in &p.elements {
            match element {
                Element::Spanset(s) => self.spansets(s),
                Element::ScalarFilter(f) => {
                    self.scalar(&f.lhs);
                    self.scalar(&f.rhs);
                }
                Element::Group(f) => self.field(f),
                Element::Aggregate(a) => a.expr.iter().for_each(|f| self.field(f)),
                Element::Select(attrs) => attrs.iter().for_each(|a| self.attribute(a)),
                Element::Coalesce => {}
            }
        }
    }

    /// What a metrics function reads besides its pipeline.
    fn metrics(&mut self, m: &MetricsPipeline) {
        self.pipeline(&m.pipeline);
        for a in &m.agg.by {
            self.attribute(a);
        }
        match &m.agg.func {
            MetricsFn::MinOverTime(a)
            | MetricsFn::MaxOverTime(a)
            | MetricsFn::AvgOverTime(a)
            | MetricsFn::SumOverTime(a)
            | MetricsFn::HistogramOverTime(a)
            | MetricsFn::QuantileOverTime(a, _) => self.attribute(a),
            // compare() reports every attribute of every span.
            MetricsFn::Compare { .. } => *self = Needs::all(),
            MetricsFn::Rate | MetricsFn::CountOverTime => {}
        }
    }

    fn expression(&mut self, e: &MetricsExpr) {
        match e {
            MetricsExpr::Pipeline(m) => self.metrics(m),
            MetricsExpr::Binary { lhs, rhs, .. } => {
                self.expression(lhs);
                self.expression(rhs);
            }
            MetricsExpr::Scalar { expr, .. } => self.expression(expr),
        }
    }
}

/// The wide columns that evaluating this query can read.
pub fn needs(kind: &QueryKind) -> Needs {
    let mut n = Needs::default();
    match kind {
        QueryKind::Pipeline(p) => n.pipeline(p),
        QueryKind::Spansets(s) => n.spansets(s),
        QueryKind::ScalarFilter(f) => {
            n.scalar(&f.lhs);
            n.scalar(&f.rhs);
        }
        QueryKind::Metrics(m) => n.metrics(m),
        QueryKind::MetricsMath { expr, .. } => n.expression(expr),
    }
    n
}

/// What a single metrics pipeline reads.
pub fn metrics_needs(m: &MetricsPipeline) -> Needs {
    let mut n = Needs::default();
    n.metrics(m);
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reqs(q: &str) -> Vec<String> {
        requirements(&traceql_syntax::parse(q).unwrap().kind)
    }

    #[test]
    fn simple_filters_become_conditions_on_one_span() {
        assert_eq!(
            reqs(r#"{ name = "a" && status = error }"#),
            ["name = 'a' AND coalesce(status_code, 0) = 2"]
        );
        assert_eq!(
            reqs("{ duration > 1s }"),
            ["duration_time_unix_nano > 1000000000"]
        );
        assert_eq!(
            reqs("{ 1s < duration }"),
            ["duration_time_unix_nano > 1000000000"]
        );
        assert_eq!(
            reqs(r#"{ span.http.method = "GET" }"#),
            [r#"(json_extract_string(span_attributes, '$."http.method"') = 'GET')"#]
        );
    }

    #[test]
    fn unscoped_attributes_look_at_span_and_resource() {
        let r = reqs(r#"{ .service.name = "x" }"#);
        assert_eq!(r.len(), 1);
        assert!(
            r[0].contains("span_attributes")
                && r[0].contains("resource_attributes")
                && r[0].contains("NULLIF(service_name"),
            "{r:?}"
        );
    }

    #[test]
    fn what_is_not_understood_is_left_out_not_guessed() {
        assert!(reqs(r#"{ name = "a" || name = "b" }"#).is_empty());
        assert!(reqs(r#"{ name =~ "a.*" }"#).is_empty());
        assert!(reqs("{ !(status = error) }").is_empty());
        assert_eq!(reqs(r#"{ name = "a" && name =~ "x.*" }"#), ["name = 'a'"]);
        assert!(reqs("{ parent.name = \"a\" }").is_empty());
        assert!(reqs(r#"{ name = "a" } || { name = "b" }"#).is_empty());
    }

    #[test]
    fn spanset_operators_require_what_must_be_present() {
        assert_eq!(
            reqs(r#"{ name = "a" } >> { name = "b" }"#),
            ["name = 'a'", "name = 'b'"]
        );
        assert_eq!(reqs(r#"{ name = "a" } !>> { name = "b" }"#), ["name = 'b'"]);
        assert_eq!(reqs(r#"{ name = "a" } | count() > 2"#), ["name = 'a'"]);
        assert_eq!(reqs(r#"{ name = "a" } | rate() by (name)"#), ["name = 'a'"]);
    }

    #[test]
    fn only_the_columns_a_query_reads_are_needed() {
        let n = |q: &str| needs(&traceql_syntax::parse(q).unwrap().kind);
        assert_eq!(n(r#"{ name = "a" && duration > 1s }"#), Needs::default());
        assert_eq!(
            n("{ .a = 1 }"),
            Needs {
                span_attrs: true,
                resource: true,
                ..Needs::default()
            }
        );
        assert_eq!(
            n("{ span.a = 1 }"),
            Needs {
                span_attrs: true,
                ..Needs::default()
            }
        );
        assert_eq!(
            n(r#"{ resource.service.name = "x" }"#),
            Needs {
                resource: true,
                ..Needs::default()
            }
        );
        assert!(n(r#"{ statusMessage = "x" }"#).status_message);
        assert!(n(r#"{ event.exception.type = "x" }"#).events);
        assert!(n(r#"{ link:spanID = "x" }"#).links);
        assert!(n(r#"{ instrumentation:name = "x" }"#).scope_names);
        assert!(n("{ } | by(span.a)").span_attrs);
        assert!(n("{ } | max(.a) > 1").resource);
        assert_eq!(n("{ } | rate()"), Needs::default());
        assert!(n("{ } | rate() by (resource.service.name)").resource);
        assert!(n("{ } | quantile_over_time(span.x, 0.5)").span_attrs);
        assert_eq!(n("{ } | compare({ status = error })"), Needs::all());
    }

    #[test]
    fn quotes_in_values_are_escaped() {
        assert_eq!(reqs(r#"{ name = "it's" }"#), ["name = 'it''s'"]);
        assert!(reqs(r#"{ span."we'ird" = "x" }"#).is_empty());
    }
}
