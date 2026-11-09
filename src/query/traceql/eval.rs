//! Evaluation of TraceQL over one trace: expressions per span, then the pipeline over spansets.
//!
//! An expression can produce several values for one span (an unscoped `.a` looks at the span and
//! at the resource, `event.name` at every event); a comparison holds when any combination does.

use super::model::{Span, Trace};
use super::value::{arith, compare, from_static, kind_of, negate, status_of, Regexes, Val};
use crate::validation::{ApiError, ApiResult};
use std::collections::BTreeMap;
use traceql_syntax::ast::*;

/// The spans (indexes into the trace, ascending) of one spanset.
pub type Group = Vec<usize>;

pub struct Eval<'a> {
    pub trace: &'a Trace,
    pub regexes: &'a Regexes,
}

fn unsupported(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "bad_data", msg)
}

impl Eval<'_> {
    fn span(&self, i: usize) -> &Span {
        &self.trace.spans[i]
    }

    fn intrinsic(&self, intrinsic: Intrinsic, i: usize) -> Vec<Val> {
        let s = self.span(i);
        let one = |v: Val| vec![v];
        match intrinsic {
            Intrinsic::Duration => one(Val::Dur(s.duration_ns)),
            Intrinsic::Name => one(Val::Str(s.name.clone())),
            Intrinsic::Status => one(Val::Status(status_of(s.status))),
            Intrinsic::StatusMessage => one(Val::Str(s.status_message.clone())),
            Intrinsic::Kind => one(Val::Kind(kind_of(s.kind))),
            Intrinsic::Parent => s.parent.map(|_| Val::Bool(true)).into_iter().collect(),
            Intrinsic::TraceRootSpan => one(Val::Str(self.trace.root_name.clone())),
            Intrinsic::TraceRootService => one(Val::Str(self.trace.root_service.clone())),
            Intrinsic::TraceDuration => one(Val::Dur(self.trace.end_ns - self.trace.start_ns)),
            Intrinsic::TraceId => one(Val::Str(self.trace.id.clone())),
            Intrinsic::SpanId => one(Val::Str(s.span_id.clone())),
            Intrinsic::ParentId if s.parent_id.is_empty() => Vec::new(),
            Intrinsic::ParentId => one(Val::Str(s.parent_id.clone())),
            Intrinsic::ChildCount => one(Val::Int(s.children as i64)),
            Intrinsic::NestedSetLeft => one(Val::Int(s.left)),
            Intrinsic::NestedSetRight => one(Val::Int(s.right)),
            Intrinsic::NestedSetParent => one(Val::Int(s.parent.map_or(-1, |p| self.span(p).left))),
            Intrinsic::EventName => s
                .events()
                .iter()
                .map(|e| Val::Str(e.name.clone()))
                .collect(),
            Intrinsic::EventTimeSinceStart => s
                .events()
                .iter()
                .map(|e| Val::Dur(e.time_ns - s.start_ns))
                .collect(),
            Intrinsic::LinkTraceId => s
                .links()
                .iter()
                .map(|l| Val::Str(l.trace_id.clone()))
                .collect(),
            Intrinsic::LinkSpanId => s
                .links()
                .iter()
                .map(|l| Val::Str(l.span_id.clone()))
                .collect(),
            Intrinsic::InstrumentationName => one(Val::Str(s.scope_name.clone())),
            Intrinsic::InstrumentationVersion => one(Val::Str(s.scope_version.clone())),
        }
    }

    fn attribute(&self, attr: &Attribute, i: usize) -> Vec<Val> {
        let i = if attr.parent {
            match self.span(i).parent {
                Some(p) => p,
                None => return Vec::new(),
            }
        } else {
            i
        };
        if let Some(intrinsic) = attr.intrinsic {
            return self.intrinsic(intrinsic, i);
        }
        let s = self.span(i);
        let name = &attr.name;
        match attr.scope {
            Scope::Span => s.span_attrs().get(name).cloned().into_iter().collect(),
            Scope::Resource => s.resource().get(name).cloned().into_iter().collect(),
            Scope::Instrumentation => s.scope_attrs().get(name).cloned().into_iter().collect(),
            Scope::Event => s
                .events()
                .iter()
                .filter_map(|e| e.attrs.get(name).cloned())
                .collect(),
            Scope::Link => s
                .links()
                .iter()
                .filter_map(|l| l.attrs.get(name).cloned())
                .collect(),
            Scope::None => s
                .span_attrs()
                .get(name)
                .into_iter()
                .chain(s.resource().get(name))
                .cloned()
                .collect(),
        }
    }

    pub fn field(&self, field: &Field, i: usize) -> Vec<Val> {
        match field {
            Field::Static(s) => from_static(s).into_iter().collect(),
            Field::Attribute(a) => self.attribute(a, i),
            Field::Unary { op, operand } => {
                let values = self.field(operand, i);
                match op {
                    // Like Tempo, a missing operand makes a boolean operator unknown, not false.
                    Op::Not if values.is_empty() => Vec::new(),
                    Op::Not => vec![Val::Bool(!values.iter().any(Val::is_true))],
                    Op::Exists => vec![Val::Bool(!values.is_empty())],
                    Op::NotExists => vec![Val::Bool(values.is_empty())],
                    Op::Sub => values.iter().filter_map(negate).collect(),
                    _ => Vec::new(),
                }
            }
            Field::Binary { op, lhs, rhs } => {
                let l = self.field(lhs, i);
                match op {
                    Op::And | Op::Or => {
                        let r = self.field(rhs, i);
                        if l.is_empty() || r.is_empty() {
                            return Vec::new();
                        }
                        let (l, r) = (l.iter().any(Val::is_true), r.iter().any(Val::is_true));
                        vec![Val::Bool(if *op == Op::And { l && r } else { l || r })]
                    }
                    _ => {
                        let r = self.field(rhs, i);
                        if op.is_boolean() {
                            let hit = l
                                .iter()
                                .any(|a| r.iter().any(|b| compare(*op, a, b, self.regexes)));
                            vec![Val::Bool(hit)]
                        } else {
                            l.iter()
                                .flat_map(|a| r.iter().filter_map(|b| arith(*op, a, b)))
                                .collect()
                        }
                    }
                }
            }
        }
    }

    fn holds(&self, field: &Field, i: usize) -> bool {
        self.field(field, i).iter().any(Val::is_true)
    }

    fn all(&self) -> Group {
        (0..self.trace.spans.len()).collect()
    }

    /// The whole trace as the input of a query.
    pub fn input(&self) -> Vec<Group> {
        vec![self.all()]
    }

    pub fn pipeline(&self, pipeline: &Pipeline, input: Vec<Group>) -> ApiResult<Vec<Group>> {
        pipeline
            .elements
            .iter()
            .try_fold(input, |groups, element| self.element(element, groups))
    }

    fn element(&self, element: &Element, input: Vec<Group>) -> ApiResult<Vec<Group>> {
        match element {
            Element::Spanset(spansets) => {
                let mut out = Vec::new();
                for group in &input {
                    out.extend(self.spansets(spansets, group)?);
                }
                Ok(out)
            }
            Element::ScalarFilter(filter) => Ok(input
                .into_iter()
                .filter(|g| self.scalar_filter(filter, g))
                .collect()),
            Element::Group(field) => {
                Ok(input.iter().flat_map(|g| self.group_by(field, g)).collect())
            }
            Element::Coalesce => {
                let mut merged: Group = input.into_iter().flatten().collect();
                merged.sort_unstable();
                merged.dedup();
                Ok(if merged.is_empty() {
                    Vec::new()
                } else {
                    vec![merged]
                })
            }
            Element::Select(_) => Ok(input),
            Element::Aggregate(_) => Err(unsupported("an aggregate must be compared to a value")),
        }
    }

    fn group_by(&self, field: &Field, group: &Group) -> Vec<Group> {
        // Spans without a value for the expression stay together in one group.
        let mut groups: BTreeMap<String, Group> = BTreeMap::new();
        for &i in group {
            let key = self
                .field(field, i)
                .first()
                .map(|v| format!("{v:?}"))
                .unwrap_or_default();
            groups.entry(key).or_default().push(i);
        }
        groups.into_values().collect()
    }

    fn spansets(&self, spansets: &Spansets, input: &Group) -> ApiResult<Vec<Group>> {
        match spansets {
            Spansets::Filter(field) => {
                let kept: Group = input
                    .iter()
                    .copied()
                    .filter(|&i| self.holds(field, i))
                    .collect();
                Ok(if kept.is_empty() {
                    Vec::new()
                } else {
                    vec![kept]
                })
            }
            Spansets::Pipeline(p) => self.pipeline(p, vec![input.clone()]),
            Spansets::Op { op, lhs, rhs } => {
                let l = self.spansets(lhs, input)?;
                let r = self.spansets(rhs, input)?;
                self.spanset_op(*op, l, r)
            }
        }
    }

    fn spanset_op(&self, op: SpansetOp, lhs: Vec<Group>, rhs: Vec<Group>) -> ApiResult<Vec<Group>> {
        match op {
            SpansetOp::And | SpansetOp::Union => {
                let both = if op == SpansetOp::And {
                    !lhs.is_empty() && !rhs.is_empty()
                } else {
                    !lhs.is_empty() || !rhs.is_empty()
                };
                if !both {
                    return Ok(Vec::new());
                }
                let mut merged: Group = lhs.into_iter().chain(rhs).flatten().collect();
                merged.sort_unstable();
                merged.dedup();
                Ok(vec![merged])
            }
            _ => {
                if lhs.len() > 1 || rhs.len() > 1 {
                    return Err(unsupported(
                        "structural operators are not supported on multiple spansets",
                    ));
                }
                let l = lhs.into_iter().next().unwrap_or_default();
                let r = rhs.into_iter().next().unwrap_or_default();
                let out = self.structural(op, &l, &r);
                Ok(if out.is_empty() {
                    Vec::new()
                } else {
                    vec![out]
                })
            }
        }
    }

    /// Whether span `r` stands in the relation of `op` to span `l` (`l` is the left operand).
    fn related(&self, op: SpansetOp, l: usize, r: usize) -> bool {
        use SpansetOp::*;
        let (ls, rs) = (self.span(l), self.span(r));
        let descendant = |a: &Span, b: &Span| a.left > b.left && a.right < b.right;
        match op {
            Child | NotChild | UnionChild => rs.parent == Some(l),
            Parent | NotParent | UnionParent => ls.parent == Some(r),
            Descendant | NotDescendant | UnionDescendant => descendant(rs, ls),
            Ancestor | NotAncestor | UnionAncestor => descendant(ls, rs),
            Sibling | NotSibling | UnionSibling => l != r && ls.parent == rs.parent,
            And | Union => false,
        }
    }

    fn structural(&self, op: SpansetOp, lhs: &Group, rhs: &Group) -> Group {
        use SpansetOp::*;
        let negated = matches!(
            op,
            NotChild | NotParent | NotDescendant | NotAncestor | NotSibling
        );
        let union = matches!(
            op,
            UnionChild | UnionParent | UnionDescendant | UnionAncestor | UnionSibling
        );
        let mut out: Group = Vec::new();
        for &r in rhs {
            let mut matched = false;
            for &l in lhs {
                if self.related(op, l, r) {
                    matched = true;
                    if union {
                        out.push(l);
                    } else {
                        break;
                    }
                }
            }
            if matched != negated {
                out.push(r);
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    fn scalar_filter(&self, filter: &ScalarFilter, group: &Group) -> bool {
        match (
            self.scalar(&filter.lhs, group),
            self.scalar(&filter.rhs, group),
        ) {
            (Some(l), Some(r)) => compare(filter.op, &l, &r, self.regexes),
            _ => false,
        }
    }

    fn scalar(&self, scalar: &Scalar, group: &Group) -> Option<Val> {
        match scalar {
            Scalar::Static(s) => from_static(s),
            Scalar::Aggregate(agg) => self.aggregate(agg, group),
            Scalar::Binary { op, lhs, rhs } => {
                arith(*op, &self.scalar(lhs, group)?, &self.scalar(rhs, group)?)
            }
            Scalar::Pipeline(p) => {
                let (last, body) = p.elements.split_last()?;
                let Element::Aggregate(agg) = last else {
                    return None;
                };
                let groups = self
                    .pipeline(
                        &Pipeline {
                            elements: body.to_vec(),
                        },
                        vec![group.clone()],
                    )
                    .ok()?;
                match groups.as_slice() {
                    [only] => self.aggregate(agg, only),
                    _ => None,
                }
            }
        }
    }

    pub fn aggregate(&self, agg: &Aggregate, group: &Group) -> Option<Val> {
        let Some(field) = &agg.expr else {
            return Some(Val::Int(group.len() as i64));
        };
        let values: Vec<Val> = group
            .iter()
            .filter_map(|&i| {
                self.field(field, i)
                    .into_iter()
                    .find(|v| matches!(v, Val::Int(_) | Val::Float(_) | Val::Dur(_)))
            })
            .collect();
        let first = values.first()?.clone();
        let float = |v: &Val| match v {
            Val::Int(x) | Val::Dur(x) => *x as f64,
            Val::Float(x) => *x,
            _ => 0.0,
        };
        let wrap = |x: f64| match first {
            Val::Dur(_) => Val::Dur(x.round() as i64),
            Val::Int(_) if x.fract() == 0.0 => Val::Int(x as i64),
            _ => Val::Float(x),
        };
        let numbers: Vec<f64> = values.iter().map(float).collect();
        Some(match agg.op {
            AggregateOp::Count => Val::Int(values.len() as i64),
            AggregateOp::Max => wrap(numbers.iter().cloned().fold(f64::MIN, f64::max)),
            AggregateOp::Min => wrap(numbers.iter().cloned().fold(f64::MAX, f64::min)),
            AggregateOp::Sum => wrap(numbers.iter().sum()),
            AggregateOp::Avg => {
                let avg = numbers.iter().sum::<f64>() / numbers.len() as f64;
                match first {
                    Val::Dur(_) => Val::Dur(avg.round() as i64),
                    _ => Val::Float(avg),
                }
            }
        })
    }

    /// Evaluates a top-level `({ ... } | count()) > 3` over the whole trace; the matching spans are
    /// those of the first pipeline operand.
    pub fn scalar_query(&self, filter: &ScalarFilter) -> ApiResult<Vec<Group>> {
        let all = self.all();
        if !self.scalar_filter(filter, &all) {
            return Ok(Vec::new());
        }
        let operand = [&filter.lhs, &filter.rhs]
            .into_iter()
            .find_map(|s| match s {
                Scalar::Pipeline(p) => Some(p),
                _ => None,
            });
        match operand {
            Some(p) => {
                let body = Pipeline {
                    elements: p.elements[..p.elements.len().saturating_sub(1)].to_vec(),
                };
                self.pipeline(&body, self.input())
            }
            None => Ok(vec![all]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::traceql::model::{Attrs, Lazy, Span};

    fn span(
        id: &str,
        parent: &str,
        name: &str,
        start: i64,
        duration: i64,
        attrs: &[(&str, Val)],
    ) -> Span {
        Span {
            span_id: id.into(),
            parent_id: parent.into(),
            name: name.into(),
            kind: 2,
            status: 0,
            status_message: String::new(),
            start_ns: start,
            duration_ns: duration,
            service: [None, None, None],
            resource: Lazy::ready(Attrs::from([(
                "service.name".to_string(),
                Val::Str("svc".into()),
            )])),
            attributes: Lazy::ready(
                attrs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            ),
            scope_name: String::new(),
            scope_version: String::new(),
            scope: Lazy::ready(Attrs::new()),
            events: Lazy::ready(Vec::new()),
            links: Lazy::ready(Vec::new()),
            parent: None,
            children: 0,
            left: 0,
            right: 0,
        }
    }

    /// root -> a -> c, root -> b
    fn trace() -> Trace {
        Trace::build(
            "t".into(),
            vec![
                span("r", "", "root", 0, 100, &[("n", Val::Int(1))]),
                span("a", "r", "a", 10, 50, &[("n", Val::Int(2))]),
                span("b", "r", "b", 20, 10, &[]),
                span("c", "a", "c", 30, 5, &[("n", Val::Int(3))]),
            ],
        )
    }

    fn run(query: &str) -> Vec<Vec<String>> {
        let t = trace();
        let regexes = Regexes::default();
        let eval = Eval {
            trace: &t,
            regexes: &regexes,
        };
        let ast = crate::query::traceql::parse(query).unwrap();
        let traceql_syntax::ast::QueryKind::Pipeline(p) = ast.kind else {
            panic!("not a pipeline")
        };
        eval.pipeline(&p, eval.input())
            .unwrap()
            .into_iter()
            .map(|g| g.into_iter().map(|i| t.spans[i].span_id.clone()).collect())
            .collect()
    }

    #[test]
    fn filters_on_attributes_and_intrinsics() {
        assert_eq!(run("{ .n > 1 }"), vec![vec!["a", "c"]]);
        assert_eq!(run(r#"{ name =~ "a|b" }"#), vec![vec!["a", "b"]]);
        assert_eq!(run("{ duration >= 50ns && .n = 2 }"), vec![vec!["a"]]);
        assert_eq!(
            run("{ resource.service.name = \"other\" }"),
            Vec::<Vec<String>>::new()
        );
    }

    #[test]
    fn missing_values_do_not_satisfy_boolean_operators() {
        assert_eq!(run("{ .nothing || true }"), Vec::<Vec<String>>::new());
        assert_eq!(run("{ !.nothing }"), Vec::<Vec<String>>::new());
        assert_eq!(run("{ .n = nil }"), vec![vec!["b"]]);
        assert_eq!(run("{ .n != nil }"), vec![vec!["r", "a", "c"]]);
    }

    #[test]
    fn structural_operators() {
        assert_eq!(
            run(r#"{ name = "nothing" } > { }"#),
            Vec::<Vec<String>>::new()
        );
        assert_eq!(run(r#"{ name = "root" } > { }"#), vec![vec!["a", "b"]]);
        assert_eq!(
            run(r#"{ name = "root" } >> { }"#),
            vec![vec!["a", "b", "c"]]
        );
        assert_eq!(run(r#"{ name = "c" } << { }"#), vec![vec!["r", "a"]]);
        assert_eq!(run(r#"{ name = "a" } ~ { }"#), vec![vec!["b"]]);
        assert_eq!(run(r#"{ name = "root" } !> { }"#), vec![vec!["r", "c"]]);
        assert_eq!(
            run(r#"{ name = "root" } &>> { name = "c" }"#),
            vec![vec!["r", "c"]]
        );
    }

    #[test]
    fn pipeline_elements() {
        assert_eq!(run("{ } | count() > 3"), vec![vec!["r", "a", "b", "c"]]);
        assert_eq!(run("{ } | count() > 4"), Vec::<Vec<String>>::new());
        assert_eq!(run("{ } | max(.n) = 3 | { .n < 3 }"), vec![vec!["r", "a"]]);
        assert_eq!(run("{ } | by(.n)").len(), 4);
        assert_eq!(
            run("{ } | by(.n) | coalesce()"),
            vec![vec!["r", "a", "b", "c"]]
        );
        assert_eq!(run("{ } | avg(.n) > 1.5"), vec![vec!["r", "a", "b", "c"]]);
    }
}
