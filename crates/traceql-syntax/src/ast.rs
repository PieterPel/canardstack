//! TraceQL syntax tree.

/// A parsed query: what it evaluates, plus its `with(...)` hints.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub kind: QueryKind,
    pub hints: Vec<Hint>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryKind {
    /// `{ ... } | ...`: a pipeline selecting spans.
    Pipeline(Pipeline),
    /// Spanset operators over pipelines: `({a} | count() > 1) >> ({b})`.
    Spansets(Spansets),
    /// `({ ... } | count()) > 3`: scalars computed from whole pipelines.
    ScalarFilter(ScalarFilter),
    /// `{ ... } | rate() by (...) | topk(5)`
    Metrics(MetricsPipeline),
    /// Arithmetic over metrics queries: `({} | rate()) / ({} | count_over_time())`.
    MetricsMath {
        expr: MetricsExpr,
        second: Vec<SecondStage>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Hint {
    pub name: String,
    pub value: Static,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pipeline {
    pub elements: Vec<Element>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Element {
    Spanset(Spansets),
    ScalarFilter(ScalarFilter),
    /// `by(expr)`
    Group(Field),
    /// `coalesce()`
    Coalesce,
    /// `select(a, b)`
    Select(Vec<Attribute>),
    /// The trailing `| count()` of a pipeline used as a scalar.
    Aggregate(Aggregate),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Spansets {
    /// `{ expr }`; `{}` is `{ true }`.
    Filter(Field),
    Op {
        op: SpansetOp,
        lhs: Box<Spansets>,
        rhs: Box<Spansets>,
    },
    /// A parenthesised pipeline used as an operand.
    Pipeline(Box<Pipeline>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpansetOp {
    And,
    Union,
    Child,
    Parent,
    Descendant,
    Ancestor,
    Sibling,
    NotChild,
    NotParent,
    NotDescendant,
    NotAncestor,
    NotSibling,
    UnionChild,
    UnionParent,
    UnionDescendant,
    UnionAncestor,
    UnionSibling,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Op {
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Re,
    NotRe,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Not,
    /// `x != nil`
    Exists,
    /// `x = nil`
    NotExists,
}

impl Op {
    pub fn is_boolean(self) -> bool {
        matches!(
            self,
            Op::And
                | Op::Or
                | Op::Eq
                | Op::Ne
                | Op::Gt
                | Op::Ge
                | Op::Lt
                | Op::Le
                | Op::Re
                | Op::NotRe
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Static(Static),
    Attribute(Attribute),
    Binary {
        op: Op,
        lhs: Box<Field>,
        rhs: Box<Field>,
    },
    Unary {
        op: Op,
        operand: Box<Field>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Static {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Nanoseconds.
    Duration(i64),
    Status(Status),
    Kind(Kind),
    Nil,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Ok,
    Error,
    Unset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    None,
    Resource,
    Span,
    Event,
    Link,
    Instrumentation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Intrinsic {
    Duration,
    Name,
    Status,
    StatusMessage,
    Kind,
    Parent,
    TraceRootSpan,
    TraceRootService,
    TraceDuration,
    TraceId,
    SpanId,
    ParentId,
    ChildCount,
    NestedSetLeft,
    NestedSetRight,
    NestedSetParent,
    EventName,
    EventTimeSinceStart,
    LinkTraceId,
    LinkSpanId,
    InstrumentationName,
    InstrumentationVersion,
}

/// An intrinsic (`name`, `span:duration`) or an attribute (`.a`, `span.a`, `parent.resource.a`).
#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub scope: Scope,
    /// `parent.` prefix.
    pub parent: bool,
    pub intrinsic: Option<Intrinsic>,
    pub name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateOp {
    Count,
    Max,
    Min,
    Avg,
    Sum,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub op: AggregateOp,
    /// None for `count()`.
    pub expr: Option<Field>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Aggregate(Aggregate),
    Static(Static),
    Binary {
        op: Op,
        lhs: Box<Scalar>,
        rhs: Box<Scalar>,
    },
    /// `({ ... } | count())`
    Pipeline(Box<Pipeline>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScalarFilter {
    pub op: Op,
    pub lhs: Scalar,
    pub rhs: Scalar,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetricsFn {
    Rate,
    CountOverTime,
    MinOverTime(Attribute),
    MaxOverTime(Attribute),
    AvgOverTime(Attribute),
    SumOverTime(Attribute),
    QuantileOverTime(Attribute, Vec<f64>),
    HistogramOverTime(Attribute),
    Compare {
        filter: Field,
        top_n: i64,
        start: i64,
        end: i64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsAgg {
    pub func: MetricsFn,
    pub by: Vec<Attribute>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SecondStage {
    TopK(i64),
    BottomK(i64),
    /// `> 10`
    Filter {
        op: Op,
        value: Static,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsPipeline {
    pub pipeline: Pipeline,
    pub agg: MetricsAgg,
    pub second: Vec<SecondStage>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetricsExpr {
    Pipeline(Box<MetricsPipeline>),
    Binary {
        op: Op,
        lhs: Box<MetricsExpr>,
        rhs: Box<MetricsExpr>,
    },
    /// `100 * (...)` (scalar on the left) or `(...) * 100`.
    Scalar {
        op: Op,
        value: f64,
        expr: Box<MetricsExpr>,
        scalar_on_left: bool,
    },
}
