//! LogQL syntax tree.

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Log(LogExpr),
    Range(Box<RangeAgg>),
    Vector(Box<VecAgg>),
    Binary(Box<Binary>),
    LabelReplace(Box<LabelReplace>),
    Number(f64),
    /// `vector(n)`
    VectorLit(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogExpr {
    pub matchers: Vec<Matcher>,
    pub stages: Vec<Stage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Eq,
    Ne,
    Re,
    NotRe,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Matcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineOp {
    Contains,
    NotContains,
    Re,
    NotRe,
    Pattern,
    NotPattern,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterValue {
    Str(String),
    /// `ip("...")`: an address, CIDR or range.
    Ip(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Stage {
    /// Alternatives joined with `or` match if any one does.
    LineFilter {
        op: LineOp,
        values: Vec<FilterValue>,
    },
    Json(Vec<Extraction>),
    Logfmt(Vec<Extraction>),
    Regexp(String),
    Pattern(String),
    Unpack,
    LineFormat(String),
    LabelFormat(Vec<LabelFormatItem>),
    Decolorize,
    Drop(Vec<DropItem>),
    Keep(Vec<DropItem>),
    LabelFilter(LabelFilter),
}

/// `label` or `label="expression"` in `| json` and `| logfmt`.
#[derive(Clone, Debug, PartialEq)]
pub struct Extraction {
    pub label: String,
    pub expr: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LabelFormatItem {
    /// `dst=src`: rename.
    Rename { dst: String, src: String },
    /// `dst="{{ template }}"`
    Template { dst: String, template: String },
}

/// `label` or `label="value"` (with a matcher operator) in `drop` / `keep`.
#[derive(Clone, Debug, PartialEq)]
pub struct DropItem {
    pub label: String,
    pub matcher: Option<(MatchOp, String)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Re,
    NotRe,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LabelValue {
    Str(String),
    Number(f64),
    /// Seconds.
    Duration(f64),
    Bytes(f64),
    Ip(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum LabelFilter {
    Cmp {
        name: String,
        op: CmpOp,
        value: LabelValue,
    },
    And(Box<LabelFilter>, Box<LabelFilter>),
    Or(Box<LabelFilter>, Box<LabelFilter>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Conv {
    None,
    Bytes,
    Duration,
    DurationSeconds,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Unwrap {
    pub conv: Conv,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Grouping {
    pub by: bool,
    pub labels: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeOp {
    Rate,
    RateCounter,
    CountOverTime,
    BytesRate,
    BytesOverTime,
    AbsentOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    StddevOverTime,
    StdvarOverTime,
    QuantileOverTime,
    FirstOverTime,
    LastOverTime,
}

impl RangeOp {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "rate" => Self::Rate,
            "rate_counter" => Self::RateCounter,
            "count_over_time" => Self::CountOverTime,
            "bytes_rate" => Self::BytesRate,
            "bytes_over_time" => Self::BytesOverTime,
            "absent_over_time" => Self::AbsentOverTime,
            "sum_over_time" => Self::SumOverTime,
            "avg_over_time" => Self::AvgOverTime,
            "min_over_time" => Self::MinOverTime,
            "max_over_time" => Self::MaxOverTime,
            "stddev_over_time" => Self::StddevOverTime,
            "stdvar_over_time" => Self::StdvarOverTime,
            "quantile_over_time" => Self::QuantileOverTime,
            "first_over_time" => Self::FirstOverTime,
            "last_over_time" => Self::LastOverTime,
            _ => return None,
        })
    }

    /// Operations that need an `unwrap` to pick the sampled number.
    pub fn needs_unwrap(self) -> bool {
        !matches!(
            self,
            Self::Rate
                | Self::CountOverTime
                | Self::BytesRate
                | Self::BytesOverTime
                | Self::AbsentOverTime
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeAgg {
    pub op: RangeOp,
    /// The quantile of `quantile_over_time`.
    pub param: Option<f64>,
    pub log: LogExpr,
    pub range_ms: i64,
    pub offset_ms: i64,
    pub unwrap: Option<Unwrap>,
    pub grouping: Option<Grouping>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VecOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    Topk,
    Bottomk,
    Sort,
    SortDesc,
}

impl VecOp {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "count" => Self::Count,
            "stddev" => Self::Stddev,
            "stdvar" => Self::Stdvar,
            "topk" => Self::Topk,
            "bottomk" => Self::Bottomk,
            "sort" => Self::Sort,
            "sort_desc" => Self::SortDesc,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VecAgg {
    pub op: VecOp,
    /// `k` of `topk` / `bottomk`.
    pub param: Option<f64>,
    pub grouping: Option<Grouping>,
    pub expr: Expr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinOp {
    Or,
    And,
    Unless,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl BinOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Ne | Self::Gt | Self::Ge | Self::Lt | Self::Le
        )
    }

    pub fn is_set(self) -> bool {
        matches!(self, Self::Or | Self::And | Self::Unless)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Binary {
    pub op: BinOp,
    pub lhs: Expr,
    pub rhs: Expr,
    pub bool_modifier: bool,
    /// `on(...)` (true) or `ignoring(...)` (false) with its labels.
    pub matching: Option<(bool, Vec<String>)>,
    /// `group_left(...)` (true) or `group_right(...)` (false) with the labels to copy.
    pub group: Option<(bool, Vec<String>)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LabelReplace {
    pub expr: Expr,
    pub dst: String,
    pub replacement: String,
    pub src: String,
    pub regex: String,
}
