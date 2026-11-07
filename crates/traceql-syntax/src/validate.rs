//! Validation of a parsed query: operand types, what a filter must evaluate to, regexes, and
//! the features that parse but are not supported (`parent`, scalar filters over anything but an
//! aggregate and a constant, ...). The rules follow Tempo's own validator.

use crate::ast::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidateError {
    /// The query is wrong: a type error, an invalid regex, an illegal argument.
    Invalid(String),
    /// The query is well formed but uses something not supported yet.
    Unsupported(String),
}

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidateError::Invalid(m) => f.write_str(m),
            ValidateError::Unsupported(m) => write!(f, "{m} not yet supported"),
        }
    }
}

impl std::error::Error for ValidateError {}

type Result = std::result::Result<(), ValidateError>;

fn invalid(msg: impl Into<String>) -> ValidateError {
    ValidateError::Invalid(msg.into())
}

/// Validates with the built-in regex syntax check.
pub fn validate(query: &Query) -> Result {
    validate_with(query, &regex_syntax_ok)
}

/// Validates, deciding whether a regex is valid with `regex_ok`; use this to plug in a real regex
/// engine (the built-in check catches syntax errors but not every restriction of an engine).
pub fn validate_with(query: &Query, regex_ok: &dyn Fn(&str) -> bool) -> Result {
    let v = Validator { regex_ok };
    match &query.kind {
        QueryKind::Pipeline(p) => v.pipeline(p),
        QueryKind::Spansets(s) => v.spansets(s),
        QueryKind::ScalarFilter(f) => v.scalar_filter(f),
        QueryKind::Metrics(m) => v.metrics_pipeline(m),
        QueryKind::MetricsMath { expr, second } => {
            v.metrics_expr(expr)?;
            v.second_stages(second)?;
            if (!second.is_empty() || !matches!(expr, MetricsExpr::Pipeline(_))) && has_compare(expr) {
                return Err(invalid("`compare()` cannot be used with second stage functions"));
            }
            Ok(())
        }
    }
}

fn has_compare(e: &MetricsExpr) -> bool {
    match e {
        MetricsExpr::Pipeline(m) => matches!(m.agg.func, MetricsFn::Compare { .. }),
        MetricsExpr::Binary { lhs, rhs, .. } => has_compare(lhs) || has_compare(rhs),
        MetricsExpr::Scalar { expr, .. } => has_compare(expr),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Ty {
    Nil,
    Attribute,
    Str,
    Int,
    Float,
    Bool,
    Duration,
    Status,
    Kind,
}

impl Ty {
    fn numeric(self) -> bool {
        matches!(self, Ty::Int | Ty::Float | Ty::Duration)
    }

    fn matches(self, other: Ty) -> bool {
        self == Ty::Attribute || other == Ty::Attribute || self == other || (self.numeric() && other.numeric()) || self == Ty::Nil || other == Ty::Nil
    }
}

fn binary_type_valid(op: Op, t: Ty) -> bool {
    match t {
        Ty::Attribute => true,
        Ty::Bool => matches!(op, Op::And | Op::Or | Op::Eq | Op::Ne),
        Ty::Int | Ty::Float | Ty::Duration => matches!(
            op,
            Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod | Op::Pow | Op::Eq | Op::Ne | Op::Gt | Op::Ge | Op::Lt | Op::Le
        ),
        Ty::Str => matches!(op, Op::Eq | Op::Ne | Op::Re | Op::NotRe | Op::Gt | Op::Ge | Op::Lt | Op::Le),
        Ty::Nil | Ty::Status | Ty::Kind => matches!(op, Op::Eq | Op::Ne),
    }
}

fn unary_type_valid(op: Op, t: Ty) -> bool {
    if t == Ty::Attribute {
        return true;
    }
    match op {
        Op::Sub => t.numeric(),
        Op::Not => t == Ty::Bool,
        Op::Exists | Op::NotExists => true,
        _ => false,
    }
}

fn static_ty(s: &Static) -> Ty {
    match s {
        Static::Str(_) => Ty::Str,
        Static::Int(_) => Ty::Int,
        Static::Float(_) => Ty::Float,
        Static::Bool(_) => Ty::Bool,
        Static::Duration(_) => Ty::Duration,
        Static::Status(_) => Ty::Status,
        Static::Kind(_) => Ty::Kind,
        Static::Nil => Ty::Nil,
    }
}

fn attribute_ty(a: &Attribute) -> Ty {
    match a.intrinsic {
        None => Ty::Attribute,
        Some(i) => match i {
            Intrinsic::Duration | Intrinsic::EventTimeSinceStart | Intrinsic::TraceDuration => Ty::Duration,
            Intrinsic::ChildCount | Intrinsic::NestedSetLeft | Intrinsic::NestedSetRight | Intrinsic::NestedSetParent => Ty::Int,
            Intrinsic::Status => Ty::Status,
            Intrinsic::Kind => Ty::Kind,
            Intrinsic::Parent => Ty::Nil,
            _ => Ty::Str,
        },
    }
}

fn field_ty(f: &Field) -> Ty {
    match f {
        Field::Static(s) => static_ty(s),
        Field::Attribute(a) => attribute_ty(a),
        Field::Binary { op, lhs, rhs } => {
            if op.is_boolean() {
                Ty::Bool
            } else {
                match field_ty(lhs) {
                    Ty::Attribute => field_ty(rhs),
                    t => t,
                }
            }
        }
        Field::Unary { op, operand } => {
            if matches!(op, Op::Exists | Op::NotExists) {
                Ty::Bool
            } else {
                field_ty(operand)
            }
        }
    }
}

fn scalar_ty(s: &Scalar) -> Ty {
    match s {
        Scalar::Aggregate(a) => match (&a.op, &a.expr) {
            (AggregateOp::Count, _) | (_, None) => Ty::Int,
            (_, Some(e)) => field_ty(e),
        },
        Scalar::Static(s) => static_ty(s),
        Scalar::Binary { lhs, rhs, .. } => match scalar_ty(lhs) {
            Ty::Attribute => scalar_ty(rhs),
            t => t,
        },
        Scalar::Pipeline(p) => match p.elements.last() {
            Some(Element::Aggregate(a)) => scalar_ty(&Scalar::Aggregate(a.clone())),
            _ => Ty::Int,
        },
    }
}

fn references_span(f: &Field) -> bool {
    match f {
        Field::Static(_) => false,
        Field::Attribute(_) => true,
        Field::Binary { lhs, rhs, .. } => references_span(lhs) || references_span(rhs),
        Field::Unary { operand, .. } => references_span(operand),
    }
}

struct Validator<'a> {
    regex_ok: &'a dyn Fn(&str) -> bool,
}

impl Validator<'_> {
    fn attribute(&self, a: &Attribute) -> Result {
        if a.parent {
            return Err(ValidateError::Unsupported("parent".to_string()));
        }
        if a.intrinsic == Some(Intrinsic::Parent) {
            return Err(ValidateError::Unsupported("intrinsic (parent)".to_string()));
        }
        Ok(())
    }

    fn field(&self, f: &Field) -> Result {
        match f {
            Field::Static(_) => Ok(()),
            Field::Attribute(a) => self.attribute(a),
            Field::Binary { op, lhs, rhs } => {
                self.field(lhs)?;
                self.field(rhs)?;
                let (lt, rt) = (field_ty(lhs), field_ty(rhs));
                if !lt.matches(rt) {
                    return Err(invalid("binary operations must operate on the same type"));
                }
                if !binary_type_valid(*op, lt) || !binary_type_valid(*op, rt) {
                    return Err(invalid("illegal operation for the given types"));
                }
                if matches!(op, Op::Re | Op::NotRe) {
                    match rhs.as_ref() {
                        Field::Static(Static::Str(pattern)) => {
                            if !(self.regex_ok)(pattern) {
                                return Err(invalid(format!("invalid regex: {pattern:?}")));
                            }
                        }
                        _ => return Err(invalid("a regex operator needs a constant string on the right")),
                    }
                }
                Ok(())
            }
            Field::Unary { op, operand } => {
                self.field(operand)?;
                if *op == Op::NotExists {
                    if let Field::Attribute(a) = operand.as_ref() {
                        if a.intrinsic.is_some() {
                            return Err(invalid("intrinsics cannot be nil"));
                        }
                        if a.scope == Scope::Resource && !a.parent && a.name == "service.name" {
                            return Err(invalid("resource.service.name cannot be nil"));
                        }
                    }
                }
                let t = field_ty(operand);
                if t != Ty::Attribute && !unary_type_valid(*op, t) {
                    return Err(invalid("illegal operation for the given type"));
                }
                Ok(())
            }
        }
    }

    fn spanset_filter_field(&self, f: &Field) -> Result {
        self.field(f)?;
        let t = field_ty(f);
        if t != Ty::Attribute && t != Ty::Bool {
            return Err(invalid("span filter field expressions must resolve to a boolean"));
        }
        Ok(())
    }

    fn spansets(&self, s: &Spansets) -> Result {
        match s {
            Spansets::Filter(f) => self.spanset_filter_field(f),
            Spansets::Op { lhs, rhs, .. } => {
                self.spansets(lhs)?;
                self.spansets(rhs)
            }
            Spansets::Pipeline(p) => self.pipeline(p),
        }
    }

    fn aggregate(&self, a: &Aggregate) -> Result {
        let Some(e) = &a.expr else { return Ok(()) };
        self.field(e)?;
        let t = field_ty(e);
        if t != Ty::Attribute && !t.numeric() {
            return Err(invalid("aggregate field expressions must resolve to a number type"));
        }
        if !references_span(e) {
            return Err(invalid("aggregate field expressions must reference the span"));
        }
        Ok(())
    }

    fn scalar(&self, s: &Scalar) -> Result {
        match s {
            Scalar::Aggregate(a) => self.aggregate(a),
            Scalar::Static(_) => Ok(()),
            Scalar::Binary { op, lhs, rhs } => {
                self.scalar(lhs)?;
                self.scalar(rhs)?;
                let (lt, rt) = (scalar_ty(lhs), scalar_ty(rhs));
                if !lt.matches(rt) {
                    return Err(invalid("binary operations must operate on the same type"));
                }
                if !binary_type_valid(*op, lt) || !binary_type_valid(*op, rt) {
                    return Err(invalid("illegal operation for the given types"));
                }
                Ok(())
            }
            Scalar::Pipeline(p) => self.pipeline(p),
        }
    }

    fn scalar_filter(&self, f: &ScalarFilter) -> Result {
        self.scalar(&f.lhs)?;
        self.scalar(&f.rhs)?;
        let (lt, rt) = (scalar_ty(&f.lhs), scalar_ty(&f.rhs));
        if !lt.matches(rt) {
            return Err(invalid("binary operations must operate on the same type"));
        }
        if !binary_type_valid(f.op, lt) || !binary_type_valid(f.op, rt) {
            return Err(invalid("illegal operation for the given types"));
        }
        if !matches!(f.lhs, Scalar::Aggregate(_)) {
            return Err(ValidateError::Unsupported("scalar filter lhs".to_string()));
        }
        if !matches!(f.rhs, Scalar::Static(_)) {
            return Err(ValidateError::Unsupported("scalar filter rhs".to_string()));
        }
        Ok(())
    }

    fn pipeline(&self, p: &Pipeline) -> Result {
        for e in &p.elements {
            match e {
                Element::Spanset(s) => self.spansets(s)?,
                Element::ScalarFilter(f) => self.scalar_filter(f)?,
                Element::Group(f) => {
                    if !references_span(f) {
                        return Err(invalid("grouping field expressions must reference the span"));
                    }
                    self.field(f)?;
                }
                Element::Coalesce => {}
                Element::Select(attrs) => attrs.iter().try_for_each(|a| self.attribute(a))?,
                Element::Aggregate(a) => self.aggregate(a)?,
            }
        }
        Ok(())
    }

    fn metrics_pipeline(&self, m: &MetricsPipeline) -> Result {
        self.pipeline(&m.pipeline)?;
        const MAX_GROUP_BYS: usize = 5;
        match &m.agg.func {
            MetricsFn::Rate | MetricsFn::CountOverTime => {}
            MetricsFn::MinOverTime(a) | MetricsFn::MaxOverTime(a) | MetricsFn::AvgOverTime(a) | MetricsFn::SumOverTime(a) => {
                self.attribute(a)?
            }
            MetricsFn::HistogramOverTime(a) => {
                self.attribute(a)?;
                if m.agg.by.len() >= MAX_GROUP_BYS {
                    return Err(ValidateError::Unsupported(format!("metrics group by {} values", m.agg.by.len())));
                }
            }
            MetricsFn::QuantileOverTime(a, quantiles) => {
                self.attribute(a)?;
                if m.agg.by.len() >= MAX_GROUP_BYS {
                    return Err(ValidateError::Unsupported(format!("metrics group by {} values", m.agg.by.len())));
                }
                if let Some(q) = quantiles.iter().find(|q| **q < 0.0 || **q > 1.0) {
                    return Err(invalid(format!("quantile must be between 0 and 1: {q}")));
                }
            }
            MetricsFn::Compare { filter, top_n, start, end } => {
                self.spanset_filter_field(filter)?;
                if *top_n <= 0 || *top_n > 1000 {
                    return Err(invalid("compare() top number of values must be an integer between 1 and 1000"));
                }
                if (*start != 0 || *end != 0) && (*start <= 0 || *end <= 0) {
                    return Err(invalid("compare() timestamps must be positive integer unix nanoseconds"));
                }
                if (*start != 0 || *end != 0) && end <= start {
                    return Err(invalid("compare() end timestamp must be greater than start timestamp"));
                }
                if !m.second.is_empty() {
                    return Err(invalid("`compare()` cannot be used with second stage functions"));
                }
            }
        }
        for a in &m.agg.by {
            self.attribute(a)?;
        }
        self.second_stages(&m.second)
    }

    fn second_stages(&self, stages: &[SecondStage]) -> Result {
        for s in stages {
            if let SecondStage::TopK(k) | SecondStage::BottomK(k) = s {
                if *k <= 0 {
                    return Err(invalid("topk and bottomk need a limit greater than 0"));
                }
            }
        }
        Ok(())
    }

    fn metrics_expr(&self, e: &MetricsExpr) -> Result {
        match e {
            MetricsExpr::Pipeline(m) => self.metrics_pipeline(m),
            MetricsExpr::Binary { lhs, rhs, .. } => {
                self.metrics_expr(lhs)?;
                self.metrics_expr(rhs)
            }
            MetricsExpr::Scalar { expr, .. } => self.metrics_expr(expr),
        }
    }
}

/// A regex syntax check: balanced groups and classes, valid escapes, repetition only after
/// something repeatable. It is not a full engine; supply one with [`validate_with`] for that.
pub fn regex_syntax_ok(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    let mut depth = 0usize;
    // Whether the previous item can take a repetition operator.
    let mut repeatable = false;
    let mut after_repeat = false;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                let Some(&e) = chars.get(i + 1) else { return false };
                if e.is_ascii_digit() && e != '0' || (e.is_alphabetic() && !"dDsSwWbBAzZnrtfvaxpPQE".contains(e)) {
                    return false;
                }
                i += 2;
                repeatable = !"bBAz".contains(e);
                after_repeat = false;
                continue;
            }
            '[' => {
                i += 1;
                if chars.get(i) == Some(&'^') {
                    i += 1;
                }
                if chars.get(i) == Some(&']') {
                    i += 1;
                }
                loop {
                    match chars.get(i) {
                        None => return false,
                        Some(']') => break,
                        Some('[') if chars.get(i + 1) == Some(&':') => match pattern_find(&chars, i + 2, ":]") {
                            Some(end) => i = end + 2,
                            None => return false,
                        },
                        Some('\\') => i += 2,
                        Some(&lo) => {
                            if chars.get(i + 1) == Some(&'-') && chars.get(i + 2).is_some_and(|c| *c != ']' && *c != '\\') && chars[i + 2] < lo {
                                return false;
                            }
                            i += 1;
                        }
                    }
                }
                repeatable = true;
                after_repeat = false;
            }
            '(' => {
                depth += 1;
                if chars.get(i + 1) == Some(&'?') {
                    let mut j = i + 2;
                    if chars.get(j) == Some(&'P') {
                        j += 1;
                    }
                    if chars.get(j) == Some(&'<') {
                        match pattern_find(&chars, j + 1, ">") {
                            Some(end) if end > j + 1 => i = end,
                            _ => return false,
                        }
                    } else {
                        while chars.get(j).is_some_and(|c| "imsU-".contains(*c)) {
                            j += 1;
                        }
                        match chars.get(j) {
                            Some(':') => i = j,
                            Some(')') => {
                                // flags only: `(?i)`
                                depth -= 1;
                                i = j + 1;
                                repeatable = false;
                                after_repeat = false;
                                continue;
                            }
                            _ => return false,
                        }
                    }
                }
                repeatable = false;
                after_repeat = false;
            }
            ')' => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
                repeatable = true;
                after_repeat = false;
            }
            '|' => {
                repeatable = false;
                after_repeat = false;
            }
            '*' | '+' | '?' => {
                if after_repeat && c == '?' {
                    // lazy quantifier: `*?`, `+?`, `??`
                    after_repeat = false;
                } else if !repeatable || after_repeat {
                    return false;
                } else {
                    after_repeat = true;
                }
            }
            '{' => match repeat_at(&chars, i) {
                Repeat::Valid(len) => {
                    if !repeatable || after_repeat {
                        return false;
                    }
                    i += len;
                    after_repeat = true;
                    continue;
                }
                Repeat::Invalid => return false,
                Repeat::NotARepeat => {
                    repeatable = true;
                    after_repeat = false;
                }
            },
            '^' | '$' => {
                repeatable = false;
                after_repeat = false;
            }
            _ => {
                repeatable = true;
                after_repeat = false;
            }
        }
        i += 1;
    }
    depth == 0
}

fn pattern_find(chars: &[char], from: usize, needle: &str) -> Option<usize> {
    let needle: Vec<char> = needle.chars().collect();
    (from..chars.len()).find(|&i| chars[i..].starts_with(&needle))
}

enum Repeat {
    /// A well-formed `{n}`, `{n,}` or `{n,m}` of this length.
    Valid(usize),
    /// Written like a repeat but with counts a regex engine rejects.
    Invalid,
    /// Just a literal `{`.
    NotARepeat,
}

fn repeat_at(chars: &[char], start: usize) -> Repeat {
    let Some(close) = (start..chars.len()).find(|&i| chars[i] == '}') else { return Repeat::NotARepeat };
    let body: String = chars[start + 1..close].iter().collect();
    let digits = |t: &str| !t.is_empty() && t.chars().all(|c| c.is_ascii_digit());
    let (min, max) = match body.split_once(',') {
        Some((a, b)) if digits(a) && (b.is_empty() || digits(b)) => (a.parse::<u32>(), if b.is_empty() { Ok(None) } else { b.parse::<u32>().map(Some) }),
        None if digits(&body) => (body.parse::<u32>(), body.parse::<u32>().map(Some)),
        _ => return Repeat::NotARepeat,
    };
    let (Ok(min), Ok(max)) = (min, max) else { return Repeat::Invalid };
    if min > 1000 || max.is_some_and(|m| m > 1000 || m < min) {
        return Repeat::Invalid;
    }
    Repeat::Valid(close - start + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_syntax_check_accepts_valid_and_rejects_broken_patterns() {
        for ok in ["test", ".*", "a.*b", "(a|b)+", "[a-z]{2,4}", r"\d+\.\d*", "(?i)abc", "(?:x)?y", "(?P<n>a)", "[[:alpha:]]", "a{,3}", "^a$"] {
            assert!(regex_syntax_ok(ok), "{ok} should be valid");
        }
        for bad in ["[", "(", ")", "*a", "a**", "a{2,1}", r"a\", "[z-a]", "(?=a)", r"\1", "a|*"] {
            assert!(!regex_syntax_ok(bad), "{bad} should be invalid");
        }
    }
}
