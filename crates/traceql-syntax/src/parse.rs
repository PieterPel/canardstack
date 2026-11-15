//! TraceQL parser. A backtracking recursive descent that follows the structure of Tempo's grammar:
//! a query is a spanset pipeline (optionally ending in a metrics function), an expression over
//! parenthesised pipelines, a scalar filter over parenthesised pipelines, or arithmetic over
//! metrics queries; each alternative is tried in turn and the one that consumes the whole input
//! wins.

use crate::ast::*;
use crate::lex::{intrinsic_keyword, lex, Tok, P};
use crate::ParseError;

pub fn parse(input: &str) -> crate::Result<Query> {
    let mut p = Parser {
        toks: lex(input)?,
        i: 0,
        furthest: 0,
    };
    type Alt = fn(&mut Parser) -> Option<QueryKind>;
    let alternatives: [Alt; 4] = [
        Parser::root_pipeline,
        Parser::root_spansets,
        Parser::root_scalar_filter,
        Parser::root_metrics_math,
    ];
    for alt in alternatives {
        p.i = 0;
        let Some(kind) = alt(&mut p) else { continue };
        let mut hints = Vec::new();
        while p.at_word("with") {
            match p.hints() {
                Some(h) => hints.extend(h),
                None => break,
            }
        }
        if p.peek() == &Tok::Eof {
            return Ok(Query { kind, hints });
        }
    }
    let (position, tok) = &p.toks[p.furthest.min(p.toks.len() - 1)];
    Err(ParseError {
        position: *position,
        message: format!("syntax error: unexpected {}", describe(tok)),
    })
}

fn describe(tok: &Tok) -> String {
    match tok {
        Tok::Eof => "end of query".to_string(),
        Tok::Word(w) => format!("{w:?}"),
        Tok::Str(s) => format!("string {s:?}"),
        Tok::Int(i) => format!("integer {i}"),
        Tok::Float(f) => format!("number {f}"),
        Tok::Duration(_) => "duration".to_string(),
        Tok::Attr(a) => format!("attribute {:?}", a.name),
        Tok::P(p) => format!("{p:?}"),
    }
}

struct Parser {
    toks: Vec<(usize, Tok)>,
    i: usize,
    /// The furthest token any alternative reached, for the error message.
    furthest: usize,
}

type Fold = Option<Static>;

fn fold(op: Op, a: &Static, b: &Static) -> Fold {
    if matches!(a, Static::Duration(_)) || matches!(b, Static::Duration(_)) {
        return None; // duration cannot be used in scalar arithmetic
    }
    Some(match (a, b) {
        (Static::Int(x), Static::Int(y)) => match op {
            Op::Add => Static::Int(x.checked_add(*y)?),
            Op::Sub => Static::Int(x.checked_sub(*y)?),
            Op::Mul => Static::Int(x.checked_mul(*y)?),
            Op::Div => Static::Int(x.checked_div(*y)?),
            Op::Mod => Static::Int(x.checked_rem(*y)?),
            Op::Pow => Static::Int(x.checked_pow(u32::try_from(*y).ok()?)?),
            _ => return None,
        },
        _ => {
            let (x, y) = (as_f64(a)?, as_f64(b)?);
            Static::Float(match op {
                Op::Add => x + y,
                Op::Sub => x - y,
                Op::Mul => x * y,
                Op::Div => x / y,
                Op::Mod => x % y,
                Op::Pow => x.powf(y),
                _ => return None,
            })
        }
    })
}

fn as_f64(s: &Static) -> Option<f64> {
    match s {
        Static::Int(i) => Some(*i as f64),
        Static::Float(f) => Some(*f),
        _ => None,
    }
}

fn negate(s: Static) -> Fold {
    Some(match s {
        Static::Int(i) => Static::Int(i.checked_neg()?),
        Static::Float(f) => Static::Float(-f),
        Static::Duration(d) => Static::Duration(d.checked_neg()?),
        _ => return None,
    })
}

fn contains_nil(f: &Field) -> bool {
    match f {
        Field::Static(Static::Nil) => true,
        Field::Binary { lhs, rhs, .. } => contains_nil(lhs) || contains_nil(rhs),
        Field::Unary { operand, .. } => contains_nil(operand),
        _ => false,
    }
}

fn contains_aggregate(s: &Scalar) -> bool {
    match s {
        Scalar::Aggregate(_) | Scalar::Pipeline(_) => true,
        Scalar::Static(_) => false,
        Scalar::Binary { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
    }
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.i].1
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.i].1.clone();
        if self.i < self.toks.len() - 1 {
            self.i += 1;
            self.furthest = self.furthest.max(self.i);
        }
        t
    }

    fn eat(&mut self, p: P) -> bool {
        if self.peek() == &Tok::P(p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn at_word(&self, w: &str) -> bool {
        matches!(self.peek(), Tok::Word(x) if x == w)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.at_word(w) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, p: P) -> Option<()> {
        self.eat(p).then_some(())
    }

    /// Runs `f`; if it fails, rewinds.
    fn attempt<T>(&mut self, f: impl FnOnce(&mut Self) -> Option<T>) -> Option<T> {
        let save = self.i;
        let out = f(self);
        if out.is_none() {
            self.i = save;
        }
        out
    }

    // ---- roots ------------------------------------------------------------------------

    fn root_pipeline(&mut self) -> Option<QueryKind> {
        let pipeline = self.pipeline()?;
        if self.peek() == &Tok::P(P::Pipe) {
            let save = self.i;
            self.bump();
            if let Some(agg) = self.metrics_agg() {
                let second = self.second_stages();
                return Some(QueryKind::Metrics(MetricsPipeline {
                    pipeline,
                    agg,
                    second,
                }));
            }
            self.i = save;
        }
        Some(QueryKind::Pipeline(pipeline))
    }

    fn root_spansets(&mut self) -> Option<QueryKind> {
        Some(QueryKind::Spansets(self.spanset_pipeline_expr(1)?))
    }

    fn root_scalar_filter(&mut self) -> Option<QueryKind> {
        let lhs = self.scalar_pipeline_expr(1)?;
        let op = self.cmp_op()?;
        let rhs = self
            .attempt(|p| p.scalar_pipeline_expr(1))
            .or_else(|| self.lit_expr(1).map(Scalar::Static))?;
        Some(QueryKind::ScalarFilter(ScalarFilter { op, lhs, rhs }))
    }

    fn root_metrics_math(&mut self) -> Option<QueryKind> {
        let Operand::Metrics(expr) = self.metrics_expr(1)? else {
            return None;
        };
        let second = self.second_stages();
        Some(QueryKind::MetricsMath { expr, second })
    }

    // ---- hints ------------------------------------------------------------------------

    fn hints(&mut self) -> Option<Vec<Hint>> {
        self.attempt(|p| {
            p.eat_word("with").then_some(())?;
            p.expect(P::LParen)?;
            let mut hints = Vec::new();
            loop {
                let Tok::Word(name) = p.bump() else {
                    return None;
                };
                p.expect(P::Eq)?;
                hints.push(Hint {
                    name,
                    value: p.static_value()?,
                });
                if !p.eat(P::Comma) {
                    break;
                }
            }
            p.expect(P::RParen)?;
            Some(hints)
        })
    }

    fn static_value(&mut self) -> Option<Static> {
        Some(match self.bump() {
            Tok::Str(s) => Static::Str(s),
            Tok::Int(i) => Static::Int(i),
            Tok::Float(f) => Static::Float(f),
            Tok::Duration(d) => Static::Duration(d),
            Tok::Word(w) => word_static(&w)?,
            _ => return None,
        })
    }

    // ---- pipelines --------------------------------------------------------------------

    fn pipeline(&mut self) -> Option<Pipeline> {
        let mut elements = vec![self.element(true)?];
        loop {
            let save = self.i;
            if !self.eat(P::Pipe) {
                break;
            }
            match self.element(false) {
                Some(e) => elements.push(e),
                None => {
                    self.i = save;
                    break;
                }
            }
        }
        Some(Pipeline { elements })
    }

    fn element(&mut self, first: bool) -> Option<Element> {
        if let Some(s) = self.attempt(|p| p.spanset_expr(1)) {
            return Some(Element::Spanset(s));
        }
        if let Some(f) = self.attempt(Self::scalar_filter) {
            return Some(Element::ScalarFilter(f));
        }
        if self.at_word("by") {
            return self.attempt(|p| {
                p.bump();
                p.expect(P::LParen)?;
                let expr = p.field(1).filter(|f| !contains_nil(f))?;
                p.expect(P::RParen)?;
                Some(Element::Group(expr))
            });
        }
        if self.at_word("coalesce") && !first {
            return self.attempt(|p| {
                p.bump();
                p.expect(P::LParen)?;
                p.expect(P::RParen)?;
                Some(Element::Coalesce)
            });
        }
        if self.at_word("select") {
            return self.attempt(|p| {
                p.bump();
                p.expect(P::LParen)?;
                let attrs = p.attribute_list()?;
                p.expect(P::RParen)?;
                Some(Element::Select(attrs))
            });
        }
        None
    }

    fn attribute(&mut self) -> Option<Attribute> {
        match self.peek().clone() {
            Tok::Attr(a) => {
                self.bump();
                Some(a)
            }
            Tok::Word(w) => {
                let intrinsic = intrinsic_keyword(&w)?;
                self.bump();
                Some(Attribute {
                    scope: Scope::None,
                    parent: false,
                    intrinsic: Some(intrinsic),
                    name: w,
                })
            }
            _ => None,
        }
    }

    fn attribute_list(&mut self) -> Option<Vec<Attribute>> {
        let mut attrs = vec![self.attribute()?];
        while self.eat(P::Comma) {
            attrs.push(self.attribute()?);
        }
        Some(attrs)
    }

    // ---- spansets ---------------------------------------------------------------------

    fn spanset_op(&self) -> Option<(SpansetOp, u8)> {
        let Tok::P(p) = self.peek() else { return None };
        Some(match p {
            P::And => (SpansetOp::And, 1),
            P::Or => (SpansetOp::Union, 1),
            P::Gt => (SpansetOp::Child, 2),
            P::Lt => (SpansetOp::Parent, 2),
            P::Desc => (SpansetOp::Descendant, 2),
            P::Anc => (SpansetOp::Ancestor, 2),
            P::Sibl => (SpansetOp::Sibling, 2),
            P::NotChild => (SpansetOp::NotChild, 2),
            P::NotParent => (SpansetOp::NotParent, 2),
            P::NotDesc => (SpansetOp::NotDescendant, 2),
            P::NotAnce => (SpansetOp::NotAncestor, 2),
            P::NotRe => (SpansetOp::NotSibling, 2),
            P::UnionChild => (SpansetOp::UnionChild, 2),
            P::UnionParent => (SpansetOp::UnionParent, 2),
            P::UnionDesc => (SpansetOp::UnionDescendant, 2),
            P::UnionAnce => (SpansetOp::UnionAncestor, 2),
            P::UnionSibl => (SpansetOp::UnionSibling, 2),
            _ => return None,
        })
    }

    fn spanset_filter(&mut self) -> Option<Spansets> {
        self.expect(P::LBrace)?;
        if self.eat(P::RBrace) {
            return Some(Spansets::Filter(Field::Static(Static::Bool(true))));
        }
        let expr = self.field(1).filter(|f| !contains_nil(f))?;
        self.expect(P::RBrace)?;
        Some(Spansets::Filter(expr))
    }

    fn spanset_expr(&mut self, min: u8) -> Option<Spansets> {
        let mut lhs = if self.eat(P::LParen) {
            let e = self.spanset_expr(1)?;
            self.expect(P::RParen)?;
            e
        } else {
            self.spanset_filter()?
        };
        while let Some((op, level)) = self.spanset_op() {
            if level < min {
                break;
            }
            self.bump();
            let rhs = self.spanset_expr(level + 1)?;
            lhs = Spansets::Op {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Some(lhs)
    }

    /// Spanset operators between parenthesised pipelines.
    fn spanset_pipeline_expr(&mut self, min: u8) -> Option<Spansets> {
        let mut lhs = if let Some(p) = self.attempt(|p| {
            p.expect(P::LParen)?;
            let pipeline = p.pipeline()?;
            p.expect(P::RParen)?;
            Some(Spansets::Pipeline(Box::new(pipeline)))
        }) {
            p
        } else {
            self.expect(P::LParen)?;
            let e = self.spanset_pipeline_expr(1)?;
            self.expect(P::RParen)?;
            e
        };
        while let Some((op, level)) = self.spanset_op() {
            if level < min {
                break;
            }
            self.bump();
            let rhs = self.spanset_pipeline_expr(level + 1)?;
            lhs = Spansets::Op {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Some(lhs)
    }

    // ---- field expressions ------------------------------------------------------------

    fn field_op(&self) -> Option<(Op, u8, bool)> {
        let Tok::P(p) = self.peek() else { return None };
        Some(match p {
            P::And => (Op::And, 1, false),
            P::Or => (Op::Or, 1, false),
            P::Eq => (Op::Eq, 2, false),
            P::Ne => (Op::Ne, 2, false),
            P::Gt => (Op::Gt, 2, false),
            P::Ge => (Op::Ge, 2, false),
            P::Lt => (Op::Lt, 2, false),
            P::Le => (Op::Le, 2, false),
            P::Re => (Op::Re, 2, false),
            P::NotRe => (Op::NotRe, 2, false),
            P::Add => (Op::Add, 3, false),
            P::Sub => (Op::Sub, 3, false),
            P::Mul => (Op::Mul, 4, false),
            P::Div => (Op::Div, 4, false),
            P::Mod => (Op::Mod, 4, false),
            P::Pow => (Op::Pow, 5, true),
            _ => return None,
        })
    }

    fn field(&mut self, min: u8) -> Option<Field> {
        let mut lhs = self.field_unary()?;
        while let Some((op, level, right)) = self.field_op() {
            if level < min {
                break;
            }
            self.bump();
            let rhs = self.field(if right { level } else { level + 1 })?;
            lhs = make_binary(op, lhs, rhs)?;
        }
        Some(lhs)
    }

    fn field_unary(&mut self) -> Option<Field> {
        if self.eat(P::Sub) {
            let operand = self.field(4)?;
            return Some(Field::Unary {
                op: Op::Sub,
                operand: Box::new(operand),
            });
        }
        if self.eat(P::Not) {
            let operand = self.field(4)?;
            return Some(Field::Unary {
                op: Op::Not,
                operand: Box::new(operand),
            });
        }
        self.field_primary()
    }

    fn field_primary(&mut self) -> Option<Field> {
        if self.eat(P::LParen) {
            let e = self.field(1)?;
            self.expect(P::RParen)?;
            return Some(e);
        }
        match self.peek().clone() {
            Tok::Str(s) => {
                self.bump();
                Some(Field::Static(Static::Str(s)))
            }
            Tok::Int(i) => {
                self.bump();
                Some(Field::Static(Static::Int(i)))
            }
            Tok::Float(f) => {
                self.bump();
                Some(Field::Static(Static::Float(f)))
            }
            Tok::Duration(d) => {
                self.bump();
                Some(Field::Static(Static::Duration(d)))
            }
            Tok::Attr(a) => {
                self.bump();
                Some(Field::Attribute(a))
            }
            Tok::Word(w) => {
                if w == "nil" {
                    self.bump();
                    return Some(Field::Static(Static::Nil));
                }
                if let Some(s) = word_static(&w) {
                    self.bump();
                    return Some(Field::Static(s));
                }
                self.attribute().map(Field::Attribute)
            }
            _ => None,
        }
    }

    // ---- scalars ----------------------------------------------------------------------

    fn cmp_op(&mut self) -> Option<Op> {
        let op = match self.peek() {
            Tok::P(P::Eq) => Op::Eq,
            Tok::P(P::Ne) => Op::Ne,
            Tok::P(P::Lt) => Op::Lt,
            Tok::P(P::Le) => Op::Le,
            Tok::P(P::Gt) => Op::Gt,
            Tok::P(P::Ge) => Op::Ge,
            _ => return None,
        };
        self.bump();
        Some(op)
    }

    fn aggregate(&mut self) -> Option<Aggregate> {
        let Tok::Word(w) = self.peek().clone() else {
            return None;
        };
        let op = match w.as_str() {
            "count" => AggregateOp::Count,
            "max" => AggregateOp::Max,
            "min" => AggregateOp::Min,
            "avg" => AggregateOp::Avg,
            "sum" => AggregateOp::Sum,
            _ => return None,
        };
        self.attempt(|p| {
            p.bump();
            p.expect(P::LParen)?;
            let expr = if op == AggregateOp::Count {
                None
            } else {
                Some(p.field(1).filter(|f| !contains_nil(f))?)
            };
            p.expect(P::RParen)?;
            Some(Aggregate { op, expr })
        })
    }

    /// A number, duration or negated/parenthesised constant arithmetic over them.
    fn lit_atom(&mut self) -> Option<Static> {
        match self.peek().clone() {
            Tok::Int(i) => {
                self.bump();
                Some(Static::Int(i))
            }
            Tok::Float(f) => {
                self.bump();
                Some(Static::Float(f))
            }
            Tok::Duration(d) => {
                self.bump();
                Some(Static::Duration(d))
            }
            Tok::P(P::Sub) => {
                self.bump();
                let atom = self.lit_atom()?;
                negate(atom)
            }
            Tok::P(P::LParen) => self.attempt(|p| {
                p.bump();
                let e = p.lit_expr(1)?;
                p.expect(P::RParen)?;
                Some(e)
            }),
            _ => None,
        }
    }

    fn arith_op(&self, with_mod_pow: bool) -> Option<(Op, u8, bool)> {
        let Tok::P(p) = self.peek() else { return None };
        Some(match p {
            P::Add => (Op::Add, 1, false),
            P::Sub => (Op::Sub, 1, false),
            P::Mul => (Op::Mul, 2, false),
            P::Div => (Op::Div, 2, false),
            P::Mod if with_mod_pow => (Op::Mod, 2, false),
            P::Pow if with_mod_pow => (Op::Pow, 3, true),
            _ => return None,
        })
    }

    /// Constant arithmetic (`1 / 2`, `-(3 + 1)`), folded; durations cannot take part.
    fn lit_expr(&mut self, min: u8) -> Option<Static> {
        let mut lhs = self.lit_atom()?;
        while let Some((op, level, _)) = self.arith_op(false) {
            if level < min {
                break;
            }
            let save = self.i;
            self.bump();
            match self.lit_expr(level + 1) {
                Some(rhs) => lhs = fold(op, &lhs, &rhs)?,
                None => {
                    self.i = save;
                    break;
                }
            }
        }
        Some(lhs)
    }

    /// Arithmetic over aggregates and constants.
    fn scalar_expr(&mut self, min: u8) -> Option<Scalar> {
        let mut lhs = if let Some(a) = self.aggregate() {
            Scalar::Aggregate(a)
        } else if let Some(inner) = self.attempt(|p| {
            p.expect(P::LParen)?;
            let e = p.scalar_expr(1)?;
            p.expect(P::RParen)?;
            Some(e)
        }) {
            inner
        } else {
            Scalar::Static(self.lit_atom()?)
        };
        while let Some((op, level, right)) = self.arith_op(true) {
            if level < min {
                break;
            }
            let save = self.i;
            self.bump();
            let Some(rhs) = self.scalar_expr(if right { level } else { level + 1 }) else {
                self.i = save;
                break;
            };
            lhs = match (&lhs, &rhs) {
                (Scalar::Static(a), Scalar::Static(b)) => Scalar::Static(fold(op, a, b)?),
                _ => Scalar::Binary {
                    op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                },
            };
        }
        Some(lhs)
    }

    /// `avg(duration) > 1s`, `count() = 2`: at least one side holds an aggregate.
    fn scalar_filter(&mut self) -> Option<ScalarFilter> {
        let lhs = self.scalar_expr(1)?;
        let op = self.cmp_op()?;
        let rhs = self.scalar_expr(1)?;
        if !contains_aggregate(&lhs) && !contains_aggregate(&rhs) {
            return None;
        }
        Some(ScalarFilter { op, lhs, rhs })
    }

    /// Arithmetic over parenthesised `( pipeline | aggregate )` scalars.
    fn scalar_pipeline_expr(&mut self, min: u8) -> Option<Scalar> {
        let mut lhs = if let Some(p) = self.attempt(|p| {
            p.expect(P::LParen)?;
            let mut pipeline = p.pipeline()?;
            p.expect(P::Pipe)?;
            let aggregate = p.aggregate()?;
            p.expect(P::RParen)?;
            pipeline.elements.push(Element::Aggregate(aggregate));
            Some(Scalar::Pipeline(Box::new(pipeline)))
        }) {
            p
        } else {
            self.expect(P::LParen)?;
            let e = self.scalar_pipeline_expr(1)?;
            self.expect(P::RParen)?;
            e
        };
        while let Some((op, level, right)) = self.arith_op(true) {
            if level < min {
                break;
            }
            let save = self.i;
            self.bump();
            let Some(rhs) = self.scalar_pipeline_expr(if right { level } else { level + 1 }) else {
                self.i = save;
                break;
            };
            lhs = Scalar::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Some(lhs)
    }

    // ---- metrics ----------------------------------------------------------------------

    fn by_clause(&mut self) -> Option<Vec<Attribute>> {
        if !self.at_word("by") {
            return Some(Vec::new());
        }
        self.attempt(|p| {
            p.bump();
            p.expect(P::LParen)?;
            let attrs = p.attribute_list()?;
            p.expect(P::RParen)?;
            Some(attrs)
        })
    }

    fn metrics_agg(&mut self) -> Option<MetricsAgg> {
        let Tok::Word(w) = self.peek().clone() else {
            return None;
        };
        self.attempt(|p| {
            p.bump();
            p.expect(P::LParen)?;
            let func = match w.as_str() {
                "rate" => MetricsFn::Rate,
                "count_over_time" => MetricsFn::CountOverTime,
                "min_over_time" => MetricsFn::MinOverTime(p.attribute()?),
                "max_over_time" => MetricsFn::MaxOverTime(p.attribute()?),
                "avg_over_time" => MetricsFn::AvgOverTime(p.attribute()?),
                "sum_over_time" => MetricsFn::SumOverTime(p.attribute()?),
                "histogram_over_time" => MetricsFn::HistogramOverTime(p.attribute()?),
                "quantile_over_time" => {
                    let attr = p.attribute()?;
                    p.expect(P::Comma)?;
                    let mut quantiles = Vec::new();
                    loop {
                        quantiles.push(match p.bump() {
                            Tok::Int(i) => i as f64,
                            Tok::Float(f) => f,
                            _ => return None,
                        });
                        if !p.eat(P::Comma) {
                            break;
                        }
                    }
                    MetricsFn::QuantileOverTime(attr, quantiles)
                }
                "compare" => {
                    let Spansets::Filter(filter) = p.spanset_filter()? else {
                        return None;
                    };
                    let (mut top_n, mut start, mut end) = (10, 0, 0);
                    if p.eat(P::Comma) {
                        let Tok::Int(n) = p.bump() else { return None };
                        top_n = n;
                        if p.eat(P::Comma) {
                            let Tok::Int(s) = p.bump() else { return None };
                            p.expect(P::Comma)?;
                            let Tok::Int(e) = p.bump() else { return None };
                            (start, end) = (s, e);
                        }
                    }
                    MetricsFn::Compare {
                        filter,
                        top_n,
                        start,
                        end,
                    }
                }
                _ => return None,
            };
            p.expect(P::RParen)?;
            let by = if matches!(func, MetricsFn::Compare { .. }) {
                Vec::new()
            } else {
                p.by_clause()?
            };
            Some(MetricsAgg { func, by })
        })
    }

    fn second_stages(&mut self) -> Vec<SecondStage> {
        let mut stages = Vec::new();
        loop {
            let stage = self.attempt(|p| {
                if p.eat(P::Pipe) {
                    let Tok::Word(w) = p.bump() else { return None };
                    let top = match w.as_str() {
                        "topk" => true,
                        "bottomk" => false,
                        _ => return None,
                    };
                    p.expect(P::LParen)?;
                    let Tok::Int(k) = p.bump() else { return None };
                    p.expect(P::RParen)?;
                    return Some(if top {
                        SecondStage::TopK(k)
                    } else {
                        SecondStage::BottomK(k)
                    });
                }
                let op = p.cmp_op()?;
                Some(SecondStage::Filter {
                    op,
                    value: p.lit_expr(1)?,
                })
            });
            match stage {
                Some(s) => stages.push(s),
                None => return stages,
            }
        }
    }

    fn wrapped_metrics(&mut self) -> Option<MetricsPipeline> {
        self.attempt(|p| {
            p.expect(P::LParen)?;
            let pipeline = p.pipeline()?;
            p.expect(P::Pipe)?;
            let agg = p.metrics_agg()?;
            let second = p.second_stages();
            p.expect(P::RParen)?;
            Some(MetricsPipeline {
                pipeline,
                agg,
                second,
            })
        })
    }

    /// Arithmetic over parenthesised metrics queries and constants. At least one operand must be
    /// a metrics query.
    fn metrics_expr(&mut self, min: u8) -> Option<Operand> {
        let mut lhs = if let Some(m) = self.wrapped_metrics() {
            Operand::Metrics(MetricsExpr::Pipeline(Box::new(m)))
        } else if let Some(inner) = self.attempt(|p| {
            p.expect(P::LParen)?;
            let e = p.metrics_expr(1)?;
            p.expect(P::RParen)?;
            Some(e)
        }) {
            inner
        } else {
            Operand::Lit(self.lit_atom()?)
        };
        while let Some((op, level, _)) = self.arith_op(false) {
            if level < min {
                break;
            }
            let save = self.i;
            self.bump();
            let Some(rhs) = self.metrics_expr(level + 1) else {
                self.i = save;
                break;
            };
            lhs = combine(op, lhs, rhs)?;
        }
        Some(lhs)
    }
}

enum Operand {
    Metrics(MetricsExpr),
    Lit(Static),
}

fn combine(op: Op, lhs: Operand, rhs: Operand) -> Option<Operand> {
    let scalar = |s: &Static| match s {
        Static::Duration(_) => None, // a duration cannot be a scalar operand in metric arithmetic
        other => as_f64(other),
    };
    Some(match (lhs, rhs) {
        (Operand::Lit(a), Operand::Lit(b)) => Operand::Lit(fold(op, &a, &b)?),
        (Operand::Metrics(a), Operand::Metrics(b)) => Operand::Metrics(MetricsExpr::Binary {
            op,
            lhs: Box::new(a),
            rhs: Box::new(b),
        }),
        (Operand::Metrics(m), Operand::Lit(s)) => Operand::Metrics(MetricsExpr::Scalar {
            op,
            value: scalar(&s)?,
            expr: Box::new(m),
            scalar_on_left: false,
        }),
        (Operand::Lit(s), Operand::Metrics(m)) => Operand::Metrics(MetricsExpr::Scalar {
            op,
            value: scalar(&s)?,
            expr: Box::new(m),
            scalar_on_left: true,
        }),
    })
}

fn make_binary(op: Op, lhs: Field, rhs: Field) -> Option<Field> {
    let nil_l = matches!(lhs, Field::Static(Static::Nil));
    let nil_r = matches!(rhs, Field::Static(Static::Nil));
    if !nil_l && !nil_r {
        return Some(Field::Binary {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        });
    }
    if nil_l && nil_r {
        return matches!(op, Op::Eq | Op::Ne).then_some(Field::Static(Static::Bool(false)));
    }
    let other = if nil_l { rhs } else { lhs };
    let op = match op {
        Op::Eq => Op::NotExists,
        Op::Ne => Op::Exists,
        _ => return None,
    };
    Some(Field::Unary {
        op,
        operand: Box::new(other),
    })
}

fn word_static(w: &str) -> Option<Static> {
    Some(match w {
        "true" => Static::Bool(true),
        "false" => Static::Bool(false),
        "ok" => Static::Status(Status::Ok),
        "error" => Static::Status(Status::Error),
        "unset" => Static::Status(Status::Unset),
        "unspecified" => Static::Kind(Kind::Unspecified),
        "internal" => Static::Kind(Kind::Internal),
        "server" => Static::Kind(Kind::Server),
        "client" => Static::Kind(Kind::Client),
        "producer" => Static::Kind(Kind::Producer),
        "consumer" => Static::Kind(Kind::Consumer),
        "minInt" => Static::Int(i64::MIN),
        "maxInt" => Static::Int(i64::MAX),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(q: &str) -> Field {
        let QueryKind::Pipeline(p) = parse(q).unwrap().kind else {
            panic!("not a pipeline: {q}")
        };
        let Element::Spanset(Spansets::Filter(f)) = p.elements.into_iter().next().unwrap() else {
            panic!("not a filter: {q}")
        };
        f
    }

    fn attr(scope: Scope, parent: bool, name: &str) -> Field {
        Field::Attribute(Attribute {
            scope,
            parent,
            intrinsic: None,
            name: name.to_string(),
        })
    }

    #[test]
    fn attributes_keep_their_scope_and_odd_names() {
        assert_eq!(
            filter("{ .http.status }"),
            attr(Scope::None, false, "http.status")
        );
        assert_eq!(
            filter("{ resource.service.name }"),
            attr(Scope::Resource, false, "service.name")
        );
        assert_eq!(filter("{ parent.span.a }"), attr(Scope::Span, true, "a"));
        assert_eq!(
            filter(r#"{ span."foo bar" }"#),
            attr(Scope::Span, false, "foo bar")
        );
        assert_eq!(filter(r#"{ ."a\"b" }"#), attr(Scope::None, false, "a\"b"));
        // `-` and `/` belong to the name when there is no space around them.
        assert_eq!(filter("{ .a-b }"), attr(Scope::None, false, "a-b"));
        assert!(matches!(
            filter("{ span:duration }"),
            Field::Attribute(Attribute {
                intrinsic: Some(Intrinsic::Duration),
                ..
            })
        ));
    }

    #[test]
    fn field_operators_follow_tempos_precedence() {
        // && and || share one level and group to the left; comparison binds tighter than both.
        let Field::Binary { op, lhs, .. } = filter("{ .a = 1 && .b = 2 || .c = 3 }") else {
            panic!()
        };
        assert_eq!(op, Op::Or);
        assert!(matches!(*lhs, Field::Binary { op: Op::And, .. }));
        let Field::Binary { op, lhs, .. } = filter("{ .a = 1 || .b = 2 && .c = 3 }") else {
            panic!()
        };
        assert_eq!(op, Op::And);
        assert!(matches!(*lhs, Field::Binary { op: Op::Or, .. }));
        // arithmetic: * binds tighter than +, ^ groups to the right
        let Field::Binary {
            op: Op::Eq, lhs, ..
        } = filter("{ 1 + 2 * 3 = 7 }")
        else {
            panic!()
        };
        let Field::Binary {
            op: Op::Add, rhs, ..
        } = *lhs
        else {
            panic!()
        };
        assert!(matches!(*rhs, Field::Binary { op: Op::Mul, .. }));
        let Field::Binary {
            op: Op::Eq, lhs, ..
        } = filter("{ 2 ^ 3 ^ 2 = 1 }")
        else {
            panic!()
        };
        let Field::Binary {
            op: Op::Pow, rhs, ..
        } = *lhs
        else {
            panic!()
        };
        assert!(matches!(*rhs, Field::Binary { op: Op::Pow, .. }));
    }

    #[test]
    fn nil_comparisons_become_existence_checks() {
        assert!(matches!(
            filter("{ .a = nil }"),
            Field::Unary {
                op: Op::NotExists,
                ..
            }
        ));
        assert!(matches!(
            filter("{ nil != .a }"),
            Field::Unary { op: Op::Exists, .. }
        ));
        assert_eq!(filter("{ nil = nil }"), Field::Static(Static::Bool(false)));
        assert!(parse("{ nil }").is_err() && parse("{ .a + nil = 1 }").is_err());
    }

    #[test]
    fn durations_and_numbers_lex_as_such() {
        assert_eq!(
            filter("{ duration > 1h30m }"),
            Field::Binary {
                op: Op::Gt,
                lhs: Box::new(Field::Attribute(Attribute {
                    scope: Scope::None,
                    parent: false,
                    intrinsic: Some(Intrinsic::Duration),
                    name: "duration".into()
                })),
                rhs: Box::new(Field::Static(Static::Duration(5_400_000_000_000))),
            }
        );
        let Field::Binary { rhs, .. } = filter("{ .a = 1.5s }") else {
            panic!()
        };
        assert_eq!(*rhs, Field::Static(Static::Duration(1_500_000_000)));
        let Field::Binary { rhs, .. } = filter("{ .a = 12 }") else {
            panic!()
        };
        assert_eq!(*rhs, Field::Static(Static::Int(12)));
        assert!(parse("{ .a = 100000000000000000000000 }").is_err()); // integer overflow
    }

    #[test]
    fn spanset_operators_and_pipelines_have_the_expected_shape() {
        let QueryKind::Pipeline(p) = parse("{ .a } >> { .b } | count() > 2 | by(.c) | select(.d)")
            .unwrap()
            .kind
        else {
            panic!()
        };
        assert_eq!(p.elements.len(), 4);
        assert!(matches!(
            &p.elements[0],
            Element::Spanset(Spansets::Op {
                op: SpansetOp::Descendant,
                ..
            })
        ));
        assert!(matches!(&p.elements[1], Element::ScalarFilter(_)));
        assert!(matches!(&p.elements[2], Element::Group(_)));
        assert!(matches!(&p.elements[3], Element::Select(a) if a.len() == 1));
    }

    #[test]
    fn metrics_queries_math_and_hints_parse_into_their_parts() {
        let q = parse("{ status = error } | quantile_over_time(duration, 0.5, 0.9) by (span.http.path) | topk(3) with(sample=0.1)").unwrap();
        let QueryKind::Metrics(m) = q.kind else {
            panic!()
        };
        assert!(matches!(&m.agg.func, MetricsFn::QuantileOverTime(_, qs) if qs == &[0.5, 0.9]));
        assert_eq!(m.agg.by.len(), 1);
        assert_eq!(m.second, vec![SecondStage::TopK(3)]);
        assert_eq!(
            q.hints,
            vec![Hint {
                name: "sample".into(),
                value: Static::Float(0.1)
            }]
        );

        let QueryKind::MetricsMath { expr, second } =
            parse("100 * ({} | rate()) / ({} | count_over_time()) > 5")
                .unwrap()
                .kind
        else {
            panic!()
        };
        assert!(matches!(expr, MetricsExpr::Binary { op: Op::Div, .. }));
        assert_eq!(
            second,
            vec![SecondStage::Filter {
                op: Op::Gt,
                value: Static::Int(5)
            }]
        );
        // constants fold: 1 / 2 is an integer division
        let QueryKind::Metrics(m) = parse("{} | rate() > 1 / 2").unwrap().kind else {
            panic!()
        };
        assert_eq!(
            m.second,
            vec![SecondStage::Filter {
                op: Op::Gt,
                value: Static::Int(0)
            }]
        );
    }

    #[test]
    fn errors_point_at_the_problem() {
        let e = parse("{ .a = }").unwrap_err();
        assert_eq!(e.position, 7);
        assert!(e.to_string().contains("unexpected"));
        assert!(parse("{ .a = \"open }").is_err());
        assert!(parse("").is_err());
    }
}
