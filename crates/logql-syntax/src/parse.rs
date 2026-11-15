//! LogQL parser: log queries (selector plus pipeline), range and vector aggregations, binary
//! operators with vector matching, `label_replace` and `vector()`.

use crate::ast::*;
use crate::lex::{err, lex, Tok};
use crate::Result;

/// Parses a LogQL expression: a log query or a metric query.
pub fn parse(input: &str) -> Result<Expr> {
    let mut p = Parser {
        toks: lex(input)?,
        i: 0,
    };
    let expr = p.expr(0)?;
    match p.peek() {
        Tok::Eof => Ok(expr),
        other => Err(err(p.pos(), format!("unexpected {other:?}"))),
    }
}

struct Parser {
    toks: Vec<(usize, Tok)>,
    i: usize,
}

/// A stage, or the `unwrap` that belongs to the enclosing range aggregation.
enum Parsed {
    Stage(Stage),
    Unwrap(Unwrap),
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.i].1
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.i + n).min(self.toks.len() - 1)].1
    }

    fn pos(&self) -> usize {
        self.toks[self.i].0
    }

    fn next(&mut self) -> Tok {
        let t = self.toks[self.i].1.clone();
        if self.i < self.toks.len() - 1 {
            self.i += 1;
        }
        t
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == tok {
            self.next();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: Tok) -> Result<()> {
        if self.eat(&tok) {
            Ok(())
        } else {
            Err(err(
                self.pos(),
                format!("expected {tok:?}, found {:?}", self.peek()),
            ))
        }
    }

    fn is_ident(&self, word: &str) -> bool {
        matches!(self.peek(), Tok::Ident(w) if w == word)
    }

    fn ident(&mut self) -> Result<String> {
        match self.next() {
            Tok::Ident(w) => Ok(w),
            other => Err(err(self.pos(), format!("expected a name, found {other:?}"))),
        }
    }

    fn string(&mut self) -> Result<String> {
        match self.next() {
            Tok::Str(s) => Ok(s),
            other => Err(err(
                self.pos(),
                format!("expected a string, found {other:?}"),
            )),
        }
    }

    fn number(&mut self) -> Result<f64> {
        let negative = self.eat(&Tok::Minus);
        match self.next() {
            Tok::Num(n) => Ok(if negative { -n } else { n }),
            other => Err(err(
                self.pos(),
                format!("expected a number, found {other:?}"),
            )),
        }
    }

    // ---- expressions ------------------------------------------------------------------

    fn binary_op(&self) -> Option<(BinOp, u8)> {
        Some(match self.peek() {
            Tok::Ident(w) if w == "or" => (BinOp::Or, 1),
            Tok::Ident(w) if w == "and" => (BinOp::And, 2),
            Tok::Ident(w) if w == "unless" => (BinOp::Unless, 2),
            Tok::EqEq => (BinOp::Eq, 3),
            Tok::Ne => (BinOp::Ne, 3),
            Tok::Gt => (BinOp::Gt, 3),
            Tok::Ge => (BinOp::Ge, 3),
            Tok::Lt => (BinOp::Lt, 3),
            Tok::Le => (BinOp::Le, 3),
            Tok::Plus => (BinOp::Add, 4),
            Tok::Minus => (BinOp::Sub, 4),
            Tok::Star => (BinOp::Mul, 5),
            Tok::Slash => (BinOp::Div, 5),
            Tok::Percent => (BinOp::Mod, 5),
            Tok::Caret => (BinOp::Pow, 6),
            _ => return None,
        })
    }

    fn expr(&mut self, min_prec: u8) -> Result<Expr> {
        let mut lhs = self.unary()?;
        while let Some((op, prec)) = self.binary_op() {
            if prec < min_prec {
                break;
            }
            self.next();
            let (bool_modifier, matching, group) = self.binary_modifiers()?;
            // `^` is right-associative; everything else binds left.
            let rhs = self.expr(if op == BinOp::Pow { prec } else { prec + 1 })?;
            lhs = Expr::Binary(Box::new(Binary {
                op,
                lhs,
                rhs,
                bool_modifier,
                matching,
                group,
            }));
        }
        Ok(lhs)
    }

    #[allow(clippy::type_complexity)]
    fn binary_modifiers(
        &mut self,
    ) -> Result<(
        bool,
        Option<(bool, Vec<String>)>,
        Option<(bool, Vec<String>)>,
    )> {
        let bool_modifier = if self.is_ident("bool") {
            self.next();
            true
        } else {
            false
        };
        let matching = if self.is_ident("on") || self.is_ident("ignoring") {
            let on = self.is_ident("on");
            self.next();
            Some((on, self.label_list()?))
        } else {
            None
        };
        let group = if self.is_ident("group_left") || self.is_ident("group_right") {
            let left = self.is_ident("group_left");
            self.next();
            let labels = if self.peek() == &Tok::LParen {
                self.label_list()?
            } else {
                Vec::new()
            };
            Some((left, labels))
        } else {
            None
        };
        Ok((bool_modifier, matching, group))
    }

    fn label_list(&mut self) -> Result<Vec<String>> {
        self.expect(Tok::LParen)?;
        let mut labels = Vec::new();
        while self.peek() != &Tok::RParen {
            labels.push(self.ident()?);
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        self.expect(Tok::RParen)?;
        Ok(labels)
    }

    fn unary(&mut self) -> Result<Expr> {
        if self.eat(&Tok::Minus) {
            return Ok(match self.unary()? {
                Expr::Number(n) => Expr::Number(-n),
                other => Expr::Binary(Box::new(Binary {
                    op: BinOp::Sub,
                    lhs: Expr::Number(0.0),
                    rhs: other,
                    bool_modifier: false,
                    matching: None,
                    group: None,
                })),
            });
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr> {
        let pos = self.pos();
        match self.peek().clone() {
            Tok::Num(n) => {
                self.next();
                Ok(Expr::Number(n))
            }
            Tok::LParen => {
                self.next();
                let inner = self.expr(0)?;
                self.expect(Tok::RParen)?;
                Ok(inner)
            }
            Tok::LBrace => Ok(Expr::Log(self.log_expr()?)),
            Tok::Ident(name) => {
                self.next();
                match name.as_str() {
                    "vector" => {
                        self.expect(Tok::LParen)?;
                        let n = self.number()?;
                        self.expect(Tok::RParen)?;
                        Ok(Expr::VectorLit(n))
                    }
                    "label_replace" => self.label_replace(),
                    other => {
                        if let Some(op) = RangeOp::parse(other) {
                            self.range_agg(op)
                        } else if let Some(op) = VecOp::parse(other) {
                            self.vec_agg(op)
                        } else {
                            Err(err(pos, format!("unknown function {other:?}")))
                        }
                    }
                }
            }
            other => Err(err(pos, format!("unexpected {other:?}"))),
        }
    }

    fn label_replace(&mut self) -> Result<Expr> {
        self.expect(Tok::LParen)?;
        let expr = self.expr(0)?;
        let mut strings = Vec::new();
        for _ in 0..4 {
            self.expect(Tok::Comma)?;
            strings.push(self.string()?);
        }
        self.expect(Tok::RParen)?;
        let [dst, replacement, src, regex]: [String; 4] = strings.try_into().expect("four strings");
        Ok(Expr::LabelReplace(Box::new(LabelReplace {
            expr,
            dst,
            replacement,
            src,
            regex,
        })))
    }

    fn grouping(&mut self) -> Result<Option<Grouping>> {
        if self.is_ident("by") || self.is_ident("without") {
            let by = self.is_ident("by");
            self.next();
            return Ok(Some(Grouping {
                by,
                labels: self.label_list()?,
            }));
        }
        Ok(None)
    }

    fn vec_agg(&mut self, op: VecOp) -> Result<Expr> {
        let mut grouping = self.grouping()?;
        self.expect(Tok::LParen)?;
        let param = if matches!(op, VecOp::Topk | VecOp::Bottomk) {
            let k = self.number()?;
            self.expect(Tok::Comma)?;
            Some(k)
        } else {
            None
        };
        let expr = self.expr(0)?;
        self.expect(Tok::RParen)?;
        if grouping.is_none() {
            grouping = self.grouping()?;
        }
        Ok(Expr::Vector(Box::new(VecAgg {
            op,
            param,
            grouping,
            expr,
        })))
    }

    fn range_agg(&mut self, op: RangeOp) -> Result<Expr> {
        self.expect(Tok::LParen)?;
        let param = if op == RangeOp::QuantileOverTime {
            let q = self.number()?;
            self.expect(Tok::Comma)?;
            Some(q)
        } else {
            None
        };
        // `({selector} | stages)[5m]` or `{selector} | stages [5m] | more stages`.
        let parenthesised = self.peek() == &Tok::LParen;
        if parenthesised {
            self.next();
        }
        let matchers = self.selector()?;
        let mut stages = Vec::new();
        let mut unwrap = None;
        self.stages(&mut stages, &mut unwrap)?;
        if parenthesised {
            self.expect(Tok::RParen)?;
        }
        self.expect(Tok::LBracket)?;
        let range_ms = self.duration_ms()?;
        self.expect(Tok::RBracket)?;
        let offset_ms = if self.is_ident("offset") {
            self.next();
            self.duration_ms()?
        } else {
            0
        };
        self.stages(&mut stages, &mut unwrap)?;
        self.expect(Tok::RParen)?;
        let grouping = self.grouping()?;
        if op.needs_unwrap() && unwrap.is_none() {
            return Err(err(
                self.pos(),
                "this range aggregation needs `| unwrap <label>`",
            ));
        }
        Ok(Expr::Range(Box::new(RangeAgg {
            op,
            param,
            log: LogExpr { matchers, stages },
            range_ms,
            offset_ms,
            unwrap,
            grouping,
        })))
    }

    fn duration_ms(&mut self) -> Result<i64> {
        match self.next() {
            Tok::Duration(secs) => Ok((secs * 1000.0).round() as i64),
            other => Err(err(
                self.pos(),
                format!("expected a duration, found {other:?}"),
            )),
        }
    }

    // ---- log queries ------------------------------------------------------------------

    fn log_expr(&mut self) -> Result<LogExpr> {
        let matchers = self.selector()?;
        let mut stages = Vec::new();
        let mut unwrap = None;
        self.stages(&mut stages, &mut unwrap)?;
        if unwrap.is_some() {
            return Err(err(
                self.pos(),
                "`unwrap` is only valid inside a range aggregation",
            ));
        }
        Ok(LogExpr { matchers, stages })
    }

    fn selector(&mut self) -> Result<Vec<Matcher>> {
        self.expect(Tok::LBrace)?;
        let mut matchers = Vec::new();
        while self.peek() != &Tok::RBrace {
            let name = self.ident()?;
            let op = match self.next() {
                Tok::Eq => MatchOp::Eq,
                Tok::Ne => MatchOp::Ne,
                Tok::Re => MatchOp::Re,
                Tok::NotRe => MatchOp::NotRe,
                other => {
                    return Err(err(
                        self.pos(),
                        format!("expected a matcher operator, found {other:?}"),
                    ))
                }
            };
            matchers.push(Matcher {
                name,
                op,
                value: self.string()?,
            });
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        self.expect(Tok::RBrace)?;
        if matchers.is_empty() {
            return Err(err(
                self.pos(),
                "a stream selector needs at least one matcher",
            ));
        }
        Ok(matchers)
    }

    /// Parses `|= ...` and `| stage` while they follow; stops at anything else.
    fn stages(&mut self, stages: &mut Vec<Stage>, unwrap: &mut Option<Unwrap>) -> Result<()> {
        loop {
            let line_op = match self.peek() {
                Tok::PipeEq => Some(LineOp::Contains),
                Tok::Ne => Some(LineOp::NotContains),
                Tok::PipeRe => Some(LineOp::Re),
                Tok::NotRe => Some(LineOp::NotRe),
                Tok::PipePat => Some(LineOp::Pattern),
                Tok::NotPat => Some(LineOp::NotPattern),
                _ => None,
            };
            if let Some(op) = line_op {
                self.next();
                stages.push(self.line_filter(op)?);
                continue;
            }
            if self.peek() != &Tok::Pipe {
                return Ok(());
            }
            self.next();
            match self.stage()? {
                Parsed::Stage(stage) => stages.push(stage),
                Parsed::Unwrap(u) => *unwrap = Some(u),
            }
        }
    }

    fn line_filter(&mut self, op: LineOp) -> Result<Stage> {
        let mut values = vec![self.filter_value()?];
        while self.is_ident("or") {
            self.next();
            values.push(self.filter_value()?);
        }
        Ok(Stage::LineFilter { op, values })
    }

    fn filter_value(&mut self) -> Result<FilterValue> {
        if self.is_ident("ip") && self.peek_at(1) == &Tok::LParen {
            self.next();
            self.next();
            let ip = self.string()?;
            self.expect(Tok::RParen)?;
            return Ok(FilterValue::Ip(ip));
        }
        Ok(FilterValue::Str(self.string()?))
    }

    fn stage(&mut self) -> Result<Parsed> {
        let Tok::Ident(word) = self.peek().clone() else {
            // `| (a > 1 or b = "x")` starts a label filter with a parenthesis.
            return Ok(Parsed::Stage(Stage::LabelFilter(self.label_filter(0)?)));
        };
        let keyword = matches!(
            word.as_str(),
            "json"
                | "logfmt"
                | "regexp"
                | "pattern"
                | "unpack"
                | "line_format"
                | "label_format"
                | "decolorize"
                | "drop"
                | "keep"
                | "unwrap"
        );
        // A label named like a keyword is still a label filter: `| json = "x"`.
        let is_filter = matches!(
            self.peek_at(1),
            Tok::Eq
                | Tok::EqEq
                | Tok::Ne
                | Tok::Re
                | Tok::NotRe
                | Tok::Gt
                | Tok::Ge
                | Tok::Lt
                | Tok::Le
        ) && !matches!(word.as_str(), "drop" | "keep");
        if !keyword || is_filter {
            return Ok(Parsed::Stage(Stage::LabelFilter(self.label_filter(0)?)));
        }
        self.next();
        Ok(Parsed::Stage(match word.as_str() {
            "json" => Stage::Json(self.extractions()?),
            "logfmt" => {
                while self.peek() == &Tok::Minus {
                    // `--strict`, `--keep-empty`: accepted, not acted on.
                    self.next();
                    self.expect(Tok::Minus)?;
                    self.ident()?;
                    if self.peek() == &Tok::Minus {
                        self.next();
                        self.ident()?;
                    }
                }
                Stage::Logfmt(self.extractions()?)
            }
            "regexp" => Stage::Regexp(self.string()?),
            "pattern" => Stage::Pattern(self.string()?),
            "unpack" => Stage::Unpack,
            "line_format" => Stage::LineFormat(self.string()?),
            "decolorize" => Stage::Decolorize,
            "label_format" => Stage::LabelFormat(self.label_format()?),
            "drop" => Stage::Drop(self.drop_items()?),
            "keep" => Stage::Keep(self.drop_items()?),
            _ => return Ok(Parsed::Unwrap(self.unwrap_stage()?)),
        }))
    }

    fn extractions(&mut self) -> Result<Vec<Extraction>> {
        let mut out = Vec::new();
        while let Tok::Ident(_) = self.peek() {
            let label = self.ident()?;
            let expr = if self.eat(&Tok::Eq) {
                Some(self.string()?)
            } else {
                None
            };
            out.push(Extraction { label, expr });
            if !self.eat(&Tok::Comma) {
                break;
            }
        }
        Ok(out)
    }

    fn label_format(&mut self) -> Result<Vec<LabelFormatItem>> {
        let mut out = Vec::new();
        loop {
            let dst = self.ident()?;
            self.expect(Tok::Eq)?;
            out.push(match self.next() {
                Tok::Ident(src) => LabelFormatItem::Rename { dst, src },
                Tok::Str(template) => LabelFormatItem::Template { dst, template },
                other => {
                    return Err(err(
                        self.pos(),
                        format!("expected a label or template, found {other:?}"),
                    ))
                }
            });
            if !self.eat(&Tok::Comma) {
                return Ok(out);
            }
        }
    }

    fn drop_items(&mut self) -> Result<Vec<DropItem>> {
        let mut out = Vec::new();
        loop {
            let label = self.ident()?;
            let matcher = match self.peek() {
                Tok::Eq | Tok::Ne | Tok::Re | Tok::NotRe => {
                    let op = match self.next() {
                        Tok::Eq => MatchOp::Eq,
                        Tok::Ne => MatchOp::Ne,
                        Tok::Re => MatchOp::Re,
                        _ => MatchOp::NotRe,
                    };
                    Some((op, self.string()?))
                }
                _ => None,
            };
            out.push(DropItem { label, matcher });
            if !self.eat(&Tok::Comma) {
                return Ok(out);
            }
        }
    }

    fn unwrap_stage(&mut self) -> Result<Unwrap> {
        let first = self.ident()?;
        let conv = match first.as_str() {
            "bytes" => Some(Conv::Bytes),
            "duration" => Some(Conv::Duration),
            "duration_seconds" => Some(Conv::DurationSeconds),
            _ => None,
        };
        if let (Some(conv), Tok::LParen) = (conv, self.peek()) {
            self.next();
            let label = self.ident()?;
            self.expect(Tok::RParen)?;
            return Ok(Unwrap { conv, label });
        }
        Ok(Unwrap {
            conv: Conv::None,
            label: first,
        })
    }

    // ---- label filters ----------------------------------------------------------------

    /// `a > 1 and b = "x", c != "y" or d`: `or` binds loosest; `and`, `,` and plain adjacency
    /// all mean AND.
    fn label_filter(&mut self, depth: u8) -> Result<LabelFilter> {
        let mut left = self.label_and(depth)?;
        while self.is_ident("or") {
            self.next();
            let right = self.label_and(depth)?;
            left = LabelFilter::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn label_and(&mut self, depth: u8) -> Result<LabelFilter> {
        let mut left = self.label_atom(depth)?;
        loop {
            let joined = if self.is_ident("and") || self.peek() == &Tok::Comma {
                self.next();
                true
            } else {
                // Adjacent filters: the next token starts another comparison.
                matches!(self.peek(), Tok::LParen)
                    || (matches!(self.peek(), Tok::Ident(w) if w != "or")
                        && matches!(
                            self.peek_at(1),
                            Tok::Eq
                                | Tok::EqEq
                                | Tok::Ne
                                | Tok::Re
                                | Tok::NotRe
                                | Tok::Gt
                                | Tok::Ge
                                | Tok::Lt
                                | Tok::Le
                        ))
            };
            if !joined {
                return Ok(left);
            }
            let right = self.label_atom(depth)?;
            left = LabelFilter::And(Box::new(left), Box::new(right));
        }
    }

    fn label_atom(&mut self, depth: u8) -> Result<LabelFilter> {
        if self.eat(&Tok::LParen) {
            let inner = self.label_filter(depth + 1)?;
            self.expect(Tok::RParen)?;
            return Ok(inner);
        }
        let name = self.ident()?;
        let op = match self.next() {
            Tok::Eq | Tok::EqEq => CmpOp::Eq,
            Tok::Ne => CmpOp::Ne,
            Tok::Re => CmpOp::Re,
            Tok::NotRe => CmpOp::NotRe,
            Tok::Gt => CmpOp::Gt,
            Tok::Ge => CmpOp::Ge,
            Tok::Lt => CmpOp::Lt,
            Tok::Le => CmpOp::Le,
            other => {
                return Err(err(
                    self.pos(),
                    format!("expected a comparison, found {other:?}"),
                ))
            }
        };
        let value = match self.peek().clone() {
            Tok::Str(s) => {
                self.next();
                LabelValue::Str(s)
            }
            Tok::Num(_) | Tok::Minus => LabelValue::Number(self.number()?),
            Tok::Duration(d) => {
                self.next();
                LabelValue::Duration(d)
            }
            Tok::Bytes(b) => {
                self.next();
                LabelValue::Bytes(b)
            }
            Tok::Ident(w) if w == "ip" && self.peek_at(1) == &Tok::LParen => {
                self.next();
                self.next();
                let ip = self.string()?;
                self.expect(Tok::RParen)?;
                LabelValue::Ip(ip)
            }
            other => {
                return Err(err(
                    self.pos(),
                    format!("expected a value, found {other:?}"),
                ))
            }
        };
        Ok(LabelFilter::Cmp { name, op, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(q: &str) -> Expr {
        parse(q).unwrap_or_else(|e| panic!("{q}: {e:?}"))
    }

    #[test]
    fn log_queries_with_every_stage_parse() {
        let Expr::Log(log) = ok(
            r#"{job="a", env=~"p.*"} |= "x" or "y" != ip("10.0.0.0/8") |~ `a.b` !> "<_>z"
               | json status, method="req.method" | logfmt --strict | regexp `(?P<m>\w+)` | pattern "<ip> -"
               | unpack | decolorize | line_format "{{.m}}" | label_format a=b, c="{{.d}}" | drop x, y="z" | keep k
               | status >= 500 and method = "GET" or took > 1s, size < 10KB # trailing comment"#,
        ) else {
            panic!("not a log expression")
        };
        assert_eq!(log.matchers.len(), 2);
        assert_eq!(log.stages.len(), 15);
        assert!(matches!(
            &log.stages[0],
            Stage::LineFilter { op: LineOp::Contains, values } if values.len() == 2
        ));
        let Stage::LabelFilter(LabelFilter::Or(_, right)) = &log.stages[14] else {
            panic!("expected an or filter, got {:?}", log.stages[14])
        };
        // `took > 1s, size < 10KB` is one AND group on the right of `or`.
        assert!(matches!(**right, LabelFilter::And(..)));
    }

    #[test]
    fn label_filter_precedence_matches_loki() {
        // (a and b) or c
        let Expr::Log(log) = ok(r#"{j="x"} | a > 1 and b > 2 or c > 3"#) else {
            panic!()
        };
        let Stage::LabelFilter(LabelFilter::Or(left, _)) = &log.stages[0] else {
            panic!()
        };
        assert!(matches!(**left, LabelFilter::And(..)));
        // a and (b or c)
        let Expr::Log(log) = ok(r#"{j="x"} | a > 1 and (b > 2 or c > 3)"#) else {
            panic!()
        };
        let Stage::LabelFilter(LabelFilter::And(_, right)) = &log.stages[0] else {
            panic!()
        };
        assert!(matches!(**right, LabelFilter::Or(..)));
        // whitespace is AND too
        let Expr::Log(log) = ok(r#"{j="x"} | a > 1 b > 2"#) else {
            panic!()
        };
        assert!(matches!(
            &log.stages[0],
            Stage::LabelFilter(LabelFilter::And(..))
        ));
    }

    #[test]
    fn metric_queries_parse() {
        let Expr::Vector(v) = ok(r#"sum by (level) (rate({app="foo"} |= "x" [1m] offset 5m))"#)
        else {
            panic!()
        };
        let Expr::Range(r) = &v.expr else { panic!() };
        assert_eq!((r.range_ms, r.offset_ms), (60_000, 300_000));
        assert_eq!(v.grouping.as_ref().unwrap().labels, ["level"]);

        let Expr::Range(r) =
            ok(r#"quantile_over_time(0.99, {a="b"} | json | unwrap duration(took) [5m]) by (x)"#)
        else {
            panic!()
        };
        assert_eq!(r.param, Some(0.99));
        assert_eq!(
            r.unwrap,
            Some(Unwrap {
                conv: Conv::Duration,
                label: "took".into()
            })
        );
        assert!(r.grouping.is_some());

        let Expr::Range(r) = ok(r#"avg_over_time(({a="b"} | unwrap bytes(size))[1h30m])"#) else {
            panic!()
        };
        assert_eq!(r.range_ms, 5_400_000);

        assert!(matches!(
            ok(r#"topk(3, sum by (a) (rate({j=~".+"}[1m])))"#),
            Expr::Vector(_)
        ));
        assert!(matches!(
            ok(r#"label_replace(rate({a="b"}[1m]), "x", "$1", "a", "(.*)")"#),
            Expr::LabelReplace(_)
        ));
        assert!(matches!(
            ok("sort_desc(sum by (a) (rate({a=\"b\"}[1m])))"),
            Expr::Vector(_)
        ));
    }

    #[test]
    fn binary_expressions_follow_precedence_and_modifiers() {
        let Expr::Binary(b) = ok("1 + 2 * 3") else {
            panic!()
        };
        assert_eq!(b.op, BinOp::Add);
        let Expr::Binary(b) = ok("2 ^ 3 ^ 2") else {
            panic!()
        };
        assert!(matches!(b.rhs, Expr::Binary(_))); // right associative
        let Expr::Binary(b) = ok(r#"sum(rate({a="b"}[1m])) > bool on(x) group_left(y) 3"#) else {
            panic!()
        };
        assert!(b.bool_modifier);
        assert_eq!(b.matching, Some((true, vec!["x".to_string()])));
        assert_eq!(b.group, Some((true, vec!["y".to_string()])));
        assert_eq!(
            ok("vector(1) + vector(2) / vector(3)"),
            ok("vector(1) + (vector(2) / vector(3))")
        );
        assert_eq!(ok("-3"), Expr::Number(-3.0));
    }

    #[test]
    fn errors_are_reported_not_panicked() {
        for q in [
            r#"{a="b""#,
            "{}",
            r#"{a="b"} |"#,
            "rate({a=\"b\"})",
            r#"count_over_time({a="b"}[5x])"#,
            "nosuchfn(1)",
            r#"sum_over_time({a="b"}[1m])"#,
        ] {
            assert!(parse(q).is_err(), "{q} should not parse");
        }
    }
}
