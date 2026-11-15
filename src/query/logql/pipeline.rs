//! Runs the stages of a log pipeline over one line and its labels.

use super::extract::{json_extract, logfmt_extract, unpack, PatternMatcher};
use super::template::{Ctx, Template};
use super::values::{parse_bytes, parse_duration_secs, IpMatcher};
use crate::query::prom_labels::Labels;
use crate::validation::{ApiError, ApiResult};
use logql_syntax::ast::*;
use regex::Regex;
use std::sync::OnceLock;

/// The terminal colour and cursor escape sequences `decolorize` removes, compiled once.
fn ansi_escapes() -> &'static Regex {
    static ANSI: OnceLock<Regex> = OnceLock::new();
    ANSI.get_or_init(|| Regex::new("\x1b\\[[0-9;]*[A-Za-z]").expect("static regex"))
}

const ERROR: &str = "__error__";
const ERROR_DETAILS: &str = "__error_details__";

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, "bad_data", msg)
}

/// A regex that must match the whole value, as for stream selectors and label filters.
pub fn anchored(pattern: &str) -> ApiResult<Regex> {
    Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| bad(format!("invalid regex {pattern:?}: {e}")))
}

/// A selector or `drop`/`keep` test on one label value.
#[derive(Clone, Debug)]
pub struct ValueTest {
    op: MatchOp,
    text: String,
    re: Option<Regex>,
}

impl ValueTest {
    pub fn new(op: MatchOp, text: &str) -> ApiResult<Self> {
        let re = matches!(op, MatchOp::Re | MatchOp::NotRe)
            .then(|| anchored(text))
            .transpose()?;
        Ok(Self {
            op,
            text: text.to_string(),
            re,
        })
    }

    pub fn ok(&self, value: &str) -> bool {
        match self.op {
            MatchOp::Eq => value == self.text,
            MatchOp::Ne => value != self.text,
            MatchOp::Re => self.re.as_ref().is_some_and(|r| r.is_match(value)),
            MatchOp::NotRe => self.re.as_ref().is_some_and(|r| !r.is_match(value)),
        }
    }
}

enum Needle {
    Text(String),
    Re(Regex),
    Pattern(PatternMatcher),
    Ip(IpMatcher),
}

impl Needle {
    fn matches(&self, line: &str) -> bool {
        match self {
            Needle::Text(t) => line.contains(t.as_str()),
            Needle::Re(r) => r.is_match(line),
            Needle::Pattern(p) => p.captures(line).is_some(),
            Needle::Ip(ip) => ip.matches_text(line),
        }
    }
}

enum Test {
    Str(ValueTest),
    Num(CmpOp, f64),
    Duration(CmpOp, f64),
    Bytes(CmpOp, f64),
    Ip(bool, IpMatcher),
}

enum Filter {
    Cmp { name: String, test: Test },
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
}

enum LabelFmt {
    Rename(String),
    Template(Template),
}

enum Compiled {
    Line { needles: Vec<Needle>, negate: bool },
    Json(Vec<Extraction>),
    Logfmt(Vec<Extraction>),
    Regexp(Regex),
    Pattern(PatternMatcher),
    Unpack,
    LineFormat(Template),
    Decolorize,
    LabelFormat(Vec<(String, LabelFmt)>),
    Drop(Vec<(String, Option<ValueTest>)>),
    Keep(Vec<(String, Option<ValueTest>)>),
    Filter(Filter),
}

pub struct Pipeline {
    stages: Vec<Compiled>,
}

fn compare(op: CmpOp, left: f64, right: f64) -> bool {
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Ne => left != right,
        CmpOp::Gt => left > right,
        CmpOp::Ge => left >= right,
        CmpOp::Lt => left < right,
        CmpOp::Le => left <= right,
        CmpOp::Re | CmpOp::NotRe => false,
    }
}

fn compile_filter(f: &LabelFilter) -> ApiResult<Filter> {
    Ok(match f {
        LabelFilter::And(a, b) => {
            Filter::And(Box::new(compile_filter(a)?), Box::new(compile_filter(b)?))
        }
        LabelFilter::Or(a, b) => {
            Filter::Or(Box::new(compile_filter(a)?), Box::new(compile_filter(b)?))
        }
        LabelFilter::Cmp { name, op, value } => {
            let test = match (value, op) {
                (LabelValue::Str(s), CmpOp::Eq) => Test::Str(ValueTest::new(MatchOp::Eq, s)?),
                (LabelValue::Str(s), CmpOp::Ne) => Test::Str(ValueTest::new(MatchOp::Ne, s)?),
                (LabelValue::Str(s), CmpOp::Re) => Test::Str(ValueTest::new(MatchOp::Re, s)?),
                (LabelValue::Str(s), CmpOp::NotRe) => Test::Str(ValueTest::new(MatchOp::NotRe, s)?),
                (LabelValue::Str(_), _) => {
                    return Err(bad("a string label filter takes =, !=, =~ or !~"))
                }
                (LabelValue::Number(n), op) => Test::Num(*op, *n),
                (LabelValue::Duration(d), op) => Test::Duration(*op, *d),
                (LabelValue::Bytes(b), op) => Test::Bytes(*op, *b),
                (LabelValue::Ip(spec), op) => {
                    let m = IpMatcher::parse(spec)
                        .ok_or_else(|| bad(format!("invalid ip({spec:?})")))?;
                    match op {
                        CmpOp::Eq => Test::Ip(true, m),
                        CmpOp::Ne => Test::Ip(false, m),
                        _ => return Err(bad("an ip() label filter takes = or !=")),
                    }
                }
            };
            Filter::Cmp {
                name: name.clone(),
                test,
            }
        }
    })
}

fn compile_needles(op: LineOp, values: &[FilterValue]) -> ApiResult<Vec<Needle>> {
    values
        .iter()
        .map(|v| {
            Ok(match (op, v) {
                (_, FilterValue::Ip(spec)) => Needle::Ip(
                    IpMatcher::parse(spec).ok_or_else(|| bad(format!("invalid ip({spec:?})")))?,
                ),
                (LineOp::Contains | LineOp::NotContains, FilterValue::Str(s)) => {
                    Needle::Text(s.clone())
                }
                (LineOp::Re | LineOp::NotRe, FilterValue::Str(s)) => {
                    Needle::Re(Regex::new(s).map_err(|e| bad(format!("invalid regex {s:?}: {e}")))?)
                }
                (LineOp::Pattern | LineOp::NotPattern, FilterValue::Str(s)) => Needle::Pattern(
                    PatternMatcher::compile(s)
                        .map_err(|e| bad(format!("invalid pattern {s:?}: {e}")))?,
                ),
            })
        })
        .collect()
}

impl Pipeline {
    pub fn compile(stages: &[Stage]) -> ApiResult<Self> {
        let mut out = Vec::new();
        for stage in stages {
            out.push(match stage {
                Stage::LineFilter { op, values } => Compiled::Line {
                    needles: compile_needles(*op, values)?,
                    negate: matches!(op, LineOp::NotContains | LineOp::NotRe | LineOp::NotPattern),
                },
                Stage::Json(e) => Compiled::Json(e.clone()),
                Stage::Logfmt(e) => Compiled::Logfmt(e.clone()),
                Stage::Regexp(p) => {
                    let re = Regex::new(p).map_err(|e| bad(format!("invalid regex {p:?}: {e}")))?;
                    if !re.capture_names().flatten().any(|_| true) {
                        return Err(bad(
                            "a regexp stage needs at least one named group (?P<name>...)",
                        ));
                    }
                    Compiled::Regexp(re)
                }
                Stage::Pattern(p) => Compiled::Pattern(
                    PatternMatcher::compile(p)
                        .map_err(|e| bad(format!("invalid pattern {p:?}: {e}")))?,
                ),
                Stage::Unpack => Compiled::Unpack,
                Stage::LineFormat(t) => Compiled::LineFormat(
                    Template::parse(t)
                        .map_err(|e| bad(format!("invalid line_format template: {e}")))?,
                ),
                Stage::LabelFormat(items) => Compiled::LabelFormat(
                    items
                        .iter()
                        .map(|item| match item {
                            LabelFormatItem::Rename { dst, src } => {
                                Ok((dst.clone(), LabelFmt::Rename(src.clone())))
                            }
                            LabelFormatItem::Template { dst, template } => Ok((
                                dst.clone(),
                                LabelFmt::Template(Template::parse(template).map_err(|e| {
                                    bad(format!("invalid label_format template: {e}"))
                                })?),
                            )),
                        })
                        .collect::<ApiResult<Vec<_>>>()?,
                ),
                Stage::Decolorize => Compiled::Decolorize,
                Stage::Drop(items) | Stage::Keep(items) => {
                    let items = items
                        .iter()
                        .map(|i| {
                            let test = i
                                .matcher
                                .as_ref()
                                .map(|(op, v)| ValueTest::new(*op, v))
                                .transpose()?;
                            Ok((i.label.clone(), test))
                        })
                        .collect::<ApiResult<Vec<_>>>()?;
                    if matches!(stage, Stage::Drop(_)) {
                        Compiled::Drop(items)
                    } else {
                        Compiled::Keep(items)
                    }
                }
                Stage::LabelFilter(f) => Compiled::Filter(compile_filter(f)?),
            });
        }
        Ok(Self { stages: out })
    }

    /// The line and labels after the pipeline, or None when a filter drops the line.
    pub fn process(&self, ts_ns: i64, line: &str, base: &Labels) -> Option<(String, Labels)> {
        let mut line = line.to_string();
        let mut labels = base.clone();
        for stage in &self.stages {
            match stage {
                Compiled::Line { needles, negate } => {
                    if needles.iter().any(|n| n.matches(&line)) == *negate {
                        return None;
                    }
                }
                Compiled::Json(wanted) if !labels.contains_key(ERROR) => {
                    match json_extract(&line, wanted) {
                        Ok(pairs) => add_all(&mut labels, pairs),
                        Err(detail) => set_error(&mut labels, "JSONParserErr", &detail),
                    }
                }
                Compiled::Logfmt(wanted) if !labels.contains_key(ERROR) => {
                    match logfmt_extract(&line, wanted) {
                        Ok(pairs) => add_all(&mut labels, pairs),
                        Err(detail) => set_error(&mut labels, "LogfmtParserErr", &detail),
                    }
                }
                Compiled::Regexp(re) if !labels.contains_key(ERROR) => {
                    if let Some(caps) = re.captures(&line) {
                        let pairs = re
                            .capture_names()
                            .flatten()
                            .filter_map(|n| {
                                Some((n.to_string(), caps.name(n)?.as_str().to_string()))
                            })
                            .collect();
                        add_all(&mut labels, pairs);
                    }
                }
                Compiled::Pattern(p) if !labels.contains_key(ERROR) => {
                    if let Some(pairs) = p.captures(&line) {
                        add_all(&mut labels, pairs);
                    }
                }
                Compiled::Unpack if !labels.contains_key(ERROR) => match unpack(&line) {
                    Ok((pairs, entry)) => {
                        add_all(&mut labels, pairs);
                        if let Some(entry) = entry {
                            line = entry;
                        }
                    }
                    Err(detail) => set_error(&mut labels, "JSONParserErr", &detail),
                },
                Compiled::Json(_)
                | Compiled::Logfmt(_)
                | Compiled::Regexp(_)
                | Compiled::Pattern(_)
                | Compiled::Unpack => {}
                Compiled::LineFormat(template) => {
                    match template.render(&Ctx {
                        labels: &labels,
                        line: &line,
                        ts_ns,
                    }) {
                        Ok(rendered) => line = rendered,
                        Err(detail) => set_error(&mut labels, "TemplateFormatErr", &detail),
                    }
                }
                Compiled::Decolorize => {
                    line = ansi_escapes().replace_all(&line, "").into_owned();
                }
                Compiled::LabelFormat(items) => {
                    for (dst, fmt) in items {
                        match fmt {
                            LabelFmt::Rename(src) => {
                                if let Some(value) = labels.remove(src) {
                                    labels.insert(dst.clone(), value);
                                }
                            }
                            LabelFmt::Template(template) => {
                                match template.render(&Ctx {
                                    labels: &labels,
                                    line: &line,
                                    ts_ns,
                                }) {
                                    Ok(rendered) => {
                                        labels.insert(dst.clone(), rendered);
                                    }
                                    Err(detail) => {
                                        set_error(&mut labels, "TemplateFormatErr", &detail)
                                    }
                                }
                            }
                        }
                    }
                }
                Compiled::Drop(items) => {
                    for (name, test) in items {
                        if labels
                            .get(name)
                            .is_some_and(|v| test.as_ref().is_none_or(|t| t.ok(v)))
                        {
                            labels.remove(name);
                        }
                    }
                }
                Compiled::Keep(items) => labels.retain(|k, v| {
                    k == ERROR
                        || k == ERROR_DETAILS
                        || items
                            .iter()
                            .any(|(name, test)| name == k && test.as_ref().is_none_or(|t| t.ok(v)))
                }),
                Compiled::Filter(f) => {
                    if !eval(f, &mut labels) {
                        return None;
                    }
                }
            }
        }
        Some((line, labels))
    }
}

fn set_error(labels: &mut Labels, kind: &str, detail: &str) {
    labels
        .entry(ERROR.to_string())
        .or_insert_with(|| kind.to_string());
    labels
        .entry(ERROR_DETAILS.to_string())
        .or_insert_with(|| detail.to_string());
}

/// A parsed label that clashes with an existing one is kept as `<name>_extracted`.
fn add_all(labels: &mut Labels, pairs: Vec<(String, String)>) {
    for (name, value) in pairs {
        let name = if labels.contains_key(&name) {
            format!("{name}_extracted")
        } else {
            name
        };
        labels.insert(name, value);
    }
}

fn eval(filter: &Filter, labels: &mut Labels) -> bool {
    match filter {
        Filter::And(a, b) => eval(a, labels) && eval(b, labels),
        Filter::Or(a, b) => eval(a, labels) || eval(b, labels),
        Filter::Cmp { name, test } => {
            // After a parse error the other filters let the line through so that
            // `| __error__ = ""` (or its absence) decides what happens to it.
            if name != ERROR && name != ERROR_DETAILS && labels.contains_key(ERROR) {
                return true;
            }
            let value = labels.get(name).cloned();
            match test {
                Test::Str(t) => t.ok(value.as_deref().unwrap_or("")),
                Test::Ip(want, m) => value.as_deref().is_some_and(|v| m.matches_text(v)) == *want,
                Test::Num(op, n) => {
                    numeric(labels, name, value, |v| v.parse::<f64>().ok(), *op, *n)
                }
                Test::Duration(op, d) => numeric(labels, name, value, parse_duration_secs, *op, *d),
                Test::Bytes(op, b) => numeric(labels, name, value, parse_bytes, *op, *b),
            }
        }
    }
}

fn numeric(
    labels: &mut Labels,
    name: &str,
    value: Option<String>,
    parse: impl Fn(&str) -> Option<f64>,
    op: CmpOp,
    right: f64,
) -> bool {
    let Some(value) = value else { return false };
    match parse(&value) {
        Some(left) => compare(op, left, right),
        None => {
            set_error(
                labels,
                "LabelFilterErr",
                &format!(
                    "label {name} has the value {value:?}, which is not a number of the right kind"
                ),
            );
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logql_syntax::ast::Expr;

    fn run(query: &str, line: &str, labels: &[(&str, &str)]) -> Option<(String, Labels)> {
        let Expr::Log(log) = logql_syntax::parse(query).unwrap() else {
            panic!("not a log query")
        };
        let base: Labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Pipeline::compile(&log.stages)
            .unwrap()
            .process(1_700_000_000_000_000_000, line, &base)
    }

    const JSON: &str =
        r#"{"method":"GET","status":503,"took":"1.5s","size":"2KB","ip":"10.1.2.3"}"#;

    #[test]
    fn line_filters_combine_with_or_and_negation() {
        let q = |filters: &str| run(&format!("{{a=\"b\"}} {filters}"), "GET /x 200", &[]).is_some();
        assert!(q(r#"|= "GET" |= "200""#));
        assert!(!q(r#"|= "POST" or "PUT""#));
        assert!(q(r#"|= "POST" or "GET""#));
        assert!(q(r#"!= "POST""#));
        assert!(!q(r#"!~ "G.T""#));
        assert!(q(r#"|~ "(?i)get""#));
        assert!(q(r#"|> "GET <_> 200""#));
        assert!(!q(r#"!> "GET <_> 200""#));
    }

    #[test]
    fn json_stage_extracts_and_typed_filters_compare() {
        let ok =
            |stages: &str| run(&format!("{{a=\"b\"}} {stages}"), JSON, &[("job", "j")]).is_some();
        assert!(ok("| json | status >= 500"));
        assert!(!ok("| json | status < 500"));
        assert!(ok(r#"| json | method = "GET" and status > 400"#));
        assert!(ok("| json | took >= 1s and size >= 1KB"));
        assert!(!ok("| json | took > 1m"));
        assert!(ok(r#"| json | ip = ip("10.0.0.0/8")"#));
        assert!(!ok(r#"| json | ip != ip("10.0.0.0/8")"#));
        assert!(ok(
            r#"| json | (status > 1000 and status < 0) or method != "HEAD""#
        ));
        assert!(!ok(
            r#"| json | status > 1000 and (status < 0 or method != "HEAD")"#
        ));
    }

    #[test]
    fn parsed_labels_follow_the_stream_labels_and_collisions_are_extracted() {
        let (_, labels) = run(
            r#"{a="b"} | json method, code="status""#,
            JSON,
            &[("method", "stream")],
        )
        .unwrap();
        assert_eq!(labels["method"], "stream");
        assert_eq!(labels["method_extracted"], "GET");
        assert_eq!(labels["code"], "503");
        assert!(!labels.contains_key("took"));
    }

    #[test]
    fn parse_errors_set_error_labels_and_let_filters_through() {
        let (_, labels) = run(r#"{a="b"} | json | status > 1"#, "not json", &[]).unwrap();
        assert_eq!(labels["__error__"], "JSONParserErr");
        assert!(run(r#"{a="b"} | json | __error__ = """#, "not json", &[]).is_none());
        let (_, labels) = run(r#"{a="b"} | json | method > 1"#, JSON, &[]).unwrap();
        assert_eq!(labels["__error__"], "LabelFilterErr");
    }

    #[test]
    fn label_stages_rename_drop_and_keep() {
        let q = r#"{a="b"} | json | label_format code=status | drop method, ip | keep code, took"#;
        let (_, labels) = run(q, JSON, &[("job", "j")]).unwrap();
        assert_eq!(
            labels.keys().map(String::as_str).collect::<Vec<_>>(),
            ["code", "took"]
        );
        let (line, _) = run(r#"{a="b"} | line_format "fixed""#, "x", &[]).unwrap();
        assert_eq!(line, "fixed");
        let (line, _) = run("{a=\"b\"} | decolorize", "\x1b[31mred\x1b[0m", &[]).unwrap();
        assert_eq!(line, "red");
    }

    #[test]
    fn regexp_pattern_and_unpack_extract() {
        let (_, l) = run(
            r#"{a="b"} | regexp `"method":"(?P<m>\w+)"` | m = "GET""#,
            JSON,
            &[],
        )
        .unwrap();
        assert_eq!(l["m"], "GET");
        let (_, l) = run(r#"{a="b"} | pattern `<_>"method":"<m>"<_>`"#, JSON, &[]).unwrap();
        assert_eq!(l["m"], "GET");
        let (line, l) = run(r#"{a="b"} | unpack"#, r#"{"_entry":"orig","k":"v"}"#, &[]).unwrap();
        assert_eq!((line.as_str(), l["k"].as_str()), ("orig", "v"));
    }

    #[test]
    fn templates_format_lines_and_labels() {
        let (line, _) = run(
            r#"{a="b"} | json | line_format "{{ .method | lower }} {{ .status }}""#,
            JSON,
            &[],
        )
        .unwrap();
        assert_eq!(line, "get 503");
        let (_, labels) = run(r#"{a="b"} | json | label_format kind="{{ if ge .status \"500\" }}server{{ else }}ok{{ end }}", code=status"#, JSON, &[]).unwrap();
        assert_eq!(labels["kind"], "server");
        assert_eq!(labels["code"], "503");
        let (line, _) = run(r#"{a="b"} | line_format "{{ __timestamp__ }}""#, "x", &[]).unwrap();
        assert_eq!(line, "2023-11-14 22:13:20 +0000 UTC");
        let (_, labels) = run(r#"{a="b"} | line_format "{{ nosuchfn }}""#, "x", &[]).unwrap();
        assert_eq!(labels["__error__"], "TemplateFormatErr");
        let Expr::Log(log) =
            logql_syntax::parse(r#"{a="b"} | line_format "{{ range .x }}""#).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            Pipeline::compile(&log.stages).err().unwrap().reason,
            "bad_data"
        );
    }
}
