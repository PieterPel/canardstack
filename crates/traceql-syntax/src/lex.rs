//! TraceQL tokens. Attribute names are lexed as one token because their syntax differs from the
//! rest of the language: after `.`, `span.`, `resource.`, ... everything up to whitespace or one of
//! `{}()=~!<>&|^,` belongs to the name, and `"quoted segments"` may contain anything.

use crate::ast::{Attribute, Intrinsic, Scope};
use crate::ParseError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum P {
    Comma,
    LBrace,
    RBrace,
    LParen,
    RParen,
    Eq,
    Ne,
    Re,
    NotRe,
    Gt,
    Ge,
    Lt,
    Le,
    Add,
    Sub,
    Div,
    Mod,
    Mul,
    Pow,
    And,
    Or,
    Not,
    Pipe,
    Desc,
    Anc,
    Sibl,
    NotChild,
    NotParent,
    NotDesc,
    NotAnce,
    UnionSibl,
    UnionChild,
    UnionParent,
    UnionDesc,
    UnionAnce,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Tok {
    /// A keyword or identifier: `true`, `count`, `duration`, `rate`, `sample`, ...
    Word(String),
    Str(String),
    Int(i64),
    Float(f64),
    /// Nanoseconds.
    Duration(i64),
    Attr(Attribute),
    P(P),
    Eof,
}

const OPERATORS: &[(&str, P)] = &[
    ("!>>", P::NotDesc),
    ("!<<", P::NotAnce),
    ("&>>", P::UnionDesc),
    ("&<<", P::UnionAnce),
    ("&&", P::And),
    ("||", P::Or),
    (">>", P::Desc),
    ("<<", P::Anc),
    ("!>", P::NotChild),
    ("!<", P::NotParent),
    ("&~", P::UnionSibl),
    ("&>", P::UnionChild),
    ("&<", P::UnionParent),
    ("!=", P::Ne),
    ("=~", P::Re),
    ("!~", P::NotRe),
    (">=", P::Ge),
    ("<=", P::Le),
    (",", P::Comma),
    ("{", P::LBrace),
    ("}", P::RBrace),
    ("(", P::LParen),
    (")", P::RParen),
    ("=", P::Eq),
    (">", P::Gt),
    ("<", P::Lt),
    ("+", P::Add),
    ("-", P::Sub),
    ("/", P::Div),
    ("%", P::Mod),
    ("*", P::Mul),
    ("^", P::Pow),
    ("!", P::Not),
    ("|", P::Pipe),
    ("~", P::Sibl),
];

fn err(position: usize, message: impl Into<String>) -> ParseError {
    ParseError {
        position,
        message: message.into(),
    }
}

fn is_attribute_rune(c: char) -> bool {
    !c.is_whitespace()
        && !matches!(
            c,
            '{' | '}' | '(' | ')' | '=' | '~' | '!' | '<' | '>' | '&' | '|' | '^' | ','
        )
}

fn scoped_intrinsic(scope: &str, name: &str) -> Option<Intrinsic> {
    Some(match (scope, name) {
        ("trace", "duration") => Intrinsic::TraceDuration,
        ("trace", "rootName") => Intrinsic::TraceRootSpan,
        ("trace", "rootService") => Intrinsic::TraceRootService,
        ("trace", "id") => Intrinsic::TraceId,
        ("span", "duration") => Intrinsic::Duration,
        ("span", "name") => Intrinsic::Name,
        ("span", "kind") => Intrinsic::Kind,
        ("span", "status") => Intrinsic::Status,
        ("span", "statusMessage") => Intrinsic::StatusMessage,
        ("span", "id") => Intrinsic::SpanId,
        ("span", "parentID") => Intrinsic::ParentId,
        ("span", "childCount") => Intrinsic::ChildCount,
        ("event", "name") => Intrinsic::EventName,
        ("event", "timeSinceStart") => Intrinsic::EventTimeSinceStart,
        ("link", "traceID") => Intrinsic::LinkTraceId,
        ("link", "spanID") => Intrinsic::LinkSpanId,
        ("instrumentation", "name") => Intrinsic::InstrumentationName,
        ("instrumentation", "version") => Intrinsic::InstrumentationVersion,
        _ => return None,
    })
}

/// The unscoped keywords that are intrinsics (`duration`, `name`, `rootName`, ...).
pub(crate) fn intrinsic_keyword(word: &str) -> Option<Intrinsic> {
    Some(match word {
        "duration" => Intrinsic::Duration,
        "name" => Intrinsic::Name,
        "status" => Intrinsic::Status,
        "statusMessage" => Intrinsic::StatusMessage,
        "kind" => Intrinsic::Kind,
        "parent" => Intrinsic::Parent,
        "rootName" => Intrinsic::TraceRootSpan,
        "rootServiceName" => Intrinsic::TraceRootService,
        "traceDuration" => Intrinsic::TraceDuration,
        "nestedSetLeft" => Intrinsic::NestedSetLeft,
        "nestedSetRight" => Intrinsic::NestedSetRight,
        "nestedSetParent" => Intrinsic::NestedSetParent,
        _ => return None,
    })
}

fn scope_prefix(word: &str) -> Option<(Scope, bool)> {
    Some(match word {
        "resource" => (Scope::Resource, false),
        "span" => (Scope::Span, false),
        "parent" => (Scope::None, true),
        "event" => (Scope::Event, false),
        "link" => (Scope::Link, false),
        "instrumentation" => (Scope::Instrumentation, false),
        _ => return None,
    })
}

struct Lexer<'a> {
    chars: Vec<(usize, char)>,
    i: usize,
    src: &'a str,
}

impl Lexer<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.i).map(|(_, c)| *c)
    }

    fn peek_at(&self, n: usize) -> Option<char> {
        self.chars.get(self.i + n).map(|(_, c)| *c)
    }

    fn pos(&self) -> usize {
        self.chars.get(self.i).map_or(self.src.len(), |(p, _)| *p)
    }

    fn rest_starts_with(&self, text: &str) -> bool {
        self.src[self.pos()..].starts_with(text)
    }

    fn attribute_name(&mut self) -> Result<String, ParseError> {
        let start = self.pos();
        let mut name = String::new();
        while let Some(c) = self.peek() {
            if c == '"' {
                self.i += 1;
                loop {
                    match self.peek() {
                        None => return Err(err(start, "unexpected EOF, expecting \"")),
                        Some('"') => {
                            self.i += 1;
                            break;
                        }
                        Some('\\') => match self.peek_at(1) {
                            Some(e @ ('"' | '\\')) => {
                                name.push(e);
                                self.i += 2;
                            }
                            _ => return Err(err(self.pos(), "invalid escape sequence")),
                        },
                        Some(c) => {
                            name.push(c);
                            self.i += 1;
                        }
                    }
                }
            } else if is_attribute_rune(c) {
                name.push(c);
                self.i += 1;
            } else {
                break;
            }
        }
        if name.is_empty() {
            return Err(err(start, "expected an attribute name"));
        }
        Ok(name)
    }

    fn word(&mut self) -> String {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            self.i += 1;
        }
        self.chars[start..self.i].iter().map(|(_, c)| c).collect()
    }

    fn number(&mut self, start_pos: usize) -> Result<Tok, ParseError> {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        let mut float = false;
        if self.peek() == Some('.') && self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
            float = true;
            self.i += 1;
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.i += 1;
            }
        }
        let number_end = self.i;
        // A duration is the number plus the unit characters that follow with no space.
        let mut j = self.i;
        while let Some((_, c)) = self.chars.get(j) {
            if c.is_whitespace() || !(c.is_ascii_digit() || *c == '.' || "nusmhµdwy".contains(*c))
            {
                break;
            }
            j += 1;
        }
        if j > number_end {
            let text: String = self.chars[start..j].iter().map(|(_, c)| c).collect();
            if let Some(ns) = parse_duration(&text) {
                self.i = j;
                return Ok(Tok::Duration(ns));
            }
        }
        let text: String = self.chars[start..number_end]
            .iter()
            .map(|(_, c)| c)
            .collect();
        if float {
            text.parse()
                .map(Tok::Float)
                .map_err(|_| err(start_pos, format!("bad number {text}")))
        } else {
            text.parse()
                .map(Tok::Int)
                .map_err(|_| err(start_pos, format!("integer {text} is out of range")))
        }
    }

    fn string(&mut self, quote: char) -> Result<String, ParseError> {
        let start = self.pos();
        self.i += 1;
        let mut out = String::new();
        loop {
            let c = self
                .peek()
                .ok_or_else(|| err(start, "unterminated string"))?;
            self.i += 1;
            if c == quote {
                return Ok(out);
            }
            if quote == '`' || c != '\\' {
                out.push(c);
                continue;
            }
            let e = self
                .peek()
                .ok_or_else(|| err(start, "unterminated string"))?;
            self.i += 1;
            out.push(match e {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                'a' => '\x07',
                'b' => '\x08',
                'f' => '\x0c',
                'v' => '\x0b',
                '\\' => '\\',
                '"' => '"',
                'x' | 'u' | 'U' => {
                    let digits = match e {
                        'x' => 2,
                        'u' => 4,
                        _ => 8,
                    };
                    let hex: String = (0..digits)
                        .filter_map(|_| self.peek().inspect(|_| self.i += 1))
                        .collect();
                    let code = u32::from_str_radix(&hex, 16)
                        .map_err(|_| err(start, "invalid escape sequence"))?;
                    char::from_u32(code).ok_or_else(|| err(start, "invalid escape sequence"))?
                }
                _ => return Err(err(start, "invalid escape sequence")),
            });
        }
    }
}

pub(crate) fn parse_duration(text: &str) -> Option<i64> {
    prometheus_duration(text).or_else(|| go_duration(text))
}

/// `1h30m`, `500ms`, `2d`, `1w`, `1y`: integer amounts with ms/s/m/h/d/w/y.
fn prometheus_duration(text: &str) -> Option<i64> {
    let mut rest = text;
    let mut total: i64 = 0;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit())?;
        if digits == 0 {
            return None;
        }
        let amount: i64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let (unit, ns) = [
            ("ms", 1_000_000i64),
            ("s", 1_000_000_000),
            ("m", 60_000_000_000),
            ("h", 3_600_000_000_000),
            ("d", 86_400_000_000_000),
            ("w", 604_800_000_000_000),
            ("y", 31_536_000_000_000_000),
        ]
        .into_iter()
        .find(|(u, _)| rest.starts_with(u))?;
        total = total.checked_add(amount.checked_mul(ns)?)?;
        rest = &rest[unit.len()..];
    }
    Some(total)
}

/// Go's `time.ParseDuration`: decimal amounts with ns/us/µs/ms/s/m/h.
fn go_duration(text: &str) -> Option<i64> {
    let mut rest = text;
    let mut total = 0f64;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
        let amount: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let (unit, ns) = [
            ("ns", 1f64),
            ("us", 1e3),
            ("µs", 1e3),
            ("ms", 1e6),
            ("s", 1e9),
            ("m", 6e10),
            ("h", 3.6e12),
        ]
        .into_iter()
        .find(|(u, _)| rest.starts_with(u))?;
        total += amount * ns;
        rest = &rest[unit.len()..];
    }
    Some(total as i64)
}

pub(crate) fn lex(src: &str) -> Result<Vec<(usize, Tok)>, ParseError> {
    let mut l = Lexer {
        chars: src.char_indices().collect(),
        i: 0,
        src,
    };
    let mut out = Vec::new();
    while let Some(c) = l.peek() {
        let pos = l.pos();
        if c.is_whitespace() {
            l.i += 1;
        } else if c == '"' || c == '`' {
            out.push((pos, Tok::Str(l.string(c)?)));
        } else if c.is_ascii_digit() {
            let tok = l.number(pos)?;
            out.push((pos, tok));
        } else if c == '.' {
            l.i += 1;
            let name = l.attribute_name()?;
            out.push((
                pos,
                Tok::Attr(Attribute {
                    scope: Scope::None,
                    parent: false,
                    intrinsic: None,
                    name,
                }),
            ));
        } else if c.is_alphabetic() || c == '_' {
            let word = l.word();
            if l.peek() == Some('.') {
                if let Some((mut scope, parent)) = scope_prefix(&word) {
                    l.i += 1;
                    // `parent.span.x` and `parent.resource.x`
                    if parent {
                        for (prefix, nested) in
                            [("span.", Scope::Span), ("resource.", Scope::Resource)]
                        {
                            if l.rest_starts_with(prefix) {
                                l.i += prefix.len();
                                scope = nested;
                                break;
                            }
                        }
                    }
                    let name = l.attribute_name()?;
                    out.push((
                        pos,
                        Tok::Attr(Attribute {
                            scope,
                            parent,
                            intrinsic: None,
                            name,
                        }),
                    ));
                    continue;
                }
            }
            if l.peek() == Some(':')
                && matches!(
                    word.as_str(),
                    "trace" | "span" | "event" | "link" | "instrumentation"
                )
            {
                l.i += 1;
                let name = l.word();
                let intrinsic = scoped_intrinsic(&word, &name)
                    .ok_or_else(|| err(pos, format!("{word}:{name} is not an intrinsic")))?;
                out.push((
                    pos,
                    Tok::Attr(Attribute {
                        scope: Scope::None,
                        parent: false,
                        intrinsic: Some(intrinsic),
                        name: format!("{word}:{name}"),
                    }),
                ));
                continue;
            }
            out.push((pos, Tok::Word(word)));
        } else {
            let rest = &src[pos..];
            let (text, p) = OPERATORS
                .iter()
                .filter(|(t, _)| rest.starts_with(t))
                .max_by_key(|(t, _)| t.len())
                .ok_or_else(|| err(pos, format!("unexpected character {c:?}")))?;
            l.i += text.chars().count();
            out.push((pos, Tok::P(*p)));
        }
    }
    out.push((src.len(), Tok::Eof));
    Ok(out)
}
