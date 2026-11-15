//! A subset of Go's `text/template` as Loki uses it in `line_format` and `label_format`: labels as
//! `.name`, pipelines (`{{ .a | ToLower | trunc 3 }}`), `if` / `else if` / `else`, comparison and
//! string functions, `printf`, and `__line__` / `__timestamp__`. `range`, `with`, variables and
//! `define` are not supported.

use crate::query::prom_labels::Labels;
use chrono::{TimeZone, Utc};
use regex::Regex;

#[derive(Clone, Debug)]
pub enum V {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
}

impl V {
    fn text(&self) -> String {
        match self {
            V::Str(s) => s.clone(),
            V::Int(i) => i.to_string(),
            V::Float(f) => f.to_string(),
            V::Bool(b) => b.to_string(),
            V::Nil => "<nil>".to_string(),
        }
    }

    fn truthy(&self) -> bool {
        match self {
            V::Str(s) => !s.is_empty(),
            V::Int(i) => *i != 0,
            V::Float(f) => *f != 0.0,
            V::Bool(b) => *b,
            V::Nil => false,
        }
    }

    fn number(&self) -> Option<f64> {
        match self {
            V::Int(i) => Some(*i as f64),
            V::Float(f) => Some(*f),
            V::Str(s) => s.trim().parse().ok(),
            V::Bool(_) | V::Nil => None,
        }
    }
}

#[derive(Debug)]
enum Operand {
    Field(String),
    Str(String),
    Num(V),
    Ident(String),
    Paren(Pipe),
}

#[derive(Debug)]
struct Pipe(Vec<Vec<Operand>>);

#[derive(Debug)]
enum Node {
    Text(String),
    Action(Pipe),
    If {
        cond: Pipe,
        then: Vec<Node>,
        otherwise: Vec<Node>,
    },
}

#[derive(Debug)]
pub struct Template {
    nodes: Vec<Node>,
}

/// What a template can read besides the labels.
pub struct Ctx<'a> {
    pub labels: &'a Labels,
    pub line: &'a str,
    pub ts_ns: i64,
}

#[derive(Debug, PartialEq)]
enum Tok {
    Ident(String),
    Field(String),
    Str(String),
    Num(String),
    Pipe,
    LParen,
    RParen,
}

fn tokenize(action: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = action.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '|' => {
                out.push(Tok::Pipe);
                i += 1;
            }
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("unterminated string".into()),
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some('\\') => {
                            s.push(match chars.get(i + 1) {
                                Some('n') => '\n',
                                Some('t') => '\t',
                                Some(c) => *c,
                                None => return Err("unterminated string".into()),
                            });
                            i += 2;
                        }
                        Some(c) => {
                            s.push(*c);
                            i += 1;
                        }
                    }
                }
                out.push(Tok::Str(s));
            }
            '`' => {
                let end = chars[i + 1..]
                    .iter()
                    .position(|c| *c == '`')
                    .ok_or("unterminated raw string")?;
                out.push(Tok::Str(chars[i + 1..i + 1 + end].iter().collect()));
                i += end + 2;
            }
            '.' if chars
                .get(i + 1)
                .is_some_and(|c| c.is_alphabetic() || *c == '_') =>
            {
                let start = i + 1;
                i += 1;
                while chars
                    .get(i)
                    .is_some_and(|c| c.is_alphanumeric() || *c == '_')
                {
                    i += 1;
                }
                out.push(Tok::Field(chars[start..i].iter().collect()));
            }
            c if c.is_ascii_digit()
                || (c == '-' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit())) =>
            {
                let start = i;
                i += 1;
                while chars
                    .get(i)
                    .is_some_and(|c| c.is_ascii_digit() || *c == '.')
                {
                    i += 1;
                }
                out.push(Tok::Num(chars[start..i].iter().collect()));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while chars
                    .get(i)
                    .is_some_and(|c| c.is_alphanumeric() || *c == '_')
                {
                    i += 1;
                }
                out.push(Tok::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(format!("unexpected {other:?} in template action")),
        }
    }
    Ok(out)
}

fn parse_pipe(toks: &[Tok], pos: &mut usize) -> Result<Pipe, String> {
    let mut commands = vec![Vec::new()];
    while *pos < toks.len() {
        match &toks[*pos] {
            Tok::RParen => break,
            Tok::Pipe => {
                commands.push(Vec::new());
                *pos += 1;
            }
            tok => {
                let operand = match tok {
                    Tok::Ident(name) => Operand::Ident(name.clone()),
                    Tok::Field(name) => Operand::Field(name.clone()),
                    Tok::Str(s) => Operand::Str(s.clone()),
                    Tok::Num(n) => Operand::Num(match n.parse::<i64>() {
                        Ok(i) => V::Int(i),
                        Err(_) => {
                            V::Float(n.parse::<f64>().map_err(|_| format!("bad number {n}"))?)
                        }
                    }),
                    Tok::LParen => {
                        *pos += 1;
                        let inner = parse_pipe(toks, pos)?;
                        if toks.get(*pos) != Some(&Tok::RParen) {
                            return Err("missing )".into());
                        }
                        Operand::Paren(inner)
                    }
                    _ => unreachable!(),
                };
                commands.last_mut().expect("one command").push(operand);
                *pos += 1;
            }
        }
    }
    if commands.iter().any(Vec::is_empty) {
        return Err("empty command in a pipeline".into());
    }
    Ok(Pipe(commands))
}

enum Piece {
    Text(String),
    Action(String),
}

fn split(source: &str) -> Result<Vec<Piece>, String> {
    let mut pieces = Vec::new();
    let mut rest = source;
    let mut trim_next = false;
    while let Some(open) = rest.find("{{") {
        let mut text = &rest[..open];
        if trim_next {
            text = text.trim_start();
        }
        let mut inner_start = open + 2;
        if rest[inner_start..].starts_with('-')
            && rest[inner_start + 1..].starts_with(char::is_whitespace)
        {
            text = text.trim_end();
            inner_start += 1;
        }
        if !text.is_empty() {
            pieces.push(Piece::Text(text.to_string()));
        }
        let close = rest[inner_start..].find("}}").ok_or("unclosed {{")? + inner_start;
        let mut inner = &rest[inner_start..close];
        trim_next = false;
        if inner.ends_with('-') && inner[..inner.len() - 1].ends_with(char::is_whitespace) {
            inner = &inner[..inner.len() - 1];
            trim_next = true;
        }
        pieces.push(Piece::Action(inner.trim().to_string()));
        rest = &rest[close + 2..];
    }
    let tail = if trim_next { rest.trim_start() } else { rest };
    if !tail.is_empty() {
        pieces.push(Piece::Text(tail.to_string()));
    }
    Ok(pieces)
}

fn parse_nodes(
    pieces: &mut std::iter::Peekable<std::vec::IntoIter<Piece>>,
    until_end: bool,
) -> Result<(Vec<Node>, String), String> {
    let mut nodes = Vec::new();
    while let Some(piece) = pieces.next() {
        match piece {
            Piece::Text(t) => nodes.push(Node::Text(t)),
            Piece::Action(a) => {
                if a.starts_with("/*") && a.ends_with("*/") {
                    continue;
                }
                let keyword = a.split_whitespace().next().unwrap_or("");
                match keyword {
                    "end" if until_end => return Ok((nodes, "end".into())),
                    "else" if until_end => return Ok((nodes, a)),
                    "if" => nodes.push(parse_if(&a["if".len()..], pieces)?),
                    "range" | "with" | "define" | "template" | "block" => {
                        return Err(format!("`{keyword}` is not supported in templates"))
                    }
                    "end" | "else" => return Err(format!("unexpected {{{{ {keyword} }}}}")),
                    _ => {
                        let toks = tokenize(&a)?;
                        if toks.contains(&Tok::Ident(":=".into()))
                            || a.contains(":=")
                            || a.starts_with('$')
                        {
                            return Err("template variables are not supported".into());
                        }
                        nodes.push(Node::Action(parse_pipe(&toks, &mut 0)?));
                    }
                }
            }
        }
    }
    if until_end {
        return Err("missing {{ end }}".into());
    }
    Ok((nodes, String::new()))
}

fn parse_if(
    condition: &str,
    pieces: &mut std::iter::Peekable<std::vec::IntoIter<Piece>>,
) -> Result<Node, String> {
    let cond = parse_pipe(&tokenize(condition)?, &mut 0)?;
    let (then, stop) = parse_nodes(pieces, true)?;
    let otherwise = if stop == "end" {
        Vec::new()
    } else if let Some(rest) = stop.strip_prefix("else") {
        let rest = rest.trim();
        if let Some(cond) = rest.strip_prefix("if") {
            vec![parse_if(cond, pieces)?]
        } else if rest.is_empty() {
            let (nodes, stop) = parse_nodes(pieces, true)?;
            if stop != "end" {
                return Err("unexpected else after else".into());
            }
            nodes
        } else {
            return Err(format!("unexpected text after else: {rest}"));
        }
    } else {
        return Err("malformed if".into());
    };
    Ok(Node::If {
        cond,
        then,
        otherwise,
    })
}

impl Template {
    pub fn parse(source: &str) -> Result<Self, String> {
        let mut pieces = split(source)?.into_iter().peekable();
        let (nodes, _) = parse_nodes(&mut pieces, false)?;
        Ok(Self { nodes })
    }

    pub fn render(&self, ctx: &Ctx) -> Result<String, String> {
        let mut out = String::new();
        render_nodes(&self.nodes, ctx, &mut out)?;
        Ok(out)
    }
}

fn render_nodes(nodes: &[Node], ctx: &Ctx, out: &mut String) -> Result<(), String> {
    for node in nodes {
        match node {
            Node::Text(t) => out.push_str(t),
            Node::Action(pipe) => out.push_str(&eval_pipe(pipe, ctx)?.text()),
            Node::If {
                cond,
                then,
                otherwise,
            } => {
                let branch = if eval_pipe(cond, ctx)?.truthy() {
                    then
                } else {
                    otherwise
                };
                render_nodes(branch, ctx, out)?;
            }
        }
    }
    Ok(())
}

fn eval_pipe(pipe: &Pipe, ctx: &Ctx) -> Result<V, String> {
    let mut piped: Option<V> = None;
    for command in &pipe.0 {
        piped = Some(eval_command(command, piped, ctx)?);
    }
    Ok(piped.unwrap_or(V::Nil))
}

fn eval_operand(op: &Operand, ctx: &Ctx) -> Result<V, String> {
    Ok(match op {
        Operand::Field(name) => V::Str(ctx.labels.get(name).cloned().unwrap_or_default()),
        Operand::Str(s) => V::Str(s.clone()),
        Operand::Num(v) => v.clone(),
        Operand::Paren(pipe) => eval_pipe(pipe, ctx)?,
        Operand::Ident(name) => call(name, Vec::new(), ctx)?,
    })
}

fn eval_command(command: &[Operand], piped: Option<V>, ctx: &Ctx) -> Result<V, String> {
    if let Operand::Ident(name) = &command[0] {
        let mut args = command[1..]
            .iter()
            .map(|o| eval_operand(o, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        args.extend(piped);
        return call(name, args, ctx);
    }
    if command.len() > 1 || piped.is_some() {
        return Err("a value cannot be called as a function".into());
    }
    eval_operand(&command[0], ctx)
}

fn cmp(a: &V, b: &V) -> std::cmp::Ordering {
    match (a, b) {
        (V::Str(x), V::Str(y)) => x.cmp(y),
        _ => match (a.number(), b.number()) {
            (Some(x), Some(y)) => x.total_cmp(&y),
            _ => a.text().cmp(&b.text()),
        },
    }
}

fn equal(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::Bool(x), V::Bool(y)) => x == y,
        (V::Nil, V::Nil) => true,
        (V::Str(x), V::Str(y)) => x == y,
        _ => {
            a.number().is_some() && b.number().is_some() && cmp(a, b).is_eq()
                || a.text() == b.text()
                    && !matches!(
                        (a, b),
                        (V::Str(_), V::Int(_) | V::Float(_)) | (V::Int(_) | V::Float(_), V::Str(_))
                    )
        }
    }
}

fn go_time(ts_ns: i64) -> String {
    let t = Utc.timestamp_nanos(ts_ns);
    let nanos = t.timestamp_subsec_nanos();
    let fraction = if nanos == 0 {
        String::new()
    } else {
        format!(".{nanos:09}").trim_end_matches('0').to_string()
    };
    format!("{}{} +0000 UTC", t.format("%Y-%m-%d %H:%M:%S"), fraction)
}

fn string_arg(args: &[V], i: usize, name: &str) -> Result<String, String> {
    args.get(i)
        .map(V::text)
        .ok_or_else(|| format!("{name}: missing argument"))
}

fn printf(args: &[V]) -> Result<String, String> {
    let format = string_arg(args, 0, "printf")?;
    let mut out = String::new();
    let mut rest = args[1..].iter();
    let mut chars = format.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut spec = String::new();
        while let Some(&n) = chars.peek() {
            if n.is_ascii_digit() || n == '.' || n == '-' || n == '+' {
                spec.push(n);
                chars.next();
            } else {
                break;
            }
        }
        let verb = chars.next().ok_or("printf: truncated format")?;
        if verb == '%' {
            out.push('%');
            continue;
        }
        let arg = rest.next().ok_or("printf: too few arguments")?;
        let precision = spec
            .split_once('.')
            .and_then(|(_, p)| p.parse::<usize>().ok());
        let width: usize = spec
            .split('.')
            .next()
            .unwrap_or("")
            .trim_start_matches(['-', '+'])
            .parse()
            .unwrap_or(0);
        let body = match verb {
            's' | 'v' => arg.text(),
            'q' => format!("{:?}", arg.text()),
            'd' => arg
                .number()
                .map(|n| (n as i64).to_string())
                .ok_or("printf: %d needs a number")?,
            'f' => arg
                .number()
                .map(|n| format!("{:.*}", precision.unwrap_or(6), n))
                .ok_or("printf: %f needs a number")?,
            'x' => arg
                .number()
                .map(|n| format!("{:x}", n as i64))
                .ok_or("printf: %x needs a number")?,
            other => return Err(format!("printf: unsupported verb %{other}")),
        };
        if spec.starts_with('-') {
            out.push_str(&format!("{body:<width$}"));
        } else {
            out.push_str(&format!("{body:>width$}"));
        }
    }
    Ok(out)
}

fn number_args(args: &[V], name: &str) -> Result<Vec<f64>, String> {
    args.iter()
        .map(|a| {
            a.number()
                .ok_or_else(|| format!("{name}: {} is not a number", a.text()))
        })
        .collect()
}

fn call(name: &str, args: Vec<V>, ctx: &Ctx) -> Result<V, String> {
    let s = |i: usize| string_arg(&args, i, name);
    Ok(match name {
        "__line__" => V::Str(ctx.line.to_string()),
        "__timestamp__" => V::Str(go_time(ctx.ts_ns)),
        "eq" => {
            let first = args.first().ok_or("eq: missing argument")?;
            V::Bool(args[1..].iter().any(|b| equal(first, b)))
        }
        "ne" => V::Bool(!equal(
            args.first().ok_or("ne: missing argument")?,
            args.get(1).ok_or("ne: missing argument")?,
        )),
        "lt" | "le" | "gt" | "ge" => {
            let ord = cmp(
                args.first().ok_or("missing argument")?,
                args.get(1).ok_or("missing argument")?,
            );
            V::Bool(match name {
                "lt" => ord.is_lt(),
                "le" => ord.is_le(),
                "gt" => ord.is_gt(),
                _ => ord.is_ge(),
            })
        }
        "and" => args
            .iter()
            .find(|a| !a.truthy())
            .or(args.last())
            .cloned()
            .unwrap_or(V::Nil),
        "or" => args
            .iter()
            .find(|a| a.truthy())
            .or(args.last())
            .cloned()
            .unwrap_or(V::Nil),
        "not" => V::Bool(!args.first().ok_or("not: missing argument")?.truthy()),
        "len" => V::Int(s(0)?.chars().count() as i64),
        "ToLower" | "lower" => V::Str(s(0)?.to_lowercase()),
        "ToUpper" | "upper" => V::Str(s(0)?.to_uppercase()),
        "Title" | "title" => V::Str(
            s(0)?
                .split(' ')
                .map(|w| {
                    let mut cs = w.chars();
                    cs.next()
                        .map(|f| f.to_uppercase().collect::<String>() + cs.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" "),
        ),
        "trim" => V::Str(s(0)?.trim().to_string()),
        "trimAll" => V::Str(
            s(1)?
                .trim_matches(|c| s(0).is_ok_and(|set| set.contains(c)))
                .to_string(),
        ),
        "trimPrefix" => V::Str(
            s(1)?
                .strip_prefix(&s(0)?)
                .map(str::to_string)
                .unwrap_or(s(1)?),
        ),
        "trimSuffix" => V::Str(
            s(1)?
                .strip_suffix(&s(0)?)
                .map(str::to_string)
                .unwrap_or(s(1)?),
        ),
        "replace" => V::Str(s(2)?.replace(&s(0)?, &s(1)?)),
        "contains" => V::Bool(s(1)?.contains(&s(0)?)),
        "hasPrefix" => V::Bool(s(1)?.starts_with(&s(0)?)),
        "hasSuffix" => V::Bool(s(1)?.ends_with(&s(0)?)),
        "repeat" => V::Str(s(1)?.repeat(number_args(&args[..1], name)?[0].max(0.0) as usize)),
        "trunc" => {
            let n = number_args(&args[..1], name)?[0] as i64;
            let text = s(1)?;
            let count = text.chars().count() as i64;
            V::Str(if n >= 0 {
                text.chars().take(n as usize).collect()
            } else {
                text.chars().skip((count + n).max(0) as usize).collect()
            })
        }
        "substr" => {
            let n = number_args(&args[..2], name)?;
            let chars: Vec<char> = s(2)?.chars().collect();
            let start = (n[0].max(0.0) as usize).min(chars.len());
            let end = if n[1] < 0.0 {
                chars.len()
            } else {
                (n[1] as usize).min(chars.len())
            };
            V::Str(chars[start..end.max(start)].iter().collect())
        }
        "quote" => V::Str(format!("{:?}", s(0)?)),
        "squote" => V::Str(format!("'{}'", s(0)?)),
        "default" => {
            let given = args.get(1).ok_or("default: missing value")?;
            if given.truthy() {
                given.clone()
            } else {
                args[0].clone()
            }
        }
        "print" => V::Str(args.iter().map(V::text).collect::<Vec<_>>().join("")),
        "printf" => V::Str(printf(&args)?),
        "toString" => V::Str(s(0)?),
        "int" => V::Int(number_args(&args[..1], name)?[0] as i64),
        "float64" => V::Float(number_args(&args[..1], name)?[0]),
        "add" => V::Float(number_args(&args, name)?.iter().sum()),
        "mul" => V::Float(number_args(&args, name)?.iter().product()),
        "sub" => {
            let n = number_args(&args[..2], name)?;
            V::Float(n[0] - n[1])
        }
        "div" => {
            let n = number_args(&args[..2], name)?;
            V::Float(n[0] / n[1])
        }
        "mod" => {
            let n = number_args(&args[..2], name)?;
            V::Float(n[0] % n[1])
        }
        "max" => V::Float(
            number_args(&args, name)?
                .into_iter()
                .fold(f64::NEG_INFINITY, f64::max),
        ),
        "min" => V::Float(
            number_args(&args, name)?
                .into_iter()
                .fold(f64::INFINITY, f64::min),
        ),
        "floor" => V::Float(number_args(&args[..1], name)?[0].floor()),
        "ceil" => V::Float(number_args(&args[..1], name)?[0].ceil()),
        "round" => {
            let places = args.get(1).and_then(V::number).unwrap_or(0.0) as i32;
            let scale = 10f64.powi(places);
            V::Float((number_args(&args[..1], name)?[0] * scale).round() / scale)
        }
        "regexReplaceAll" | "regexReplaceAllLiteral" => {
            let re = Regex::new(&s(0)?).map_err(|e| format!("{name}: {e}"))?;
            let replacement = s(2)?;
            V::Str(if name == "regexReplaceAll" {
                re.replace_all(&s(1)?, replacement.as_str()).into_owned()
            } else {
                re.replace_all(&s(1)?, regex::NoExpand(&replacement))
                    .into_owned()
            })
        }
        other => return Err(format!("function {other:?} is not defined")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(source: &str, labels: &[(&str, &str)]) -> Result<String, String> {
        let labels: Labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Template::parse(source)?.render(&Ctx {
            labels: &labels,
            line: "the line",
            ts_ns: 1_700_000_000_123_456_789,
        })
    }

    #[test]
    fn fields_pipelines_and_functions_render() {
        let l = [("method", "GET"), ("status", "503"), ("name", "  padded  ")];
        assert_eq!(render("{{ .method }}", &l).unwrap(), "GET");
        assert_eq!(render("{{ .method | ToLower }}", &l).unwrap(), "get");
        assert_eq!(
            render("{{ .method | lower | trunc 2 | upper }}-{{ .status }}", &l).unwrap(),
            "GE-503"
        );
        assert_eq!(
            render("{{ .name | trim }}|{{ .missing }}|", &l).unwrap(),
            "padded||"
        );
        assert_eq!(
            render(r#"{{ replace "a" "b" "banana" }}"#, &l).unwrap(),
            "bbnbnb"
        );
        assert_eq!(
            render(r#"{{ printf "%s=%d (%.1f)" .method .status 2.25 }}"#, &l).unwrap(),
            "GET=503 (2.2)"
        );
        assert_eq!(
            render(r#"{{ regexReplaceAll "[0-9]+" .status "N" }}"#, &l).unwrap(),
            "N"
        );
        assert_eq!(render("{{ add 1 2 3 }}", &l).unwrap(), "6");
        assert_eq!(render(r#"{{ .method | quote }}"#, &l).unwrap(), "\"GET\"");
        assert_eq!(render("{{ __line__ }}", &l).unwrap(), "the line");
        assert_eq!(
            render("{{ __timestamp__ }}", &l).unwrap(),
            "2023-11-14 22:13:20.123456789 +0000 UTC"
        );
    }

    #[test]
    fn conditionals_and_comparisons_branch() {
        let l = [("status", "503"), ("level", "")];
        let t = r#"{{ if ge .status "500" }}error{{ else if eq .status "200" }}ok{{ else }}other{{ end }}"#;
        assert_eq!(render(t, &l).unwrap(), "error");
        assert_eq!(render(t, &[("status", "200")]).unwrap(), "ok");
        assert_eq!(render(t, &[("status", "404")]).unwrap(), "other");
        assert_eq!(
            render("{{ if .level }}has{{ else }}none{{ end }}", &l).unwrap(),
            "none"
        );
        assert_eq!(
            render(
                "{{ if and (gt (.status | int) 400) (not .level) }}y{{ end }}",
                &l
            )
            .unwrap(),
            "y"
        );
        assert_eq!(
            render("{{ .level | default \"info\" }}", &l).unwrap(),
            "info"
        );
    }

    #[test]
    fn whitespace_trim_markers_and_errors() {
        assert_eq!(render("a  {{- \"b\" -}}  c", &[]).unwrap(), "abc");
        assert!(render("{{ range .x }}{{ end }}", &[]).is_err());
        assert!(render("{{ $x := 1 }}", &[]).is_err());
        assert!(render("{{ nosuchfn 1 }}", &[]).is_err());
        assert!(render("{{ if .a }}open", &[]).is_err());
        assert!(render("{{ .a ", &[]).is_err());
    }
}
