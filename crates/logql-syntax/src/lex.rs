//! LogQL tokens.

use crate::{ParseError, Result};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Tok {
    Ident(String),
    Str(String),
    Num(f64),
    /// Seconds.
    Duration(f64),
    Bytes(f64),
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Eq,
    EqEq,
    Ne,
    Re,
    NotRe,
    Pipe,
    PipeEq,
    PipeRe,
    PipePat,
    NotPat,
    Gt,
    Ge,
    Lt,
    Le,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Eof,
}

pub(crate) fn err(position: usize, message: impl std::fmt::Display) -> ParseError {
    ParseError {
        position,
        message: message.to_string(),
    }
}

fn duration_unit(unit: &str) -> Option<f64> {
    Some(match unit {
        "ns" => 1e-9,
        "us" | "µs" => 1e-6,
        "ms" => 1e-3,
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 86_400.0,
        "w" => 604_800.0,
        "y" => 31_536_000.0,
        _ => return None,
    })
}

fn bytes_unit(unit: &str) -> Option<f64> {
    let kib = 1024f64;
    Some(match unit.to_ascii_lowercase().as_str() {
        "b" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "pb" => 1e15,
        "eb" => 1e18,
        "kib" | "ki" | "k" => kib,
        "mib" | "mi" => kib.powi(2),
        "gib" | "gi" => kib.powi(3),
        "tib" | "ti" => kib.powi(4),
        "pib" | "pi" => kib.powi(5),
        "eib" | "ei" => kib.powi(6),
        _ => return None,
    })
}

/// Unescapes a double-quoted LogQL string (Go escapes).
fn unquote(raw: &str, pos: usize) -> Result<String> {
    let mut out = String::new();
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next().ok_or_else(|| err(pos, "dangling escape"))? {
            'n' => out.push('\n'),
            't' => out.push('\t'),
            'r' => out.push('\r'),
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            'a' => out.push('\x07'),
            'b' => out.push('\x08'),
            'f' => out.push('\x0c'),
            'v' => out.push('\x0b'),
            'x' => {
                let hex: String = chars.by_ref().take(2).collect();
                let byte = u8::from_str_radix(&hex, 16).map_err(|_| err(pos, "bad \\x escape"))?;
                out.push(char::from(byte));
            }
            other => return Err(err(pos, format!("unknown escape \\{other}"))),
        }
    }
    Ok(out)
}

pub(crate) fn lex(input: &str) -> Result<Vec<(usize, Tok)>> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let at = |i: usize| chars.get(i).map(|(_, c)| *c);
    while i < chars.len() {
        let (pos, c) = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '#' => {
                while at(i).is_some_and(|c| c != '\n') {
                    i += 1;
                }
            }
            '"' => {
                let start = i + 1;
                i += 1;
                while at(i).is_some_and(|c| c != '"') {
                    i += if at(i) == Some('\\') { 2 } else { 1 };
                }
                if at(i).is_none() {
                    return Err(err(pos, "unterminated string"));
                }
                let raw: String = chars[start..i].iter().map(|(_, c)| c).collect();
                out.push((pos, Tok::Str(unquote(&raw, pos)?)));
                i += 1;
            }
            '`' => {
                let start = i + 1;
                i += 1;
                while at(i).is_some_and(|c| c != '`') {
                    i += 1;
                }
                if at(i).is_none() {
                    return Err(err(pos, "unterminated raw string"));
                }
                out.push((
                    pos,
                    Tok::Str(chars[start..i].iter().map(|(_, c)| c).collect()),
                ));
                i += 1;
            }
            c if c.is_ascii_digit()
                || (c == '.' && at(i + 1).is_some_and(|d| d.is_ascii_digit())) =>
            {
                let (tok, next) = lex_number(&chars, i, pos)?;
                out.push((pos, tok));
                i = next;
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while at(i).is_some_and(|c| c.is_alphanumeric() || c == '_') {
                    i += 1;
                }
                out.push((
                    pos,
                    Tok::Ident(chars[start..i].iter().map(|(_, c)| c).collect()),
                ));
            }
            _ => {
                let two: String = chars[i..chars.len().min(i + 2)]
                    .iter()
                    .map(|(_, c)| c)
                    .collect();
                let (tok, len) = match two.as_str() {
                    "==" => (Tok::EqEq, 2),
                    "!=" => (Tok::Ne, 2),
                    "=~" => (Tok::Re, 2),
                    "!~" => (Tok::NotRe, 2),
                    "|=" => (Tok::PipeEq, 2),
                    "|~" => (Tok::PipeRe, 2),
                    "|>" => (Tok::PipePat, 2),
                    "!>" => (Tok::NotPat, 2),
                    ">=" => (Tok::Ge, 2),
                    "<=" => (Tok::Le, 2),
                    _ => match c {
                        '{' => (Tok::LBrace, 1),
                        '}' => (Tok::RBrace, 1),
                        '(' => (Tok::LParen, 1),
                        ')' => (Tok::RParen, 1),
                        '[' => (Tok::LBracket, 1),
                        ']' => (Tok::RBracket, 1),
                        ',' => (Tok::Comma, 1),
                        '=' => (Tok::Eq, 1),
                        '|' => (Tok::Pipe, 1),
                        '>' => (Tok::Gt, 1),
                        '<' => (Tok::Lt, 1),
                        '+' => (Tok::Plus, 1),
                        '-' => (Tok::Minus, 1),
                        '*' => (Tok::Star, 1),
                        '/' => (Tok::Slash, 1),
                        '%' => (Tok::Percent, 1),
                        '^' => (Tok::Caret, 1),
                        other => return Err(err(pos, format!("unexpected character {other:?}"))),
                    },
                };
                out.push((pos, tok));
                i += len;
            }
        }
    }
    out.push((input.len(), Tok::Eof));
    Ok(out)
}

/// A number, optionally with a duration (`5m`, `1h30m`) or bytes (`10KB`) unit.
fn lex_number(chars: &[(usize, char)], start: usize, pos: usize) -> Result<(Tok, usize)> {
    let at = |i: usize| chars.get(i).map(|(_, c)| *c);
    let mut i = start;
    let mut total_secs = 0.0;
    let mut saw_duration = false;
    loop {
        let begin = i;
        while at(i).is_some_and(|c| c.is_ascii_digit() || c == '.') {
            i += 1;
        }
        if matches!(at(i), Some('e' | 'E'))
            && at(i + 1).is_some_and(|c| c.is_ascii_digit() || c == '+' || c == '-')
            && !saw_duration
        {
            i += 2;
            while at(i).is_some_and(|c| c.is_ascii_digit()) {
                i += 1;
            }
        }
        let text: String = chars[begin..i].iter().map(|(_, c)| c).collect();
        let value: f64 = text
            .parse()
            .map_err(|_| err(pos, format!("bad number {text:?}")))?;
        let unit_start = i;
        while at(i).is_some_and(|c| c.is_alphabetic() || c == 'µ') {
            i += 1;
        }
        let unit: String = chars[unit_start..i].iter().map(|(_, c)| c).collect();
        if unit.is_empty() {
            return Ok((Tok::Num(value), i));
        }
        if let Some(scale) = duration_unit(&unit) {
            total_secs += value * scale;
            saw_duration = true;
            // `1h30m`: another number-unit pair directly after.
            if at(i).is_some_and(|c| c.is_ascii_digit()) {
                continue;
            }
            return Ok((Tok::Duration(total_secs), i));
        }
        if !saw_duration {
            if let Some(scale) = bytes_unit(&unit) {
                return Ok((Tok::Bytes(value * scale), i));
            }
        }
        return Err(err(pos, format!("unknown unit {unit:?}")));
    }
}
