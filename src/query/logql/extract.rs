//! Label extraction from the log line: `json`, `logfmt`, `pattern` and `unpack`.

use crate::query::prom_labels::sanitize_label_name;
use logql_syntax::ast::Extraction;
use serde_json::Value;

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Flattens nested objects with `_`; arrays stay as their JSON text.
fn flatten(prefix: &str, value: &Value, out: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                let key = sanitize_label_name(key);
                let name = if prefix.is_empty() {
                    key
                } else {
                    format!("{prefix}_{key}")
                };
                flatten(&name, inner, out);
            }
        }
        other => out.push((prefix.to_string(), text(other))),
    }
}

/// Follows `a.b`, `a["b c"]` and `a[0]` into the document.
fn lookup<'a>(root: &'a Value, expr: &str) -> Option<&'a Value> {
    let mut current = root;
    let mut rest = expr;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('[') {
            let end = after.find(']')?;
            let inner = after[..end].trim();
            current = match inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                Some(key) => current.get(key)?,
                None => current.get(inner.parse::<usize>().ok()?)?,
            };
            rest = &after[end + 1..];
        } else {
            let rest_trimmed = rest.strip_prefix('.').unwrap_or(rest);
            let end = rest_trimmed.find(['.', '[']).unwrap_or(rest_trimmed.len());
            current = current.get(&rest_trimmed[..end])?;
            rest = &rest_trimmed[end..];
        }
    }
    Some(current)
}

/// All fields of a JSON object line, or only the requested ones.
pub fn json_extract(line: &str, wanted: &[Extraction]) -> Result<Vec<(String, String)>, String> {
    let doc: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
    if !doc.is_object() {
        return Err("the line is not a JSON object".to_string());
    }
    if wanted.is_empty() {
        let mut out = Vec::new();
        flatten("", &doc, &mut out);
        return Ok(out);
    }
    Ok(wanted
        .iter()
        .filter_map(|w| {
            let path = w.expr.as_deref().unwrap_or(&w.label);
            Some((w.label.clone(), text(lookup(&doc, path)?)))
        })
        .collect())
}

/// `key=value key2="quoted value" flag`.
pub fn logfmt_extract(line: &str, wanted: &[Extraction]) -> Result<Vec<(String, String)>, String> {
    let chars: Vec<char> = line.chars().collect();
    let mut out: Vec<(String, String)> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '=' {
            i += 1;
        }
        if start == i {
            if i < chars.len() && chars[i] == '=' {
                return Err("unexpected '='".to_string());
            }
            continue;
        }
        let key: String = chars[start..i].iter().collect();
        let mut value = String::new();
        if i < chars.len() && chars[i] == '=' {
            i += 1;
            if i < chars.len() && chars[i] == '"' {
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("unterminated quoted value".to_string()),
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some('\\') if i + 1 < chars.len() => {
                            value.push(match chars[i + 1] {
                                'n' => '\n',
                                't' => '\t',
                                other => other,
                            });
                            i += 2;
                        }
                        Some(c) => {
                            value.push(*c);
                            i += 1;
                        }
                    }
                }
            } else {
                while i < chars.len() && !chars[i].is_whitespace() {
                    value.push(chars[i]);
                    i += 1;
                }
            }
        }
        let key = sanitize_label_name(&key);
        if !out.iter().any(|(k, _)| *k == key) {
            out.push((key, value));
        }
    }
    if wanted.is_empty() {
        return Ok(out);
    }
    Ok(wanted
        .iter()
        .filter_map(|w| {
            let source = w.expr.as_deref().unwrap_or(&w.label);
            let source = sanitize_label_name(source);
            out.iter()
                .find(|(k, _)| *k == source)
                .map(|(_, v)| (w.label.clone(), v.clone()))
        })
        .collect())
}

/// The labels of a packed line, and the original line when it carried one.
pub type Unpacked = (Vec<(String, String)>, Option<String>);

/// Packed lines (`{"_entry": "...", "k": "v"}`): the labels, and the original line.
pub fn unpack(line: &str) -> Result<Unpacked, String> {
    let Value::Object(map) = serde_json::from_str::<Value>(line).map_err(|e| e.to_string())? else {
        return Err("the line is not a JSON object".to_string());
    };
    let mut entry = None;
    let mut labels = Vec::new();
    for (key, value) in map {
        match (key.as_str(), &value) {
            ("_entry", Value::String(s)) => entry = Some(s.clone()),
            ("__stream__", _) => {}
            (_, Value::String(s)) => labels.push((sanitize_label_name(&key), s.clone())),
            _ => {}
        }
    }
    Ok((labels, entry))
}

#[derive(Clone, Debug)]
enum Piece {
    Literal(String),
    Capture(Option<String>),
}

/// `<ip> - <user> "<_>"`: literals must appear in order; `<name>` captures up to the next
/// literal and `<_>` skips.
#[derive(Clone, Debug)]
pub struct PatternMatcher {
    pieces: Vec<Piece>,
}

impl PatternMatcher {
    pub fn compile(pattern: &str) -> Result<Self, String> {
        let mut pieces = Vec::new();
        let mut rest = pattern;
        while !rest.is_empty() {
            match rest.find('<') {
                Some(open) => {
                    if open > 0 {
                        pieces.push(Piece::Literal(rest[..open].to_string()));
                    }
                    let close = rest[open..].find('>').ok_or("unterminated <capture>")? + open;
                    let name = &rest[open + 1..close];
                    pieces.push(Piece::Capture((name != "_").then(|| name.to_string())));
                    rest = &rest[close + 1..];
                }
                None => {
                    pieces.push(Piece::Literal(rest.to_string()));
                    break;
                }
            }
        }
        if !pieces.iter().any(|p| matches!(p, Piece::Capture(_))) && pieces.len() != 1 {
            return Err("a pattern needs at least one capture".to_string());
        }
        if pieces
            .windows(2)
            .any(|w| matches!((&w[0], &w[1]), (Piece::Capture(_), Piece::Capture(_))))
        {
            return Err("two captures in a row are ambiguous".to_string());
        }
        Ok(Self { pieces })
    }

    /// The captures, or None when the line does not fit.
    pub fn captures(&self, line: &str) -> Option<Vec<(String, String)>> {
        let mut out = Vec::new();
        let mut pos = 0;
        let mut pending: Option<Option<String>> = None;
        for (index, piece) in self.pieces.iter().enumerate() {
            match piece {
                Piece::Capture(name) => pending = Some(name.clone()),
                Piece::Literal(lit) => {
                    let at = if index == 0 {
                        line.starts_with(lit.as_str()).then_some(0)?
                    } else {
                        line[pos..].find(lit.as_str())? + pos
                    };
                    if let Some(Some(name)) = pending.take() {
                        out.push((name, line[pos..at].to_string()));
                    }
                    pos = at + lit.len();
                }
            }
        }
        if let Some(Some(name)) = pending {
            out.push((name, line[pos..].to_string()));
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wanted(label: &str, expr: Option<&str>) -> Extraction {
        Extraction {
            label: label.into(),
            expr: expr.map(str::to_string),
        }
    }

    #[test]
    fn json_flattens_and_extracts_by_path() {
        let line = r#"{"method":"GET","status":200,"req":{"id":"a-1","tags":["x"]},"took":null}"#;
        let all = json_extract(line, &[]).unwrap();
        assert!(all.contains(&("method".into(), "GET".into())));
        assert!(all.contains(&("status".into(), "200".into())));
        assert!(all.contains(&("req_id".into(), "a-1".into())));
        assert!(all.contains(&("req_tags".into(), r#"["x"]"#.into())));
        let some = json_extract(
            line,
            &[
                wanted("m", Some("method")),
                wanted("rid", Some("req.id")),
                wanted("t0", Some(r#"req["tags"][0]"#)),
            ],
        )
        .unwrap();
        assert_eq!(
            some,
            [
                ("m".into(), "GET".into()),
                ("rid".into(), "a-1".into()),
                ("t0".into(), "x".into())
            ]
        );
        assert!(json_extract("not json", &[]).is_err());
    }

    #[test]
    fn logfmt_reads_quoted_values_and_bare_keys() {
        let got = logfmt_extract(r#"level=info msg="hello world" ok took=5ms"#, &[]).unwrap();
        assert_eq!(got[1], ("msg".into(), "hello world".into()));
        assert_eq!(got[2], ("ok".into(), String::new()));
        assert!(logfmt_extract(r#"a="open"#, &[]).is_err());
    }

    #[test]
    fn patterns_capture_between_literals() {
        let p = PatternMatcher::compile(r#"<ip> - <user> "<method> <_>""#).unwrap();
        let got = p.captures(r#"10.0.0.1 - bob "GET /x""#).unwrap();
        assert_eq!(
            got,
            [
                ("ip".into(), "10.0.0.1".into()),
                ("user".into(), "bob".into()),
                ("method".into(), "GET".into())
            ]
        );
        assert!(p.captures("nothing like it").is_none());
        let p = PatternMatcher::compile(r#"<_>"method":"<method>"<_>"#).unwrap();
        assert_eq!(
            p.captures(r#"{"method":"HEAD","x":1}"#).unwrap(),
            [("method".into(), "HEAD".into())]
        );
        assert!(PatternMatcher::compile("<a><b>").is_err());
    }
}
