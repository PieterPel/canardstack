use crate::query::plan::{
    normalize_matchers, parse_selector_matchers, LogPlan, SelectorPlan, SortDirection,
    TextFilter, TimeBounds,
};
use crate::semantic_labels::{self, LabelScope};
use crate::validation::{ApiError, ApiResult};

pub fn parse_loki_query(
    raw: &str,
    time_bounds: TimeBounds,
    limit: usize,
    direction: &str,
) -> ApiResult<LogPlan> {
    let (selector, pipeline) = split_selector(raw);
    let (text_filters, line_format) = parse_pipeline(pipeline)?;
    let (_, matchers) = parse_selector_matchers(selector, "unsupported_selector")?;
    let aliases = semantic_labels::alias_pairs(LabelScope::Logs);
    let matchers = normalize_matchers(matchers, &aliases, "unsupported_selector")?;
    Ok(LogPlan {
        selector: SelectorPlan {
            resource: None,
            matchers,
            text_filters,
        },
        time_bounds,
        limit,
        direction: SortDirection::from_loki(direction),
        stream_labels: semantic_labels::loki_stream_labels(),
        line_format,
    })
}

/// Splits `{matchers} | stage | stage` at the closing brace of the stream selector.
pub(crate) fn split_selector(raw: &str) -> (&str, &str) {
    let raw = raw.trim();
    if !raw.starts_with('{') {
        return (raw, "");
    }
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in raw.char_indices() {
        match quote {
            Some(_) if escaped => escaped = false,
            Some(q) if c == '\\' && q == '"' => escaped = true,
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if matches!(c, '"' | '`') => quote = Some(c),
            None if c == '}' => return (&raw[..=i], raw[i + 1..].trim()),
            None => {}
        }
    }
    (raw, "")
}

/// Parses the line filters (`|=`, `!=`, `|~`, `!~`) and a constant `line_format` after a selector.
pub(crate) fn parse_pipeline(mut rest: &str) -> ApiResult<(Vec<TextFilter>, Option<String>)> {
    let mut filters = Vec::new();
    let mut line_format = None;
    rest = rest.trim_start();
    while !rest.is_empty() {
        let (kind, after) = if let Some(after) = rest.strip_prefix("| line_format") {
            ("line_format", after)
        } else if let Some(after) = rest.strip_prefix("|=") {
            ("|=", after)
        } else if let Some(after) = rest.strip_prefix("|~") {
            ("|~", after)
        } else if let Some(after) = rest.strip_prefix("!=") {
            ("!=", after)
        } else if let Some(after) = rest.strip_prefix("!~") {
            ("!~", after)
        } else {
            return Err(ApiError::new(
                400,
                "unsupported_logql",
                "supported pipeline stages are |=, !=, |~, !~ and | line_format \"text\"",
            ));
        };
        let (term, after) = parse_quoted(after.trim_start())?;
        rest = after.trim_start();
        match kind {
            "|=" => filters.push(TextFilter::BodyContains(term)),
            "!=" => filters.push(TextFilter::BodyNotContains(term)),
            "|~" => filters.push(TextFilter::BodyRegex(term)),
            "!~" => filters.push(TextFilter::BodyNotRegex(term)),
            _ if term.contains("{{") => {
                return Err(ApiError::new(
                    400,
                    "unsupported_logql",
                    "line_format templates are not supported, only constant text",
                ))
            }
            _ => line_format = Some(term),
        }
    }
    Ok((filters, line_format))
}

/// Reads one `"..."` or backtick string and returns it with the text after it.
fn parse_quoted(raw: &str) -> ApiResult<(String, &str)> {
    let bad = || ApiError::new(400, "invalid_selector", "expected a quoted string");
    let mut chars = raw.char_indices();
    let (_, open) = chars.next().ok_or_else(bad)?;
    if !matches!(open, '"' | '`') {
        return Err(bad());
    }
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        if c == open {
            return Ok((out, &raw[i + 1..]));
        }
        if c == '\\' && open == '"' {
            match chars.next().ok_or_else(bad)?.1 {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                '\\' => out.push('\\'),
                '"' => out.push('"'),
                other => {
                    out.push('\\');
                    out.push(other);
                }
            }
        } else {
            out.push(c);
        }
    }
    Err(bad())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bounds() -> TimeBounds {
        TimeBounds {
            from: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(1970, 1, 1, 1, 0, 0).unwrap(),
        }
    }

    #[test]
    fn loki_selector_aliases_normalize_to_promoted_fields() {
        let plan = parse_loki_query(
            r#"{service.name="checkout",http.route="/smoke"} |= "timeout""#,
            bounds(),
            100,
            "forward",
        )
        .unwrap();

        assert_eq!(
            plan.selector.matcher_value("service_name"),
            Some("checkout")
        );
        assert_eq!(plan.selector.matcher_value("http_route"), Some("/smoke"));
        assert!(matches!(
            plan.selector.text_filters.as_slice(),
            [TextFilter::BodyContains(term)] if term == "timeout"
        ));
        assert_eq!(plan.direction, SortDirection::Forward);
    }

    #[test]
    fn loki_selector_conflicting_aliases_fail_closed() {
        let err = parse_loki_query(
            r#"{service.name="checkout",service_name="payments"}"#,
            bounds(),
            100,
            "backward",
        )
        .unwrap_err();

        assert_eq!(err.reason, "unsupported_selector");
    }

    #[test]
    fn loki_selector_supports_regex_and_resource_aliases() {
        let plan = parse_loki_query(
            r#"{resource.service.name=~"front.*",resource.service.namespace="demo",severity.text!="DEBUG"} |~ "GET|POST""#,
            bounds(),
            100,
            "backward",
        )
        .unwrap();

        assert_eq!(
            plan.selector.matcher_value("service_namespace"),
            Some("demo")
        );
        assert!(matches!(
            plan.selector.text_filters.as_slice(),
            [TextFilter::BodyRegex(term)] if term == "GET|POST"
        ));
    }

    #[test]
    fn loki_selector_keeps_commas_inside_quoted_regex_matchers() {
        let plan = parse_loki_query(
            r#"{service.name=~"frontend,checkout",severity.text!="DEBUG"}"#,
            bounds(),
            100,
            "backward",
        )
        .unwrap();

        let matcher = plan
            .selector
            .matchers
            .iter()
            .find(|matcher| matcher.field == "service_name")
            .unwrap();
        assert_eq!(matcher.value, "frontend,checkout");
        assert_eq!(matcher.op, crate::query::plan::MatchOp::Regex);
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    #[test]
    fn chained_filters_and_negations_parse_in_order() {
        let (selector, rest) = split_selector(r#"{a="}"} |= "x" != "y" |~ `z\d` !~ "w""#);
        assert_eq!(selector, r#"{a="}"}"#);
        let (filters, format) = parse_pipeline(rest).unwrap();
        assert_eq!(
            filters,
            vec![
                TextFilter::BodyContains("x".into()),
                TextFilter::BodyNotContains("y".into()),
                TextFilter::BodyRegex(r"z\d".into()),
                TextFilter::BodyNotRegex("w".into()),
            ]
        );
        assert_eq!(format, None);
    }

    #[test]
    fn constant_line_format_is_kept_and_templates_are_rejected() {
        assert_eq!(parse_pipeline(r#"| line_format "L""#).unwrap().1.as_deref(), Some("L"));
        assert_eq!(
            parse_pipeline(r#"| line_format "{{.a}}""#).unwrap_err().reason,
            "unsupported_logql"
        );
        assert_eq!(parse_pipeline("| json").unwrap_err().reason, "unsupported_logql");
    }
}
