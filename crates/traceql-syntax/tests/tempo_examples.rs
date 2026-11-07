//! Tempo's own TraceQL example queries (tests/test_examples.yaml, copied from Tempo): every query
//! must land in the category Tempo puts it in.

use traceql_syntax::{parse, validate, ValidateError};
use yaml_rust2::YamlLoader;

fn examples(section: &str) -> Vec<String> {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_examples.yaml")).unwrap();
    let docs = YamlLoader::load_from_str(&text).unwrap();
    docs[0][section]
        .as_vec()
        .unwrap_or_else(|| panic!("no section {section}"))
        .iter()
        .map(|y| y.as_str().expect("a string").to_string())
        .collect()
}

fn check(section: &str, verdict: impl Fn(&str) -> Result<(), String>) {
    let list = examples(section);
    assert!(list.len() > 20, "the {section} section is suspiciously small: {}", list.len());
    eprintln!("{section}: {} queries", list.len());
    let failures: Vec<String> = list
        .iter()
        .filter_map(|q| verdict(q).err().map(|why| format!("  {q}\n      {why}")))
        .collect();
    assert!(failures.is_empty(), "{} of the {section} examples are classified differently:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn valid_queries_parse_and_validate() {
    check("valid", |q| {
        let query = parse(q).map_err(|e| format!("does not parse: {e}"))?;
        validate(&query).map_err(|e| format!("does not validate: {e}"))
    });
}

#[test]
fn queries_that_must_not_parse_do_not() {
    check("parse_fails", |q| match parse(q) {
        Err(_) => Ok(()),
        Ok(_) => Err("parses".to_string()),
    });
}

#[test]
fn queries_that_parse_but_are_invalid_fail_validation() {
    check("validate_fails", |q| {
        let query = parse(q).map_err(|e| format!("does not parse: {e}"))?;
        match validate(&query) {
            Err(ValidateError::Invalid(_)) => Ok(()),
            Err(ValidateError::Unsupported(m)) => Err(format!("reported as unsupported: {m}")),
            Ok(()) => Err("validates".to_string()),
        }
    });
}

#[test]
fn queries_using_unsupported_features_are_reported_as_unsupported() {
    check("unsupported", |q| {
        let query = parse(q).map_err(|e| format!("does not parse: {e}"))?;
        match validate(&query) {
            Err(ValidateError::Unsupported(_)) => Ok(()),
            Err(ValidateError::Invalid(m)) => Err(format!("reported as invalid: {m}")),
            Ok(()) => Err("validates".to_string()),
        }
    });
}

/// The harness itself: a query in the wrong category must be reported.
#[test]
fn the_checks_catch_a_misclassified_query() {
    let wrong_valid = parse("{ true } + { true }").is_ok();
    assert!(!wrong_valid, "a query that must not parse parsed");
    let q = parse("{ 1 + 1 }").unwrap();
    assert!(matches!(validate(&q), Err(ValidateError::Invalid(_))));
    let q = parse("{ parent.a = 1 }").unwrap();
    assert!(matches!(validate(&q), Err(ValidateError::Unsupported(_))));
}
