use crate::validation::MULTI_VALUE_SEPARATOR;
use std::collections::HashMap;

/// Query parameters. A repeated key ending in `[]` keeps all its values, joined with
/// [`MULTI_VALUE_SEPARATOR`]; for other keys the last value wins.
pub(super) fn query_params(query: &str) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match out.get_mut(key.as_ref()) {
            Some(existing) if key.ends_with("[]") => {
                existing.push(MULTI_VALUE_SEPARATOR);
                existing.push_str(&value);
            }
            _ => {
                out.insert(key.into_owned(), value.into_owned());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_bracket_keys_keep_every_value() {
        let p = query_params("match%5B%5D=a&match%5B%5D=b&x=1&x=2");
        assert_eq!(p["match[]"], "a\u{1f}b");
        assert_eq!(p["x"], "2");
    }
}
