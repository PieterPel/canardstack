# logql-syntax

A parser for [LogQL](https://grafana.com/docs/loki/latest/query/), Grafana Loki's query language.
It produces a syntax tree and nothing more: no evaluation, no storage, and no dependencies.

```rust
use logql_syntax::{ast::Expr, parse};

let expr = parse(r#"sum by (level) (rate({app="api"} |= "error" | json [5m]))"#)?;
assert!(matches!(expr, Expr::Vector(_)));
```

## What it parses

- Stream selectors with `=`, `!=`, `=~`, `!~`.
- Line filters `|=`, `!=`, `|~`, `!~`, `|>`, `!>`, with `or` alternatives and `ip("...")`.
- Stages: `json`, `logfmt`, `regexp`, `pattern`, `unpack`, `line_format`, `label_format`,
  `decolorize`, `drop`, `keep`, and typed label filters (strings, numbers, durations such as `1h30m`,
  bytes such as `10KB`, `ip()`) combined with `and`, `or`, `,` and parentheses with Loki's precedence.
- Range aggregations (`rate`, `count_over_time`, `bytes_rate`, `quantile_over_time`, `*_over_time`,
  `absent_over_time`, ...) with `unwrap` (including `bytes()`, `duration()`), `offset` and grouping.
- Vector aggregations (`sum`, `avg`, `topk`, `sort_desc`, ...) with `by`/`without` before or after.
- Binary operators with `bool`, `on`/`ignoring` and `group_left`/`group_right`; `label_replace`,
  `vector()`, number literals and `#` comments.

Errors carry the byte position of the problem ([`ParseError`]).

## What it does not do

It does not check that a function is applied to a sensible argument beyond the grammar (for example
that a regex compiles), and it keeps `line_format` / `label_format` templates as the strings they are.
It started inside [canardstack](https://github.com/smithclay/canardstack), an observability query
server, and is tested against the queries that server answers.

## License

Apache-2.0.
