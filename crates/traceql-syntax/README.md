# traceql-syntax

A parser and validator for [TraceQL](https://grafana.com/docs/tempo/latest/traceql/), Grafana
Tempo's query language for traces. It produces a syntax tree and applies Tempo's type rules; it does
not evaluate anything, store anything, or depend on any other crate.

```rust
use traceql_syntax::{parse, validate, ast::QueryKind};

let query = parse(r#"{ resource.service.name = "api" && duration > 1s } | count() > 3"#)?;
assert!(matches!(query.kind, QueryKind::Pipeline(_)));
validate(&query)?;                       // type rules, regexes, unsupported features

assert!(parse("{ .a < }").is_err());     // syntax error with a byte position
```

## What it covers

Span filters with attributes (`.a`, `span.a`, `resource.a`, `parent.span.a`, quoted names),
intrinsics (`duration`, `name`, `status`, `kind`, and the scoped `span:duration`, `trace:rootName`,
...), arithmetic, comparison and regex operators, `nil` checks; spanset operators including the
structural ones (`>`, `>>`, `<`, `<<`, `~` and their negated and union forms); pipelines with
scalar filters over aggregates (`count()`, `avg(duration) > 1s`), `by()`, `coalesce()`, `select()`;
and the metrics functions (`rate`, `count_over_time`, the `*_over_time` family, `quantile_over_time`,
`histogram_over_time`, `compare`), `topk` / `bottomk`, filters on metrics results, arithmetic
between metrics queries, and `with(...)` hints.

`validate` reports two kinds of problem, as Tempo does: `Invalid` (a type error, an illegal
argument, a bad regex) and `Unsupported` (well formed, but a feature Tempo has not implemented).
The built-in regex check catches syntax errors only; pass your regex engine to `validate_with` for
the real thing.

## Tested against Tempo's examples

`tests/test_examples.yaml` is copied from Tempo (`pkg/traceql/test_examples.yaml`, Apache-2.0). Each
of its 463 queries must land where Tempo puts it: 284 valid, 87 that do not parse, 70 that parse but
fail validation, 22 that are unsupported.

## License

Apache-2.0.
