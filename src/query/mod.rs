pub mod log;
pub mod logmetric;
pub mod logql;
pub mod plan;
pub mod prom_discovery;
pub mod prom_labels;
pub mod prom_names;
pub mod promql;
pub mod trace;
pub mod vector;

use crate::config::Config;
use crate::db::sql::{quote as sql_quote, time_predicate};
use crate::metrics::Timer;
use crate::query::plan::{
    FieldMatcher, LogPlan, MatchOp, TextFilter,
    TracePlan, TraceSort,
};
use crate::semantic_labels::{self, LabelScope};
use crate::signal::StorageSignal;
use crate::storage::QueryExecution;
use crate::storage::{QueryTimeoutError, Storage};
use crate::validation::{ApiError, ApiResult};
use duckdb::Row;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct QueryEngine {
    interactive_timeout: Duration,
    interactive_memory_limit: String,
    execution: Option<Arc<QueryExecution>>,
    prom_name_suffixes: bool,
}

impl QueryEngine {
    pub fn new(config: &Config) -> Self {
        Self {
            interactive_timeout: Duration::from_secs(
                config.operator.query_interactive.timeout_secs,
            ),
            interactive_memory_limit: config.operator.query_interactive.memory_limit.clone(),
            execution: None,
            prom_name_suffixes: config.operator.prometheus_name_suffixes,
        }
    }

    /// Whether Prometheus metric names carry unit and `_total` suffixes (see `prom_names`).
    pub fn prom_name_suffixes(&self) -> bool {
        self.prom_name_suffixes
    }

    pub(crate) fn with_execution(&self, execution: Arc<QueryExecution>) -> Self {
        Self {
            execution: Some(execution),
            ..self.clone()
        }
    }

    pub fn health(&self) -> Value {
        json!({
            "interactive_timeout_seconds": self.interactive_timeout.as_secs(),
            "memory_limit_interactive": self.interactive_memory_limit
        })
    }

    pub fn run_interactive<T>(
        &self,
        storage: &Storage,
        f: impl FnMut(&duckdb::Connection, &str) -> anyhow::Result<T>,
    ) -> ApiResult<T> {
        storage
            .with_query_execution(
                &self.interactive_memory_limit,
                self.interactive_timeout,
                self.execution.as_deref(),
                f,
            )
            .map_err(storage_err)
    }

    pub fn execute_logs(&self, storage: &Storage, plan: &LogPlan) -> ApiResult<Value> {
        let timer = Timer::start();
        let where_sql = log_where_sql(plan)?;
        let projected = semantic_labels::projected_labels(LabelScope::Logs);
        let sql = log_select_sql(
            &format!("{{prefix}}{}", StorageSignal::Logs.as_str()),
            &projected,
            &where_sql,
            plan.direction.sql(),
            plan.limit + 1,
        );
        let rows = storage
            .with_query_execution(
                &self.interactive_memory_limit,
                self.interactive_timeout,
                self.execution.as_deref(),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let mapped = stmt.query_map([], |row| log_row(row, &projected))?;
                    Ok(collect_rows(mapped)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, plan.limit, timer.elapsed_ms()))
    }

    pub fn execute_trace_search(&self, storage: &Storage, plan: &TracePlan) -> ApiResult<Value> {
        let TracePlan {
            selector,
            time_bounds,
            limit,
            sort,
            durations,
        } = plan;
        let timer = Timer::start();
        let mut where_sql = vec![time_predicate(
            "start_time_unix_nano",
            time_bounds.from,
            time_bounds.to,
        )];
        for matcher in &selector.matchers {
            push_matcher(
                &mut where_sql,
                matcher,
                LabelScope::Spans,
                "unsupported_selector",
            )?;
        }
        for d in durations {
            where_sql.push(format!("duration_time_unix_nano {} {}", d.op, d.nanos));
        }
        let order = match sort {
            TraceSort::DurationDesc => "7 DESC",
            TraceSort::TimestampDesc => "3 DESC",
        };
        // duckdb-otlp: `timestamp` → `start_time_unix_nano`; `span_name` →
        // `name`; `duration` → `duration_time_unix_nano` (BIGINT nanoseconds,
        // downstream `tempo_*` converts to ms).
        let sql = format!(
            "SELECT lower(trace_id), min(start_time_unix_nano)::VARCHAR, max(start_time_unix_nano)::VARCHAR, max(service_name), max(name), count(*), max(duration_time_unix_nano) FROM {{prefix}}{} WHERE {} GROUP BY trace_id ORDER BY {order} LIMIT {}",
            StorageSignal::Spans.as_str(),
            where_sql.join(" AND "),
            limit + 1
        );
        let rows = storage
            .with_query_execution(
                &self.interactive_memory_limit,
                self.interactive_timeout,
                self.execution.as_deref(),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let rows = stmt.query_map([], |row| {
                        Ok(json!({
                            "trace_id": row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            "start_time": row.get::<_, Option<String>>(1)?,
                            "end_time": row.get::<_, Option<String>>(2)?,
                            "service_name": row.get::<_, Option<String>>(3)?,
                            "span_name": row.get::<_, Option<String>>(4)?,
                            "matched_spans": row.get::<_, i64>(5)?,
                            "duration": row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                        }))
                    })?;
                    Ok(collect_rows(rows)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, *limit, timer.elapsed_ms()))
    }

}

fn log_row(row: &Row<'_>, projected: &[(&str, String)]) -> duckdb::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "timestamp".to_string(),
        json!(row.get::<_, String>("timestamp")?),
    );
    obj.insert(
        "body".to_string(),
        json!(row.get::<_, Option<String>>("body")?),
    );
    for (name, _) in projected {
        obj.insert(
            (*name).to_string(),
            json!(row.get::<_, Option<String>>(*name)?),
        );
    }
    Ok(Value::Object(obj))
}

fn log_where_sql(plan: &LogPlan) -> ApiResult<Vec<String>> {
    let mut where_sql = vec![time_predicate(
        "time_unix_nano",
        plan.time_bounds.from,
        plan.time_bounds.to,
    )];
    for matcher in &plan.selector.matchers {
        push_matcher(
            &mut where_sql,
            matcher,
            LabelScope::Logs,
            "unsupported_selector",
        )?;
    }
    for filter in &plan.selector.text_filters {
        match filter {
            TextFilter::BodyContains(text) => where_sql.push(body_contains(text)),
            TextFilter::BodyRegex(pattern) => where_sql.push(format!(
                "regexp_matches(coalesce(body, ''), {})",
                sql_quote(pattern)
            )),
            TextFilter::BodyNotContains(text) => {
                where_sql.push(format!("NOT {}", body_contains(text)))
            }
            TextFilter::BodyNotRegex(pattern) => where_sql.push(format!(
                "NOT regexp_matches(coalesce(body, ''), {})",
                sql_quote(pattern)
            )),
        }
    }
    Ok(where_sql)
}

fn log_select_sql(
    source: &str,
    projected: &[(&str, String)],
    where_sql: &[String],
    direction: &str,
    limit: usize,
) -> String {
    let mut columns = vec![
        // Alias the `time_unix_nano` storage column back to the stable
        // `timestamp` output column the log row reader pulls by name.
        "time_unix_nano::VARCHAR AS timestamp".to_string(),
        "body".to_string(),
    ];
    // Project every registry label as VARCHAR so the row reader can pull each
    // column back by name with one uniform type; NULLs are preserved.
    for (name, expr) in projected {
        columns.push(format!("cast({expr} AS VARCHAR) AS {name}"));
    }
    // Pick the page of rows first on raw columns, then evaluate the projection (JSON
    // extraction, lower(), casts) on only those rows. Projecting inline would evaluate
    // every label expression for every row that matches the filter before the LIMIT.
    format!(
        "SELECT {} FROM (SELECT * FROM {source} WHERE {} ORDER BY time_unix_nano {direction} LIMIT {limit}) ORDER BY time_unix_nano {direction}",
        columns.join(", "),
        where_sql.join(" AND ")
    )
}

fn collect_rows(
    iter: duckdb::MappedRows<'_, impl FnMut(&Row<'_>) -> duckdb::Result<Value>>,
) -> Result<Vec<Value>, duckdb::Error> {
    iter.collect()
}

fn wrap_rows(mut rows: Vec<Value>, limit: usize, query_duration_ms: u128) -> Value {
    let truncated = rows.len() > limit;
    rows.truncate(limit);
    json!({
        "rows": rows,
        "next_cursor": null,
        "applied_limit": limit,
        "truncated": truncated,
        "query_duration_ms": query_duration_ms
    })
}

fn push_matcher(
    where_sql: &mut Vec<String>,
    matcher: &FieldMatcher,
    scope: LabelScope,
    reason: &'static str,
) -> ApiResult<()> {
    let expr = semantic_labels::label_expr(scope, &matcher.field).ok_or_else(|| {
        ApiError::new(
            400,
            reason,
            format!("unsupported label {} in v0 selector", matcher.field),
        )
    })?;
    let value_expr = format!("coalesce(cast({expr} AS VARCHAR), '')");
    match matcher.op {
        MatchOp::Eq => where_sql.push(format!("{value_expr} = {}", sql_quote(&matcher.value))),
        MatchOp::NotEq => where_sql.push(format!("{value_expr} != {}", sql_quote(&matcher.value))),
        MatchOp::Regex if matcher.value == ".*" => {}
        MatchOp::Regex => where_sql.push(format!(
            "regexp_matches({value_expr}, {})",
            sql_quote(&matcher.value)
        )),
        MatchOp::NotRegex if matcher.value == ".*" => where_sql.push("FALSE".to_string()),
        MatchOp::NotRegex => where_sql.push(format!(
            "NOT regexp_matches({value_expr}, {})",
            sql_quote(&matcher.value)
        )),
    }
    Ok(())
}

/// LogQL `|=`: a literal, case-sensitive substring match.
fn body_contains(text: &str) -> String {
    format!("contains(coalesce(body, ''), {})", sql_quote(text))
}

fn storage_err(err: anyhow::Error) -> ApiError {
    // Use a typed downcast rather than substring-matching on the error
    // message — that was a brittle contract between this layer and storage.rs.
    if err.downcast_ref::<QueryTimeoutError>().is_some() {
        return ApiError::new(503, "query_timeout", err.to_string());
    }
    ApiError::new(503, "query_storage_unavailable", err.to_string())
}
