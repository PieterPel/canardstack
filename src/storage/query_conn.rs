use super::ducklake::quote_ident;
use super::{QueryTimeoutError, Storage};
use crate::db::sql::escape_value;
use crate::LockExt;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub(crate) struct QueryExecution {
    deadline: Instant,
    budget: Duration,
    cancelled: AtomicBool,
    interrupt: Mutex<Option<Arc<duckdb::InterruptHandle>>>,
}

impl QueryExecution {
    pub(crate) fn new(budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + budget,
            budget,
            cancelled: AtomicBool::new(false),
            interrupt: Mutex::new(None),
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(interrupt) = self.interrupt.lock_or_poisoned().as_ref() {
            interrupt.interrupt();
        }
    }

    fn remaining(&self) -> Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if self.cancelled.load(Ordering::SeqCst) || remaining.is_zero() {
            return Err(anyhow::Error::new(QueryTimeoutError {
                timeout: self.budget,
            }));
        }
        Ok(remaining)
    }
}

struct ActiveInterrupt<'a>(&'a QueryExecution);

impl Drop for ActiveInterrupt<'_> {
    fn drop(&mut self) {
        *self.0.interrupt.lock_or_poisoned() = None;
    }
}

/// DuckLake reports this when its cached catalog still references inlined-data
/// tables that a writer flushed away.
fn is_stale_catalog_error(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}");
    message.contains("Failed to read inlined data from DuckLake")
        && message.contains("does not exist")
}

impl Storage {
    /// Read-side connection access for SELECT-only paths. Do not call into
    /// write paths from inside the closure.
    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection, &str) -> Result<T>) -> Result<T> {
        let conn = self.reader.lock_or_poisoned();
        f(&conn, &self.target_prefix)
    }

    pub fn with_query_conn<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        f: impl FnMut(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        self.with_query_execution(memory_limit, timeout, None, f)
    }

    pub(crate) fn with_query_execution<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        execution: Option<&QueryExecution>,
        mut f: impl FnMut(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        let result = self.run_query_once(memory_limit, timeout, execution, &mut f);
        match result {
            Err(err) if is_stale_catalog_error(&err) => {
                // An external writer flushed or dropped inlined-data tables that the
                // attached catalog still references. Re-attach and retry once.
                tracing::warn!(event = "ducklake_stale_catalog_reattach", error = %err);
                self.reattach_catalog()?;
                self.run_query_once(memory_limit, timeout, execution, &mut f)
            }
            result => result,
        }
    }

    /// Replace the attached DuckLake catalog so the next query rebuilds its
    /// cached view of the catalog from the metadata store.
    fn reattach_catalog(&self) -> Result<()> {
        let conn = self.reader.lock_or_poisoned();
        conn.execute_batch(&format!(
            "USE {}; DETACH {};",
            quote_ident(&self.home_catalog),
            self.catalog_name
        ))
        .context("detach stale DuckLake catalog")?;
        conn.execute_batch(&self.reattach_sql)
            .context("re-attach DuckLake catalog")?;
        self.metadata_generation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn run_query_once<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        execution: Option<&QueryExecution>,
        f: &mut impl FnMut(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        if let Some(execution) = execution {
            execution.remaining()?;
        }
        let conn = self.open_scoped_query_connection()?;
        let interrupt = conn.interrupt_handle();
        let _active = execution.map(|execution| {
            *execution.interrupt.lock_or_poisoned() = Some(interrupt.clone());
            ActiveInterrupt(execution)
        });
        let timeout = match execution {
            Some(execution) => timeout.min(execution.remaining()?),
            None => timeout,
        };
        {
            let mut applied = self.applied_memory_limit.lock_or_poisoned();
            if applied.as_deref() != Some(memory_limit) {
                conn.execute_batch(&format!(
                    "SET memory_limit = '{}';",
                    escape_value(memory_limit)
                ))?;
                *applied = Some(memory_limit.to_string());
            }
        }

        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let timer_state = state.clone();
        let timer = thread::spawn(move || {
            let (done, cvar) = &*timer_state;
            let done = done.lock_or_poisoned();
            if *done {
                return false;
            }
            let (done, wait) = cvar
                .wait_timeout_while(done, timeout, |done| !*done)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !*done && wait.timed_out() {
                interrupt.interrupt();
                return true;
            }
            false
        });

        // One read transaction per query: prepare binds and execute runs inside it, so
        // DuckLake fetches the catalog snapshot once. With auto-commit, each statement
        // is its own transaction and every query pays for the catalog lookups twice.
        conn.execute_batch("BEGIN TRANSACTION READ ONLY")?;
        let result = f(&conn, &self.target_prefix);
        // Read-only: rolling back and committing are equivalent. Best effort, because the
        // connection is dropped right after and a failed query may have aborted it already.
        let _ = conn.execute_batch("ROLLBACK");
        let (done, cvar) = &*state;
        *done.lock_or_poisoned() = true;
        cvar.notify_one();
        // Timer panic means "not timed out": trust the query result over a panicked timer.
        let timed_out = match timer.join() {
            Ok(fired) => fired,
            Err(_) => {
                tracing::warn!(event = "query_timer_panicked");
                false
            }
        };
        // Timer fired: surface a typed timeout even if the query raced to Ok.
        if timed_out {
            return Err(anyhow::Error::new(QueryTimeoutError { timeout }));
        }
        if let Some(execution) = execution {
            execution.remaining()?;
        }
        result
    }

    fn open_scoped_query_connection(&self) -> Result<Connection> {
        // Attached schemas, extensions and the database-wide settings from
        // `configure_base_connection` are inherited; do not SET them again here.
        let parent = self.reader.lock_or_poisoned();
        parent.try_clone().context("clone DuckDB connection")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_closure_runs_in_one_read_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(&crate::Config::test(dir.path().join("query.duckdb"))).unwrap();
        let (first, second) = storage
            .with_query_conn("128MiB", Duration::from_secs(5), |conn, _| {
                let txid = |conn: &Connection| {
                    conn.query_row("SELECT txid_current()", [], |row| row.get::<_, i64>(0))
                };
                Ok((txid(conn)?, txid(conn)?))
            })
            .unwrap();
        assert_eq!(
            first, second,
            "statements in one query must share a transaction"
        );
    }

    #[test]
    fn query_connections_disable_late_materialization() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(&crate::Config::test(dir.path().join("query.duckdb"))).unwrap();
        let setting = storage
            .with_query_conn("128MiB", Duration::from_secs(5), |conn, _| {
                conn.query_row("SELECT current_setting('disabled_optimizers')", [], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(Into::into)
            })
            .unwrap();
        assert!(setting.contains("late_materialization"), "{setting}");
    }

    #[test]
    fn request_cancellation_interrupts_active_duckdb_work() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(&crate::Config::test(dir.path().join("query.duckdb"))).unwrap();
        let execution = Arc::new(QueryExecution::new(Duration::from_secs(5)));
        let worker_execution = execution.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            storage.with_query_execution(
                "128MiB",
                Duration::from_secs(5),
                Some(&worker_execution),
                |conn, _| {
                    started_tx.send(()).unwrap();
                    conn.query_row(
                        "SELECT sum(i::HUGEINT)::VARCHAR FROM range(100000000000) t(i)",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(Into::into)
                },
            )
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        execution.cancel();
        let err = worker.join().unwrap().unwrap_err();
        assert!(err.downcast_ref::<QueryTimeoutError>().is_some(), "{err}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
