use crate::config::Config;
use crate::metrics::{MetricName, Metrics};
use crate::validation::{ApiError, ApiResult};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryClass {
    Cheap,
    Heavy,
}

impl QueryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cheap => "cheap",
            Self::Heavy => "heavy",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AdmissionSnapshot {
    pub cheap_query_active: usize,
    pub cheap_query_capacity: usize,
    pub heavy_query_active: usize,
    pub heavy_query_capacity: usize,
    pub query_rejections_total: u64,
}

pub struct AdmissionController {
    cheap: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
    rejections: AtomicU64,
}

pub type QueryAdmissionGuard = OwnedSemaphorePermit;

impl AdmissionController {
    pub fn new(config: &Config) -> Self {
        let total = config.operator.query_interactive.concurrency;
        let cheap = config
            .operator
            .cheap_query_admission_capacity
            .min(total.saturating_sub(1))
            .max(1);
        let heavy = total.saturating_sub(cheap).max(1);
        Self {
            cheap: Arc::new(Semaphore::new(cheap)),
            heavy: Arc::new(Semaphore::new(heavy)),
            cheap_query_capacity: cheap,
            heavy_query_capacity: heavy,
            rejections: AtomicU64::new(0),
        }
    }

    fn semaphore(&self, class: QueryClass) -> Arc<Semaphore> {
        match class {
            QueryClass::Cheap => self.cheap.clone(),
            QueryClass::Heavy => self.heavy.clone(),
        }
    }

    pub fn reserve_query(
        &self,
        class: QueryClass,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard> {
        let result = self
            .semaphore(class)
            .try_acquire_owned()
            .map_err(|_| self.reject(class, metrics));
        self.record_metrics(metrics);
        result
    }

    pub async fn reserve_query_with_wait(
        &self,
        class: QueryClass,
        max_wait: Duration,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard> {
        if max_wait.is_zero() {
            return self.reserve_query(class, metrics);
        }
        let result =
            match tokio::time::timeout(max_wait, self.semaphore(class).acquire_owned()).await {
                Ok(Ok(permit)) => Ok(permit),
                _ => Err(self.reject(class, metrics)),
            };
        self.record_metrics(metrics);
        result
    }

    fn reject(&self, class: QueryClass, metrics: &Metrics) -> ApiError {
        self.rejections.fetch_add(1, Ordering::Relaxed);
        let (reason, message) = match class {
            QueryClass::Cheap => (
                "cheap_query_admission_full",
                "cheap query admission capacity is exhausted",
            ),
            QueryClass::Heavy => (
                "heavy_query_admission_full",
                "heavy query admission capacity is exhausted",
            ),
        };
        metrics.inc(
            MetricName::AdmissionRejectionsTotal,
            &[("admission", "query"), ("reason", reason)],
            1,
        );
        ApiError::new(429, reason, message).with_retry_after(1)
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            cheap_query_active: self.cheap_query_capacity - self.cheap.available_permits(),
            cheap_query_capacity: self.cheap_query_capacity,
            heavy_query_active: self.heavy_query_capacity - self.heavy.available_permits(),
            heavy_query_capacity: self.heavy_query_capacity,
            query_rejections_total: self.rejections.load(Ordering::Relaxed),
        }
    }

    pub fn record_metrics(&self, metrics: &Metrics) {
        let snapshot = self.snapshot();
        for (label, capacity, active) in [
            (
                "query_cheap",
                snapshot.cheap_query_capacity,
                snapshot.cheap_query_active,
            ),
            (
                "query_heavy",
                snapshot.heavy_query_capacity,
                snapshot.heavy_query_active,
            ),
        ] {
            metrics.gauge(
                MetricName::AdmissionCapacity,
                &[("admission", label)],
                capacity as f64,
            );
            metrics.gauge(
                MetricName::AdmissionInUse,
                &[("admission", label)],
                active as f64,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller() -> AdmissionController {
        AdmissionController::new(&Config::test(std::path::PathBuf::from("unused.duckdb")))
    }

    #[test]
    fn cheap_and_heavy_queries_use_separate_capacity() {
        let admission = controller();
        let metrics = Metrics::default();
        let cheap = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let heavy = (0..3)
            .map(|_| {
                admission
                    .reserve_query(QueryClass::Heavy, &metrics)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            admission
                .reserve_query(QueryClass::Cheap, &metrics)
                .unwrap_err()
                .status,
            429
        );
        assert_eq!(
            admission
                .reserve_query(QueryClass::Heavy, &metrics)
                .unwrap_err()
                .reason,
            "heavy_query_admission_full"
        );
        assert_eq!(admission.snapshot().cheap_query_active, 1);
        drop((cheap, heavy));
        assert_eq!(admission.snapshot().heavy_query_active, 0);
    }

    #[tokio::test]
    async fn query_waits_and_cancelled_waiter_does_not_leak_capacity() {
        let admission = Arc::new(controller());
        let metrics = Arc::new(Metrics::default());
        let first = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let waiting = {
            let admission = admission.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                admission
                    .reserve_query_with_wait(QueryClass::Cheap, Duration::from_secs(1), &metrics)
                    .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        drop(waiting.await.unwrap().unwrap());
        let first = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let result = admission
            .reserve_query_with_wait(QueryClass::Cheap, Duration::from_millis(10), &metrics)
            .await;
        assert_eq!(result.unwrap_err().status, 429);
        drop(first);
        assert_eq!(admission.snapshot().cheap_query_active, 0);
    }
}
