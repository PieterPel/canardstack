//! Shared physical signal/table vocabulary.
//!
//! `StorageSignal` names one variant per DuckLake table and is used across
//! ingest, storage, query, metrics, validation, and metadata. It is the single
//! place that maps a physical signal to its on-disk table name, so it lives in a
//! neutral top-level module rather than under any one pipeline stage.

use serde::Serialize;
use std::fmt;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum StorageSignal {
    Logs,
    Spans,
    MetricGauge,
    MetricSum,
    MetricHistogram,
}

impl StorageSignal {
    pub const ALL: [StorageSignal; 5] = [
        StorageSignal::Logs,
        StorageSignal::Spans,
        StorageSignal::MetricGauge,
        StorageSignal::MetricSum,
        StorageSignal::MetricHistogram,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StorageSignal::Logs => "otlp_logs",
            StorageSignal::Spans => "otlp_traces",
            StorageSignal::MetricGauge => "otlp_metrics_gauge",
            StorageSignal::MetricSum => "otlp_metrics_sum",
            StorageSignal::MetricHistogram => "otlp_metrics_histogram",
        }
    }
}

impl fmt::Display for StorageSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
