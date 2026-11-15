---
title: Runtime Contract
description: Process shape, storage requirements, auth, limits, and unsupported behavior.
---

canardstack is a query-only process.

## Process

- one Rust binary: `canardstack`
- Axum/Hyper HTTP server on Tokio
- one embedded DuckDB instance inside canardstack
- synchronous database execution in bounded blocking tasks
- asynchronous cheap/heavy admission, with permits held until execution finishes
- no OTLP ingest endpoint
- no gRPC endpoint
- no bundled DuckLake catalog service

## Required Storage

Set a DuckLake attach target:

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data
```

The catalog must contain the `otlp_*` tables in the
[storage schema reference](/reference/storage-schema/).

## Auth

- Query routes use `CANARDSTACK_API_KEY`.
- Admin health routes use `CANARDSTACK_ADMIN_API_KEY`.
- Non-empty, distinct query and admin keys are required. Unset settings use the
  development defaults; query/admin routes still require authentication.

## Query Bounds

Query paths are constrained by:

- time range
- row limit
- request timeout
- DuckDB memory limit
- query concurrency
- admission class

Broad scans can still be expensive. Dashboard panels should use explicit
selectors, short time ranges, and panel limits.

## Compatibility Scope

canardstack implements subsets of Prometheus, Loki, and Tempo APIs. It is not a
full PromQL, LogQL, TraceQL, Prometheus, Loki, or Tempo implementation.

Prometheus and Loki-compatible routes return their protocol error envelope:

```json
{"status":"error","errorType":"...","error":"..."}
```

## Unsupported

- OTLP ingest
- OTLP/gRPC
- Kafka
- arbitrary SQL over HTTP
- canardstack-owned ingest durability or table maintenance
- bundled DuckLake catalog service
- histograms and exponential histograms through the compatibility APIs

Use direct DuckDB SQL for analysis outside the compatibility surface.
