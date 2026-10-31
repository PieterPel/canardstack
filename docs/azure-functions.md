# Azure Functions custom handler

canardstack remains an ordinary HTTP server. Azure-specific port and local-path
handling lives in `deploy/azure-functions/launch.sh`; application Rust code
does not inspect Functions environment variables.

The deployment template targets **Linux Azure Functions**. It has not yet been
tested against Functions Core Tools or deployed to Azure. The local example
uses the emulator connection string for the Functions host's own storage;
Azure Blob as the DuckLake data path is a separate, unfinished integration.

## Request path

The Functions host starts `launch.sh`, which passes its assigned
`FUNCTIONS_CUSTOMHANDLER_PORT` to the existing `serve --listen` option.
`enableProxyingHttpRequest` forwards HTTP requests/responses instead of wrapping
them in the custom-handler invocation JSON protocol. This setting follows the
current Functions documentation; verify availability on the deployed host
version. HTTP-only bindings are required for this template.

The empty `routePrefix` and catch-all HTTP trigger preserve paths such as
`/api/v1/query`, `/loki/api/v1/query_range`, and `/api/v2/traces/<id>`. Azure is not
a transparent general-purpose reverse proxy: query strings, POST bodies,
`Authorization`, status codes, `Retry-After`, and Tempo's binary responses must
be checked through the actual Functions host.

The trigger uses `authLevel: anonymous` because canardstack validates its own
bearer/API keys for query and admin routes. If platform function-key auth is
enabled instead, clients must also supply that key; Grafana datasource
configuration must match. `/healthz` and `/metrics` retain their existing public
semantics. Apply deployment access restrictions if those endpoints should not
be publicly reachable. Azure terminates TLS; set `CANARDSTACK_TLS_ENABLED=false`.

## Package and run locally

1. Copy the deployment directory to a staging directory, preserving the
   `query/function.json` layout.
2. Put the compiled `canardstack` binary beside `launch.sh` and make both
   executable. Use a native macOS binary for local Core Tools, but build a
   Linux binary for the target Azure architecture and libc. A macOS executable
   cannot run in the Linux Function App.
3. Copy `local.settings.example.json` to `local.settings.json`. Configure the
   external DuckLake catalog/data path and credentials there for an integration
   test. An empty configuration uses a local, disposable DuckLake only.
4. Run `func start` from that staging directory. Test the API through port 7071,
   not the internal custom-handler port.
5. Use Function App application settings in Azure; do not publish local
   development settings or credentials.

The host concurrency limits in `host.json` are an initial per-instance bound.
They do not replace canardstack's cheap/heavy admission limits or cap the total
load across scaled-out instances.

## Deployment blockers and improvements

| Area | Current limitation | Required action |
| --- | --- | --- |
| Reader/writer compatibility | The Compose cross-signal smoke exposed missing DuckLake inlined-data tables. | Resolve R1 in `work-log.md` before relying on this stack in Azure. Test the exact DuckDB/DuckLake version matrix. |
| Durable storage | Compose volumes and a local DuckDB metadata file are not a shared catalog across Function instances. | Host the external writer/catalog independently and use remote data storage. Do not share the writable local DuckDB control file between workers. |
| Azure Blob | The application currently explicitly configures S3/GCS, not Azure credentials. | Implement and verify the Azure extension/identity path from the work log. Function host `AzureWebJobsStorage` is not DuckLake data-storage configuration. |
| Startup deadline | Custom handlers must start their web server within 60 seconds. `AppState` opens storage and loads extensions before serving. | Preinstall compatible extensions and measure cold attach time with real networking/identity. Keep startup failure explicit; do not claim readiness before storage is usable. |
| Local filesystem | Deployment files can be read-only; default local state cannot be authoritative. | The launch script creates a distinct temporary control directory by default. Treat it as disposable. Set extension cache/HOME to a suitable writable or prepopulated path, and plan scratch-space cleanup/quotas. |
| Startup writes | The query server currently creates tables, changes partitioning, and stamps schema metadata. | Fix schema-fence/bootstrap ordering and define privileges before promising a read-only identity or unconstrained scale-out. |
| Memory | DuckDB memory is per process; the Functions host, query results, network buffers, and extensions also consume memory. | Size the hosting plan with headroom and set DuckDB memory/query concurrency accordingly. Do not budget the full instance memory to DuckDB. |
| HTTP timeouts | The Functions front end has its own request timeout; a longer function timeout does not remove it. | Keep query/admission/body deadlines comfortably below the platform deadline. Verify gateway/client timeout behavior and cancellation. |
| Recycling and scale | Caches are instance-local and processes may be stopped. Increasing instance count multiplies catalog connections and query concurrency. | Bound scale and connection budgets; consider warm instances where predictable Grafana latency matters. Verify simultaneous startup and query-only behavior. |
| Native dependencies | A bundled binary still depends on the target system ABI and compatible extensions. | Verify the binary and extension loading in the actual Linux host environment. Use an appropriate Functions custom container if the required native dependencies are absent. |
| Observability | Metrics are process-local, and `/healthz` can report healthy despite unreadable data. | Verify per-instance collection, structured stdout logs, bounded health checks, and the readiness improvements in R7. |

Axum/Tokio addresses HTTP handling and bounded execution. It does not itself
resolve these storage/deployment constraints.

## Acceptance checks before deployment

- Start with an empty local control/extension cache and measure handler startup.
- Ingest logs, metrics, and spans through the external writer, then query them
  through Functions with Grafana-compatible paths.
- Exercise GET and form POST requests, invalid auth, 429/503 and `Retry-After`,
  and Tempo JSON/protobuf content negotiation.
- Recycle the handler and start two instances against the same remote catalog.
- Check managed identity/RBAC, private endpoint DNS/egress, memory/scratch
  pressure, client disconnects, and scale-in shutdown.

Sources:

- [Azure Functions custom handlers](https://learn.microsoft.com/en-us/azure/azure-functions/functions-custom-handlers)
- [Functions hosting and timeouts](https://learn.microsoft.com/en-us/azure/azure-functions/functions-scale#function-app-time-out-duration)
