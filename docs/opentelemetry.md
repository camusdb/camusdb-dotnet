# OpenTelemetry

The driver emits traces (one span per operation) and metrics through .NET `ActivitySource` and `Meter`. No OpenTelemetry package is required.

## Setup

```csharp
using CamusDB.Client;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource(CamusTelemetry.ActivitySourceName))   // "CamusDB.Client"
    .WithMetrics(metrics => metrics.AddMeter(CamusTelemetry.MeterName));            // "CamusDB.Client"
```

Or use the optional `CamusDB.OpenTelemetry` package, which depends only on `OpenTelemetry.Api`:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddCamusDBInstrumentation())
    .WithMetrics(metrics => metrics.AddCamusDBInstrumentation());
```

## Spans

The driver records a client span for:

- each command execution, including DDL, EF Core and Dapper queries;
- each typed insert (`CamusInsertCommand`);
- `BEGIN`, `COMMIT` and `ROLLBACK`. Retries of an unresolved commit (`CADB0509`) are `camusdb.retry` events on one span;
- each pipeline exchange, as a `BATCH` span with `db.operation.batch.size`.

The span name is the operation and its tables, such as `SELECT orders customers`. Without a table, it is `{operation} {database}`, such as `SELECT shop`.

Attributes, from the [database conventions](https://opentelemetry.io/docs/specs/semconv/database/):

| Attribute | Value |
| --- | --- |
| `db.system.name` | `camusdb` |
| `db.namespace` | `Database=` |
| `db.operation.name` | First SQL keyword if known, or `BEGIN`, `COMMIT`, `ROLLBACK`, `BATCH` |
| `db.collection.name` | The table, if exactly one |
| `db.query.summary` | Same as the span name, max 255 characters |
| `server.address`, `server.port` | Endpoint used |
| `db.query.text` | SQL text, only with `TraceQueryText` |
| `error.type`, `db.response.status_code` | `CADBxxxx` code, or exception type |
| `camusdb.transport` | `rest` or `grpc` |

`TraceQueryText` defaults to `false`. Set `true` to record SQL as sent. Set `Sanitized` to replace literals with `?`, drop comments, and write lists of `?` as `?, ...`. Parameter values are never recorded.

Spans are children of `Activity.Current`. Transport HTTP or gRPC spans are children of the driver span.

## Metrics

`db.client.operation.duration` is a histogram, in seconds, of the same operations. Tags: `db.system.name`, `db.namespace`, `db.operation.name`, `server.*`, `error.type`, `db.response.status_code`.

| Metric | Counts |
| --- | --- |
| `camusdb.client.auth.logins` | Logins, by `camusdb.login.mode`: `blocking` (a statement waited), `background` (renewal), `explicit` (app call) |
| `camusdb.client.operation.retries` | Resends, by `camusdb.retry.kind`: `finalize` (after `CADB0509`), `autocommit`, `token_renewal` (after `CADB0516`), `create_database` |
| `camusdb.client.endpoint.quarantines` | Endpoint failures that set it aside for 30 seconds |
| `camusdb.client.endpoint.quarantined` | Gauge: `1` while set aside, else `0` |
| `camusdb.client.prepared_statement.lookups` | Preparable runs: `hit` (prepared), `registered` (prepared now), `miss` (sent as text) |
| `camusdb.client.prepared_statement.registrations` | Prepare requests: `success`, `refused` (runs as text), `unsupported` (auto-prepare stops) |
| `camusdb.client.prepared_statement.evictions` | Statements dropped for newer ones |
| `camusdb.client.prepared_statement.count` | Statements prepared now |

If `count` stays at `MaxAutoPrepare` and `evictions` grows, raise [MaxAutoPrepare](client/prepared-statements.md).

## Notes

- With no listener, telemetry costs nothing.
- Tables come from the names after `FROM`, `JOIN`, `INTO`, `UPDATE` and `TABLE`. A comma join reports only its first table; a CTE name counts as a table.
- A streaming reader's span ends at the last row, a read error, or close.
- No `db.client.connection.*` metrics exist; `OpenAsync` opens no network connection.
- With EF Core instrumentation enabled too, each query gets two spans.
