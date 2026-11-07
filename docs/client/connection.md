# Connecting with CamusDB.Client

How to open a connection, the connection string keys, and how the string is parsed.

## Installation

```shell
dotnet add package CamusDB.Client
```

Or, in the Visual Studio Package Manager Console:

```shell
Install-Package CamusDB.Client
```

## Configuration

```csharp
using CamusDB.Client;

CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=test");
await using CamusConnection connection = new(builder);

await connection.OpenAsync();
```

| Key | Default | Description |
| --- | --- | --- |
| `Endpoint` | required | Server URL, or a comma-separated pool. |
| `Database` | required | Database name sent with each request. |
| `Timeout` | `10` | Request timeout in seconds. |
| `Protocol` | `rest` | `rest` or `grpc`. See [Transport](transport.md). |
| `HttpVersion` | `1.1` | Highest HTTP version for REST: `1.1`, `2` or `3`. See [HTTP/2 and HTTP/3](transport.md#http2-and-http3-over-rest). |
| `User` | | User to log in as. Aliases: `UserId`, `Uid`, `Username`. See [Authentication](authentication.md). |
| `Password` | | Password of `User`. Alias: `Pwd`. |
| `AccessToken` | | Bearer token from elsewhere, sent as is instead of logging in. |
| `TokenLifetime` | `600` | Seconds to reuse a token when the server reports no expiry. Renewed at 80%. |
| `MaxAutoPrepare` | `128` | Statements to keep prepared; `0` disables. See [Prepared statements](prepared-statements.md). |
| `AutoPrepareMinUsages` | `2` | Runs of the same SQL before it is prepared. |
| `ChannelPoolSize` | `2` | gRPC only: `BatchExecute` streams per endpoint. See [Stream pool](transport.md#stream-pool). |
| `CoalescingThreshold` | `10` | gRPC only: `1` disables coalescing. See [Stream pool](transport.md#stream-pool). |
| `CoalescingDelay` | `0` | gRPC only: coalescing wait in ms; `0` is off. |
| `RequestFrames` | `true` | gRPC only: send waiting operations as one message. Also gates [pipelined transactions](transactions.md#pipelined-transactions). See [Stream frames](transport.md#stream-frames). |
| `TraceQueryText` | `false` | SQL text on spans as `db.query.text`: `false` none, `true` as sent, `Sanitized` with literals replaced by `?`. See [OpenTelemetry](../opentelemetry.md). |
| `BackupEndpoint` | `Endpoint` | HTTP endpoint of the backup API; required with `Protocol=grpc`. See [Backups](backups.md). |
| `BackupTimeout` | `300` | Backup request timeout in seconds. |
| `AllowInsecureCredentials` | `false` | `true` allows credentials to a remote `http://` endpoint. See [TLS](authentication.md#tls). |
| `RoutingMode` | `Auto` | `Auto`, `Learned` or `Off`. See [Learned routing](learned-routing.md). |
| `RoutingNodes` | | Map from server node identities to `Endpoint` members. |
| `RoutingMaxHintAge` | `5000` | Max ms to reuse a learned route. |
| `IsolationLevel`, `TransactionMode`, `Locking` | server default | Defaults for transactions and autocommit statements. See [Transactions](transactions.md#isolation-level-mode-and-optimistic-locking). |

An invalid value falls back to the default.

## Endpoint pool

With several endpoints, the client uses round-robin (each request takes the next endpoint):

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://localhost:8082,http://localhost:8084,http://localhost:8086;Database=test");
```

An unreachable endpoint is skipped for 30 seconds. All connections with the same `Endpoint` value share the rotation and this health state, also under EF Core.

## Parsing

- Keys are case-insensitive and trimmed.
- A repeated key throws `CADB0000`.
- An unquoted value is trimmed and ends at the next `;`.
- Quote a value (single or double quotes) to keep a `;` or spaces. Double the quote to include it.

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=https://db.example:5095;Database=test;User=app;Password='pa;ss''word'");
// Password is: pa;ss'word
```

## Logging and changing the string

`builder.ToString()` includes credentials. For logs, use `builder.ToRedactedString()` or `CamusConnectionStringBuilder.Redact(...)`, which mask `Password`, `Pwd` and `AccessToken`. EF Core debug info uses the redacted form.

Assigning a different connection string to a `CamusConnection` throws `NotSupportedException`. Create a new connection instead.
