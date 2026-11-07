# Transport: REST and gRPC

How to choose between the REST/JSON and gRPC protocols, how the gRPC stream pool works, and how to use HTTP/2 and HTTP/3 over REST.

## Transport (REST / gRPC)

Set the protocol per connection with `Protocol` (`rest` or `grpc`, case-insensitive). The default is REST, and any other value also means REST. The ADO.NET API and the EF Core provider work the same on both.

```csharp
// REST (default) — no change needed
CamusConnectionStringBuilder rest = new("Endpoint=http://localhost:5095;Database=test");

// gRPC — set Protocol=grpc and point Endpoint at the gRPC port
CamusConnectionStringBuilder grpc = new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc");
```

- The server serves REST and gRPC on separate ports, for example `5095` and `5096`. With `Protocol=grpc`, `Endpoint` must be the gRPC port.
- Use `http://` for h2c (HTTP/2 without TLS) in local development, or `https://` for TLS.
- An endpoint pool works with both protocols.

Both protocols throw the same `CamusException` with the server's `CADBxxxx` code, so error handling and transaction retries do not change.

Differences between the protocols:

- `CreateDatabaseAsync` and `DropDatabaseAsync` use the `/create-db` and `/drop-db` routes over REST and SQL over gRPC. The result is the same.
- `CreateBranchDatabaseAsync`, `ShowBranchesAsync` and `ShowAncestorsAsync` run SQL on both (`CREATE DATABASE … BRANCH FROM …`, `SHOW BRANCHES FROM …`, `SHOW ANCESTORS FROM …`). With authentication on, creating a branch needs a superuser (`CADB0517` otherwise). The SHOW methods list only databases the user can see.

## Stream pool

Over gRPC, queries, non-queries and transactions share a small pool of long-lived `BatchExecute` streams (two-way streams that carry many requests). Concurrent operations share streams instead of each making its own call.

- Autocommit statements spread across the pool.
- A transaction's `BEGIN`, statements and `COMMIT` stay on one stream, so the server keeps their order.
- DDL and ping use single-request calls.

`ChannelPoolSize` (default `2`) sets the number of streams per endpoint. It does not limit concurrent transactions. Raise it when many long-running streaming queries to one endpoint queue behind each other.

`CoalescingThreshold` (default `10`) and `CoalescingDelay` (default `0`, off) trade latency for larger messages when a burst of operations arrives on one connection. The delay never applies to a lone operation, because a caller that waits for each reply would pay it on every round trip.

Coming from Cloud Spanner: `ChannelPoolSize` is like `NumChannels`. CamusDB has no server-side sessions, so there is no `MinSessions` or `MaxSessions`. `SessionPoolManager` and `SessionPoolOptions` are obsolete and do nothing.

## Stream frames

When several operations wait for the same stream, the client sends them as one message, a *frame*. This saves the fixed cost of each message. A frame never waits for more operations, so a lone operation adds no latency.

- The client sends frames only after the server announces support on that stream (response header `camusdb-batch-frames`). Older servers get one message per operation.
- The client sends `camusdb-batch-frames-accept` when a stream opens, so the server can pack multi-row results the same way. Older servers ignore it.
- A frame gives no atomicity or ordering. Use a transaction for that.
- If a frame write fails, each operation in it fails with the transport error, and the normal retry rules apply. The client never resends a frame.

Set `RequestFrames=false` to send one operation per message, for A/B tests or as a kill switch.

## HTTP/2 and HTTP/3 over REST

REST uses HTTP/1.1 by default. Set `HttpVersion` to ask for a later version:

```csharp
// HTTP/3 (QUIC) against a server with http3_enabled: true
CamusConnectionStringBuilder h3 = new("Endpoint=https://db.example.com:7141;Database=test;HttpVersion=3");
```

`HttpVersion` accepts `1.1`, `2` or `3` (also `2.0`, `3.0`); other values mean `1.1`. It applies to every REST call: statements, login, logout and the [backup API](backups.md) on any `Protocol`. gRPC statements always use HTTP/2.

The value is a maximum. The client falls back to a lower version when needed:

- `HttpVersion=2` gives HTTP/2 on `https://`. On `http://` it stays on HTTP/1.1.
- `HttpVersion=3` sends the first request over TCP. The server's `Alt-Svc` header announces HTTP/3, and later requests use QUIC (HTTP/3's UDP-based transport). Without a QUIC library on the client, all requests stay on TCP.
- If QUIC fails (for example, a TLS handshake abort or blocked UDP), the client resends that request over TCP and stops using HTTP/3 for that endpoint. The first attempt never reached the server, so no statement runs twice.

HTTP/3 needs:

- An `https://` endpoint on the server's HTTPS port.
- `http3_enabled: true` and `https_certificate` on the server.
- The HTTPS port open for UDP and TCP, including Docker port mappings and firewalls.
- QUIC on the client: Windows 11 / Windows Server 2022 or later, or `libmsquic` on Linux and macOS (`brew install libmsquic`). On macOS, start the app with `DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib`. macOS strips `DYLD_*` variables for system binaries such as `nohup`, so set it in the app's own environment.

To check, run `SHOW CONNECTIONS` on the server. The `protocol` column shows `HTTP/3` for a QUIC connection.
