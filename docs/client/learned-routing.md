# Learned routing

The driver can learn which node leads (coordinates) the data of a statement and send the statement there, saving a forwarding hop. This changes only latency: never results, isolation or commits.

A server with `sql_routing_advice_enabled` (on by default) adds this advice to SQL responses.

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://a:5095,http://b:5095,http://c:5095;Database=test;RoutingMode=Learned;" +
    "RoutingNodes='camus-a:7070=http://a:5095,camus-b:7070=http://b:5095,camus-c:7070=http://c:5095'");
```

## Configuration

`RoutingNodes` maps server node identities (`host:port`) to endpoints. Quote it, because it contains `=`. The driver routes only to mapped addresses that are also in `Endpoint`, and never connects to an address from the server.

| `RoutingMode` | Routing is on when |
| --- | --- |
| `Auto` (default) | `RoutingNodes` maps 2 or more distinct endpoints. Without `RoutingNodes`, requests are unchanged. |
| `Learned` | `RoutingNodes` maps 1 or more endpoints. |
| `Off` | Never. |

## What is learned

- Advice is stored per database, exact SQL text and statement kind. It never applies to other SQL text, so parameterized and prepared statements benefit most.
- A route expires after the smaller of the server's `maxAgeMs` and `RoutingMaxHintAge`.
- The store holds at most 4096 entries or 4 MiB.
- Connections with the same endpoints and routing settings share it, also under EF Core.

## Precedence

- A transaction stays on the endpoint where it started.
- A route to an unreachable endpoint falls back to rotation.
- Routing adds no retries.

## Where a transaction starts

With routing off, `BeginTransaction` sends `BEGIN` at once, on rotation.

With routing on, `BeginTransaction` sends nothing. The first statement starts the transaction on its learned endpoint, and the rest of the transaction stays there. To use a later statement's endpoint instead, pass its exact SQL as `CamusTransactionOptions.Affinity`; `BEGIN` is then sent at once.

### What the deferral changes for a caller

- `TxnIdPT`, `TxnIdCounter` and `TransactionId` are zero until the first statement. Check `IsStarted`.
- A `BEGIN` failure is thrown by the first statement.
- A transaction with no statements starts at `Commit` or `Rollback`, on rotation.
- Set isolation and locking with `BeginTransactionAsync(CamusTransactionOptions)` or connection string defaults. A first `SET TRANSACTION` statement has no advice, so the transaction starts on rotation.

## Compatibility and diagnostics

Older servers ignore routing, and the driver works unchanged. `CamusCommand.LastRoutingAdvice` shows the latest advice. Eligible statements and the wire format are in the server's `docs/sql-routing-advice.md`.
