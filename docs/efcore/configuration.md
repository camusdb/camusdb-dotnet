# EF Core: installation and configuration

Installing the provider and configuring a `DbContext` (EF Core's session class).

## Installation

```shell
dotnet add package CamusDB.EntityFrameworkCore
```

Or:

```shell
Install-Package CamusDB.EntityFrameworkCore
```

## Configuration

Register the provider:

```csharp
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb;Timeout=30")
    .Options;
```

Or in `OnConfiguring`:

```csharp
public class AppDbContext : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseCamusDB("Endpoint=http://localhost:8082;Database=mydb");
}
```

All [`CamusDB.Client` connection string keys](../client/connection.md) work, including `Protocol=grpc` ([Transport](../client/transport.md)) and `User`/`Password` ([Authentication](../client/authentication.md)):

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb;User=app;Password=app-secret")
    .Options;
```

A context logs in once per credential set. A query fails with `CADB0517` unless every table it touches is granted.

### Prepared statements

EF statements are prepared automatically from the second run of the same SQL. See [Prepared statements](../client/prepared-statements.md) for settings and the `IN (…)` caveat.

### Retry on failure

`EnableRetryOnFailure` makes EF's execution strategy (retry policy) retry `SaveChangesAsync` and queries on `CADB0502`, `CADB0504`, `CADB0505`, or a message with `MustRetry`, `AlreadyLocked` or `commit returned Aborted`.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb", o =>
    {
        o.EnableRetryOnFailure();
    })
    .Options;
```

| Parameter | Default | Description |
| --- | --- | --- |
| `maxRetryCount` | `15` | Maximum retries |
| `maxRetryDelay` | `1 s` | Longest delay between retries |
| `retryDeadline` | `5 s` | No retry after this time since the first failure |
| `medianFirstRetryDelay` | `30 ms` | Shortest delay between retries |

```csharp
o.EnableRetryOnFailure(
    maxRetryCount: 5,
    maxRetryDelay: TimeSpan.FromMilliseconds(500),
    retryDeadline: TimeSpan.FromSeconds(3),
    medianFirstRetryDelay: TimeSpan.FromMilliseconds(20));
```

### Optimistic locking & isolation defaults

Make [optimistic locking](../client/transactions.md#isolation-level-mode-and-optimistic-locking) (conflicts found at commit) the default, with retries to replay the loser:

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb", o =>
    {
        o.UseOptimisticLocking();   // or o.UsePessimisticLocking()
        o.EnableRetryOnFailure();
    })
    .Options;
```

`UseTransactionDefaults(new CamusTransactionOptions { … })` sets isolation, mode and locking; unset values fall back to the connection string, then the server. `Database.BeginTransaction(IsolationLevel.X)` overrides them. `ReadCommitted`, `Serializable` and `Snapshot` (Serializable read-only) work; other levels throw `NotSupportedException`.

> EF replays the **whole unit of work**. For transactions you begin yourself, use [`SerializableRetryHelper`](../client/transactions.md#serializable-isolation--retries).

### Sharing an existing connection

Pass an open `CamusConnection` to share it or its transaction. The context never closes or disposes it.

```csharp
CamusConnection connection = await GetOpenConnectionAsync();

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB(connection)
    .Options;

await using var ctx = new AppDbContext(options);
await ctx.Database.BeginTransactionAsync();
// ... SaveChangesAsync, then CommitTransactionAsync
```
