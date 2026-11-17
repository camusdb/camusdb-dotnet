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

### Supported versions

The package has one build for each EF Core major version. NuGet picks the build from the target framework of your project:

| Your project targets | EF Core |
| --- | --- |
| `net10.0` | 10.0.x |
| `net11.0` or later | 11.0.x |

- From 0.19.0, a `net11.0` project gets EF Core 11. If it references an EF Core 10 package directly, the restore fails with `NU1605` (package downgrade). Update those references to 11.0.x.
- Do not use EF Core 11 with a provider release before 0.19.0. Those releases have an EF Core 10 build only, and some queries fail on EF Core 11 with `MissingMethodException`. On .NET 11 with an older release, keep EF Core 10.
- From 0.19.1, the provider checks the EF Core major at the first `UseCamusDB` call. If the process loads an EF Core major that the provider build was not compiled against, the call throws `InvalidOperationException`. The message names both versions and both file paths. Without this check, the wrong build fails later, in the first query that uses a changed EF Core member, with `MissingMethodException`.
- Both builds have the same assembly version, so a deployment can put the wrong one next to the application. Typical causes are a publish folder that is not clean, a copy from another build, or a plugin folder with its own copy of `CamusDB.EntityFrameworkCore.dll`. Deploy the build for the target framework of the application.
- To skip the check, set the `AppContext` switch `CamusDB.EntityFrameworkCore.DisableEfCoreVersionCheck` to `true`. Do this only for diagnosis.

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
