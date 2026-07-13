# CamusDB Connector for .NET

.NET idiomatic client libraries for [CamusDB](https://github.com/camusdb/camusdb)

This repository contains two packages:

| Package | Description |
| --- | --- |
| `CamusDB.Client` | ADO.NET provider — recommended for direct database access from .NET |
| `CamusDB.EntityFrameworkCore` | Entity Framework Core provider built on top of `CamusDB.Client` |

---

## CamusDB.Client (ADO.NET)

### Installation

```shell
dotnet add package CamusDB.Client
```

Or via the Package Manager Console:

```shell
Install-Package CamusDB.Client
```

### Configuration

Create a `CamusConnectionStringBuilder` with a connection string:

```csharp
using CamusDB.Client;

CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
await using CamusConnection connection = new(builder);

await connection.OpenAsync();
```

Supported connection string keys:

| Key | Required | Description |
| --- | --- | --- |
| `Endpoint` | Yes | Base URL for the CamusDB HTTP endpoint. |
| `Database` | Yes | Database name sent with requests. |
| `Timeout` | No | HTTP request timeout in seconds (default: `10`). |

`Endpoint` also supports a comma-separated pool. The client selects endpoints with round-robin routing:

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://localhost:8082,http://localhost:8084,http://localhost:8086;Database=test");
```

When a request fails because an endpoint is unreachable, that endpoint is marked unavailable and skipped by later requests made through the same `CamusConnectionStringBuilder`.

### Usage

#### Database Management

CamusDB requires databases to be explicitly created before use. Call `CreateDatabaseAsync` once during application startup or provisioning:

```csharp
// Create the database (no-op if it already exists)
await connection.CreateDatabaseAsync(ifNotExists: true);
```

To drop a database:

```csharp
await connection.DropDatabaseAsync();
```

Both methods operate on the database named in the connection string. An explicit name can also be passed:

```csharp
await connection.CreateDatabaseAsync("otherdb", ifNotExists: true);
await connection.DropDatabaseAsync("otherdb");
```

#### Ping

```csharp
await using CamusCommand ping = connection.CreatePingCommand();

int result = await ping.ExecuteNonQueryAsync();
```

#### Execute DDL

```csharp
await using CamusCommand command = connection.CreateCamusCommand("""
    CREATE TABLE robots (
        id OID PRIMARY KEY NOT NULL,
        name STRING NOT NULL,
        type STRING,
        year INT64,
        price FLOAT64,
        enabled BOOL
    )
    """);

bool created = await command.ExecuteDDLAsync();
```

CamusDB supports `CHECK` and named `NOT NULL` constraints, addable and droppable via `ALTER TABLE`.
A `CHECK` is a deterministic single-row predicate evaluated on every insert and update; a row is
rejected only when the predicate is `FALSE` (a `NULL` operand yields `UNKNOWN`, which passes). A
violation throws a `CamusException` with code `CADB0303`; a named `NOT NULL` violation throws
`CADB0301`. Both kinds are dropped by name with `DROP CONSTRAINT`:

```csharp
await connection.CreateCamusCommand("""
    CREATE TABLE products (
        id       OID PRIMARY KEY NOT NULL,
        name     STRING CONSTRAINT products_name_required NOT NULL,
        price    INT64 CHECK (price > 0),
        discount INT64,
        CONSTRAINT valid_discount CHECK (price >= discount)
    )
    """).ExecuteDDLAsync();

// Add / drop after the fact (ADD CHECK scans existing rows and rejects if any violate it)
await connection.CreateCamusCommand(
    "ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0)").ExecuteDDLAsync();
await connection.CreateCamusCommand(
    "ALTER TABLE products DROP CONSTRAINT positive_price").ExecuteDDLAsync();
```

#### Data Types

CamusDB columns are declared with these SQL types; each maps to a `ColumnType` on the wire and a CLR type the reader/parameters understand:

| SQL DDL type | `ColumnType` | CLR type(s) | Notes |
| --- | --- | --- | --- |
| `OID` (alias `OBJECT_ID`) | `Id` | `string`, `Guid`, `CamusObjectIdValue` | Native identifier; shares string key encoding. |
| `UUID` (alias `GUID`) | `Uuid` | `Guid` | Native 128-bit UUID; indexable, ordered by big-endian byte order. `gen_uuid_v4()` / `gen_uuid_v7()` generate values server-side. |
| `INT64` (aliases `INT`, `INTEGER`) | `Integer64` | `long`, `int`, `short`, `byte` | 64-bit signed. |
| `FLOAT64` | `Float64` | `double` | IEEE-754 double. |
| `FLOAT32` (alias `REAL`) | `Float32` | `float` | Stored at single precision. |
| `BOOL` (alias `BOOLEAN`) | `Bool` | `bool` | |
| `STRING` / `STRING(N)` | `String` | `string` | `N` bounds the length (UTF-16 code units); over-length is rejected. |
| `DATE` | `Date` | `DateOnly`, `DateTime` | Calendar date; stored as UTC ticks at midnight. |
| `DATETIME` (alias `TIMESTAMP`) | `DateTime` | `DateTime`, `DateTimeOffset` | Instant; normalized to UTC and read back as `DateTimeKind.Utc`. |
| `BYTES` (alias `BLOB`) | `Bytes` | `byte[]` | base64 over JSON, `0x`-hex in SQL literals. Default max 10 MB. |
| `ARRAY(T)` | `Array` | any `IEnumerable` of `T` | Homogeneous scalar list; not indexable, no inline SQL literal. |

```csharp
await using CamusCommand command = connection.CreateCamusCommand("""
    CREATE TABLE events (
        id        OID PRIMARY KEY NOT NULL,
        ref       UUID DEFAULT gen_uuid_v7(),
        name      STRING(64),
        payload   BYTES,
        score     FLOAT32,
        happened  DATETIME,
        day       DATE,
        tags      ARRAY(INT64)
    )
    """);

await command.ExecuteDDLAsync();
```

Dates/datetimes are normalized to UTC before being sent. Arrays carry a scalar element type, inferred from the values or set explicitly (required for an empty array):

```csharp
await using CamusCommand insert = connection.CreateInsertCommand("events");

insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("ref", ColumnType.Uuid, Guid.NewGuid());
insert.Parameters.Add("name", ColumnType.String, "launch");
insert.Parameters.Add("payload", ColumnType.Bytes, new byte[] { 0xDE, 0xAD });
insert.Parameters.Add("score", ColumnType.Float32, 9.5f);
insert.Parameters.Add("happened", ColumnType.DateTime, DateTime.UtcNow);
insert.Parameters.Add("day", ColumnType.Date, new DateOnly(2026, 5, 1));
insert.Parameters.Add("tags", ColumnType.Integer64, new long[] { 1, 2, 3 }, isArray: true);

await insert.ExecuteNonQueryAsync();
```

Reading them back uses the typed `CamusDataReader` accessors:

```csharp
Guid      reference = reader.GetGuid(reader.GetOrdinal("ref"));
byte[]    payload  = reader.GetFieldValue<byte[]>(reader.GetOrdinal("payload"));
float     score    = reader.GetFloat(reader.GetOrdinal("score"));
DateTime  happened = reader.GetDateTime(reader.GetOrdinal("happened"));
DateOnly  day      = reader.GetFieldValue<DateOnly>(reader.GetOrdinal("day"));
object?[] tags     = (object?[])reader.GetValue(reader.GetOrdinal("tags"));
```

#### Insert Rows

```csharp
using CamusDB.Core.Util.ObjectIds;

await using CamusCommand insert = connection.CreateInsertCommand("robots");

insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("name", ColumnType.String, "T-800");
insert.Parameters.Add("type", ColumnType.String, "cyborg");
insert.Parameters.Add("year", ColumnType.Integer64, 1984);
insert.Parameters.Add("price", ColumnType.Float64, 10.0);
insert.Parameters.Add("enabled", ColumnType.Bool, true);

int insertedRows = await insert.ExecuteNonQueryAsync();
```

You can also execute parameterized SQL:

```csharp
const string sql = """
    INSERT INTO robots (id, name, year, type, price, enabled)
    VALUES (GEN_ID(), @name, @year, @type, @price, @enabled)
    """;

await using CamusCommand insert = connection.CreateCamusCommand(sql);

insert.Parameters.Add("@name", ColumnType.String, "R2-D2");
insert.Parameters.Add("@year", ColumnType.Integer64, 1977);
insert.Parameters.Add("@type", ColumnType.String, "mechanical");
insert.Parameters.Add("@price", ColumnType.Float64, 25.5);
insert.Parameters.Add("@enabled", ColumnType.Bool, true);

int insertedRows = await insert.ExecuteNonQueryAsync();
```

See [Data Types](#data-types) above for inserting `bytes`, `float32`, `date`, `datetime` and `array(T)` values.

#### Select Rows

```csharp
await using CamusCommand select = connection.CreateSelectCommand(
    "SELECT * FROM robots WHERE year = @year");

select.Parameters.Add("@year", ColumnType.Integer64, 1977);

CamusDataReader reader = await select.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    string id   = reader.GetString(0);
    string name = reader.GetString(1);
    string type = reader.GetString(2);
    long   year = reader.GetInt64(3);
}
```

#### Query Result Cache

CamusDB has an opt-in, per-node, in-memory cache of fully materialized `SELECT` results. A query
joins it with an inline `{cache=name}` hint placed right after a table reference; an identical later
query (same shape, same bound values, same schema) can then be served from memory. The cache only
serves single-table, autocommit reads — hints on joins or inside explicit transactions are inert.

Add the hint directly in your SQL, optionally with a per-entry TTL or `strict` (validate every hit
against live storage). `CamusCacheHint.Build(...)` assembles the fragment for you:

```csharp
string hint = CamusCacheHint.Build("recent_orders", ttl: TimeSpan.FromSeconds(30));

await using CamusCommand select = connection.CreateSelectCommand(
    $"SELECT id, total FROM orders {hint} WHERE status = @status ORDER BY total DESC LIMIT 20");

select.Parameters.Add("@status", ColumnType.Integer64, 1);

CamusDataReader reader = await select.ExecuteReaderAsync();

// Inspect how the server resolved the cache for this query.
CamusCacheMetadata? cache = reader.CacheMetadata;   // also on select.LastCacheMetadata
if (cache is not null)
    Console.WriteLine($"{cache.Status} ({cache.Name}), age={cache.AgeMs}ms");
```

`CamusCacheMetadata` exposes `Status` (`Hit`, `Miss`, `Bypass`, `StaleRevalidated`,
`EvictedBeforePublish`), `BypassReason`, `Name`, `CachedAtHlc`, and `AgeMs`. It is `null` for any
query that carried no hint, so ordinary queries are unaffected.

Evict entries manually — both are scoped to the current database:

```csharp
await connection.EvictCacheAsync("recent_orders");   // one family
await connection.EvictAllCacheAsync();               // every entry for this database
```

#### Transactions

```csharp
CamusTransaction transaction = await connection.BeginTransactionAsync();

await using CamusCommand insert = connection.CreateInsertCommand("robots");
insert.Transaction = transaction;

insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("name", ColumnType.String, "HAL 9000");
insert.Parameters.Add("type", ColumnType.String, "electronic");
insert.Parameters.Add("year", ColumnType.Integer64, 1968);
insert.Parameters.Add("price", ColumnType.Float64, 42.0);
insert.Parameters.Add("enabled", ColumnType.Bool, true);

await insert.ExecuteNonQueryAsync();
await transaction.CommitAsync();
```

Use `await transaction.RollbackAsync()` to roll back instead.

#### Serializable Isolation & Retries

Serializable is the default isolation level in CamusDB. When two serializable transactions conflict, one is aborted immediately and must be **replayed from `BEGIN`** — retrying a single statement is not safe.

Three error codes indicate a transient conflict that a full retry can resolve:

| Code | Name | When raised |
| --- | --- | --- |
| `CADB0502` | `TransactionConflict` | Lock conflict; server aborted at lock-acquire time |
| `CADB0504` | `TransactionMustRetry` | Routing retry budget exhausted; no data written |
| `CADB0505` | `TransactionLifetimeExceeded` | Transaction held open past the server lifetime limit |

Use `SerializableRetryHelper.IsRetryable(ex)` to test any exception, and `SerializableRetryHelper.ExecuteAutocommitAsync` for bounded automatic retry of single-statement (autocommit) operations:

```csharp
await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
{
    CamusTransaction tx = await connection.BeginTransactionAsync(ct);
    try
    {
        await using CamusCommand cmd = connection.CreateCamusCommand(
            "UPDATE robots SET price = @price WHERE name = @name");
        cmd.Transaction = tx;
        cmd.Parameters.Add("@price", ColumnType.Float64, 99.0);
        cmd.Parameters.Add("@name",  ColumnType.String,  "T-800");
        await cmd.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
    }
    catch
    {
        await tx.RollbackAsync(ct);
        throw;
    }
}, maxAttempts: 5, cancellationToken);
```

Back-off schedule: `min(20 ms × 2^attempt, 400 ms)` ± 25 % jitter. Any non-retryable exception propagates immediately.

For explicit multi-statement transactions, own the retry loop yourself so you can replay every read and write from scratch:

```csharp
const int MaxAttempts = 5;
int attempt = 0;

while (true)
{
    CamusTransaction tx = await connection.BeginTransactionAsync();
    try
    {
        // re-execute ALL reads and writes on every attempt
        long balance = await ReadBalance(tx, accountId);
        if (balance < amount)
            throw new InvalidOperationException("Insufficient funds");
        await Debit(tx, accountId, balance - amount);
        await tx.CommitAsync();
        break;
    }
    catch (CamusException ex) when (SerializableRetryHelper.IsRetryable(ex))
    {
        await tx.RollbackAsync();
        if (++attempt >= MaxAttempts)
            throw;
        await Task.Delay(20 * (1 << attempt));
    }
    catch
    {
        await tx.RollbackAsync();
        throw;
    }
}
```

---

## CamusDB.EntityFrameworkCore (EF Core)

### Installation

```shell
dotnet add package CamusDB.EntityFrameworkCore
```

Or via the Package Manager Console:

```shell
Install-Package CamusDB.EntityFrameworkCore
```

### Configuration

Register the provider via `UseCamusDB` in your `DbContext` options:

```csharp
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb;Timeout=30")
    .Options;
```

Or configure it inside `OnConfiguring`:

```csharp
public class AppDbContext : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseCamusDB("Endpoint=http://localhost:8082;Database=mydb");
}
```

The same connection string keys are supported as in `CamusDB.Client` — see the [connection string reference](#configuration) above.

#### Retry on failure

Call `EnableRetryOnFailure` on the `CamusDBDbContextOptionsBuilder` to let EF Core automatically retry `SaveChangesAsync` (and query execution) when a transient serialization conflict is detected. Only the three retryable CamusDB error codes (`CADB0502`, `CADB0504`, `CADB0505`) trigger a retry — all other exceptions propagate immediately.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb", o =>
    {
        o.EnableRetryOnFailure();
    })
    .Options;
```

Default parameters:

| Parameter | Default | Description |
| --- | --- | --- |
| `maxRetryCount` | `15` | Maximum number of retry attempts |
| `maxRetryDelay` | `1 s` | Upper bound on the delay between retries |
| `retryDeadline` | `5 s` | Wall-clock deadline from first failure; no further retries after this |
| `medianFirstRetryDelay` | `30 ms` | Median delay before the first retry |

Override any parameter explicitly:

```csharp
o.EnableRetryOnFailure(
    maxRetryCount: 5,
    maxRetryDelay: TimeSpan.FromMilliseconds(500),
    retryDeadline: TimeSpan.FromSeconds(3),
    medianFirstRetryDelay: TimeSpan.FromMilliseconds(20));
```

> EF Core's execution strategy retries the **entire unit of work** — never only the failing statement. If you manage transactions manually with `BeginTransactionAsync` / `CommitTransactionAsync`, use `SerializableRetryHelper` in `CamusDB.Client` instead.

#### Sharing an existing connection

Pass an open `CamusConnection` when you want to share a connection or attach to an externally managed transaction:

```csharp
CamusConnection connection = await GetOpenConnectionAsync();

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB(connection)
    .Options;

await using var ctx = new AppDbContext(options);
await ctx.Database.BeginTransactionAsync();
// ... SaveChangesAsync, then CommitTransactionAsync
```

The `DbContext` does **not** take ownership of the supplied connection and will not close or dispose it.

### Defining a Model

Use standard EF Core data annotations or the fluent API. Map ID columns to the `"id"` store type and call `ValueGeneratedOnAdd()` so the provider generates a client-side ObjectId automatically:

```csharp
public class Robot
{
    public string Id   { get; set; } = "";
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public int    Year { get; set; }
    public double Price   { get; set; }
    public bool   Enabled { get; set; }
}

public class AppDbContext : DbContext
{
    public DbSet<Robot> Robots => Set<Robot>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Robot>(b =>
        {
            b.ToTable("robots");
            b.HasKey(e => e.Id);
            b.Property(e => e.Id)
             .HasColumnType("id")
             .ValueGeneratedOnAdd();   // client-side ObjectId generation
            b.Property(e => e.Name).HasColumnType("string");
            b.Property(e => e.Type).HasColumnType("string");
            b.Property(e => e.Year).HasColumnType("int64");
            b.Property(e => e.Price).HasColumnType("float64");
            b.Property(e => e.Enabled).HasColumnType("bool");
        });
    }
}
```

Add a `CHECK` constraint with `ToTable(t => t.HasCheckConstraint(...))`. It is enforced server-side on
every insert and update, and rejected rows surface as a `DbUpdateException` whose inner `CamusException`
carries code `CADB0303`:

```csharp
b.ToTable("robots", t => t.HasCheckConstraint("ck_robots_price", "price >= 0"));
```

### CamusDB Type Mapping

| CLR type | CamusDB store type | DDL type |
| --- | --- | --- |
| `string` (ID / PK) | `id` or `oid` | `OID` |
| `Guid` (ID / PK) | `id` or `oid` | `OID` |
| `Guid` + `HasColumnType("uuid")` | `uuid` (alias `guid`) | `UUID` |
| `string` | `string` | `STRING` |
| `string` + `HasMaxLength(n)` | `string` | `STRING(n)` |
| `bool` | `bool` | `BOOL` |
| `short`, `int`, `long` | `int64` | `INT64` |
| `float` | `float32` | `FLOAT32` |
| `double` | `float64` | `FLOAT64` |
| `byte[]` | `bytes` (alias `blob`) | `BYTES` |
| `DateOnly` | `date` | `DATE` |
| `DateTime`, `DateTimeOffset` | `datetime` (alias `timestamp`) | `DATETIME` |
| `long[]`, `string[]`, `double[]`, `bool[]` | `array(int64/string/float64/bool)` | `ARRAY(T)` |

Use `HasColumnType("id")` (or the alias `"oid"`) for primary key columns backed by CamusDB ObjectIds. The provider sends the value as an OID on the wire regardless of whether the CLR property is `string` or `Guid`.

A plain `Guid` property defaults to the `id`/OID store type for backward compatibility. To store it as CamusDB's native `UUID` column instead — indexable and ordered by big-endian byte order — declare it explicitly with `HasColumnType("uuid")`:

```csharp
b.Property(e => e.ExternalRef).HasColumnType("uuid");
```

Dates and datetimes are stored as UTC ticks; the provider normalizes `DateTime` values to UTC before sending and reconstructs them as `DateTimeKind.Utc`. `byte[]` is exchanged as base64 over the JSON wire (SQL literals use `0x`-hex). A `string` property maps to `float32`/`bytes`/`date`/`datetime` etc. either by its CLR type or by an explicit `HasColumnType(...)`.

`long[]`, `string[]`, `double[]`, and `bool[]` properties map to CamusDB native `ARRAY(T)` columns (not EF's JSON "primitive collection" — the provider registers a direct mapping that stores a real array). No `HasColumnType` is required; declare the property as usual:

```csharp
b.Property(e => e.Tags);   // long[]  → ARRAY(INT64)
```

Arrays are not indexable and have no inline SQL literal, so an array value can only travel as a bound parameter — it cannot appear in a `Where(...)` predicate, as a default value, or in migration seed data.

### Database and Table Lifecycle

`EnsureCreatedAsync()` creates the database and all tables defined in the model. Both operations are idempotent — it is safe to call on a database or tables that already exist:

```csharp
await using var ctx = new AppDbContext();
await ctx.Database.EnsureCreatedAsync();
```

`EnsureDeletedAsync()` drops the database entirely:

```csharp
await ctx.Database.EnsureDeletedAsync();
```

### Insert

```csharp
await using var ctx = new AppDbContext();

ctx.Robots.Add(new Robot
{
    Name    = "T-800",
    Type    = "cyborg",
    Year    = 1984,
    Price   = 10.0,
    Enabled = true
});

await ctx.SaveChangesAsync(); // Id is generated automatically
```

### Query

```csharp
await using var ctx = new AppDbContext();

// Key lookup
Robot? robot = await ctx.Robots.FindAsync(id);

// LINQ predicate
List<Robot> active = await ctx.Robots
    .Where(r => r.Enabled && r.Year > 1980)
    .ToListAsync();
```

The provider translates a broad set of LINQ shapes to server-side SQL: `Where`, `OrderBy`/`ThenBy`,
`Skip`/`Take`, `Distinct`, scalar aggregates (`Count`, `Sum`, `Average`, `Min`, `Max`, `Any`),
`GroupBy` + aggregate, inner `join`s, and correlated subqueries (`Where(x => x.Items.Any(...))`).

The following `string` members translate to CamusDB's native scalar/predicate functions and run on
the server (rather than forcing client-side evaluation):

| LINQ | CamusDB function |
| --- | --- |
| `s.StartsWith(x)` / `s.EndsWith(x)` / `s.Contains(x)` | `starts_with` / `ends_with` / `contains` |
| `s.StartsWith(x, StringComparison.OrdinalIgnoreCase)` (and the `…IgnoreCase` variants) | wrapped in `lower(...)` |
| `s.ToUpper()` / `s.ToLower()` | `upper` / `lower` |
| `s.Trim()` / `s.TrimStart()` / `s.TrimEnd()` | `trim` / `ltrim` / `rtrim` |
| `s.Replace(a, b)` | `replace` |
| `s.Length` | `length` |

The `starts_with` / `ends_with` / `contains` predicates take the search term as a plain argument, so
they work with a column or parameter (not just a literal) and need no `LIKE` wildcard escaping.

##### Regular expressions

`System.Text.RegularExpressions.Regex` static calls and the `EF.Functions.Regexp*` helpers map onto
CamusDB's native regex operators and `regexp_*` scalar functions (PostgreSQL semantics):

| LINQ | CamusDB |
| --- | --- |
| `Regex.IsMatch(s, pattern)` | `regexp_like(s, pattern)` (the `~` operator) |
| `Regex.IsMatch(s, pattern, RegexOptions.IgnoreCase)` | `regexp_like(s, pattern, 'i')` (the `~*` operator) |
| `Regex.Replace(s, pattern, repl)` | `regexp_replace(s, pattern, repl, 'g')` (replaces every match, like .NET) |
| `EF.Functions.RegexpLike(s, pattern[, flags])` | `regexp_like` |
| `EF.Functions.RegexpReplace(s, pattern, repl[, flags])` | `regexp_replace` (first match; pass `"g"` to replace all) |
| `EF.Functions.RegexpCount(s, pattern[, flags])` | `regexp_count` |
| `EF.Functions.RegexpSubstr(s, pattern[, flags])` | `regexp_substr` |
| `EF.Functions.RegexpInstr(s, pattern[, flags])` | `regexp_instr` |

```csharp
using System.Text.RegularExpressions;

var valid = await ctx.People
    .Where(p => Regex.IsMatch(p.Name, "^[A-Z]"))                                   // name ~ '^[A-Z]'
    .Where(p => EF.Functions.RegexpLike(p.Email, "^[^@]+@[^@]+\\.[a-z]+$", "i"))   // email ~* '…'
    .ToListAsync();
```

The `flags` string uses PostgreSQL option characters: `i` case-insensitive, `c` case-sensitive,
`m`/`n` multiline, `s` single-line (dot matches newline), `x` extended, and `g` replace-all (for
`RegexpReplace`). A `RegexOptions` argument to a `Regex` call is honored only when it is a compile-time
constant made of flags CamusDB can express (`IgnoreCase`, `Multiline`, `Singleline`,
`IgnorePatternWhitespace`); anything else leaves the call untranslated. Because these are equivalent to
CHECK-constraint predicates, the same patterns work in `HasCheckConstraint` (e.g.
`t.HasCheckConstraint("ck_email", "email ~* '^[^@]+@[^@]+$'")`). The set-returning functions
`regexp_matches` / `regexp_split_to_table` are not supported by the server.

> **Not translatable (server limitations):** `Include`/collection navigations and left/optional joins
> — CamusDB supports only `INNER JOIN` (no `LEFT`/`OUTER JOIN`) — and set operators (`Union`/`Concat`)
> — CamusDB has no `UNION`. These raise a translation or SQL error; project the shape with an explicit
> inner `join` or issue separate queries instead.

#### Query Result Cache

Opt a LINQ query into CamusDB's [query result cache](#query-result-cache) with `WithCache(name)`.
The provider injects the `{cache=…}` hint into the generated SQL, so an identical later query is
served from the server's in-memory cache. As with the raw client, only single-table, autocommit
reads are cacheable — a query with a join, or one inside an explicit transaction, reads live storage.

```csharp
using CamusDB.EntityFrameworkCore;

// Cache with the server's default TTL
List<Order> recent = await ctx.Orders
    .Where(o => o.Status == 1)
    .OrderByDescending(o => o.Total)
    .Take(20)
    .WithCache("recent_orders")
    .ToListAsync();

// Per-entry TTL override and strict per-hit validation
List<Order> hot = await ctx.Orders
    .Where(o => o.Status == 1)
    .WithCache("hot_orders", ttl: TimeSpan.FromSeconds(30), strict: true)
    .ToListAsync();
```

Evict entries through the underlying `CamusConnection` (`EvictCacheAsync` / `EvictAllCacheAsync`).

### Update

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

robot.Price = 99.0;
await ctx.SaveChangesAsync();
```

### Delete

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

ctx.Robots.Remove(robot);
await ctx.SaveChangesAsync();
```

### Migrations

The provider supports EF Core migrations for the following DDL operations:

| Operation | Generated SQL |
| --- | --- |
| Create table | `CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col1, ...))` |
| Drop table | `DROP TABLE t` |
| Rename table | `ALTER TABLE t RENAME TO new_name` |
| Add column | `ALTER TABLE t ADD COLUMN col TYPE [NOT NULL] [DEFAULT (value)]` |
| Drop column | `ALTER TABLE t DROP COLUMN col` |
| Rename column | `ALTER TABLE t RENAME COLUMN old TO new` |
| Alter column nullability | `ALTER TABLE t ALTER COLUMN col SET NOT NULL` / `DROP NOT NULL` |
| Add check constraint | `ALTER TABLE t ADD CONSTRAINT name CHECK (expr)` |
| Drop check constraint | `ALTER TABLE t DROP CONSTRAINT name` |
| Create index | `CREATE INDEX name ON t (col1, col2)` |
| Create unique index | `CREATE UNIQUE INDEX name ON t (col1, col2)` |
| Drop index | `ALTER TABLE t DROP INDEX name` |
| Rename index | `ALTER TABLE t RENAME INDEX old TO new` |
| Raw SQL | passed through as-is |

A `CHECK` constraint declared with `ToTable(t => t.HasCheckConstraint(...))` is emitted as a
table-level `CONSTRAINT name CHECK (expr)` inside `CREATE TABLE` (both via migrations and
`EnsureCreated`), and can be added or dropped later with `migrationBuilder.AddCheckConstraint` /
`DropCheckConstraint`. Changing a property's nullability (with the column type unchanged) maps to
CamusDB's `ALTER COLUMN … SET/DROP NOT NULL`; `DropCheckConstraint` also drops a named `NOT NULL`
constraint by name, since CamusDB resolves `DROP CONSTRAINT` against both.

The provider ships design-time services so the EF tooling can discover the provider automatically. No extra flags are needed:

```shell
dotnet ef migrations add InitialCreate
dotnet ef database update
```

Example migration using the supported operations:

```csharp
public partial class AddStockColumn : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<int>(
            name: "Stock",
            table: "products",
            type: "int64",
            nullable: false,
            defaultValue: 0);

        migrationBuilder.CreateIndex(
            name: "idx_products_name",
            table: "products",
            column: "Name",
            unique: true);

        migrationBuilder.AddCheckConstraint(
            name: "ck_products_stock",
            table: "products",
            sql: "Stock >= 0");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropCheckConstraint(name: "ck_products_stock", table: "products");
        migrationBuilder.DropIndex(name: "idx_products_name", table: "products");
        migrationBuilder.DropColumn(name: "Stock", table: "products");
    }
}
```

### Concurrency Tokens

The provider supports EF Core optimistic concurrency. When a tracked entity has a concurrency token,
its previously-loaded value is added to the UPDATE's `WHERE` clause; a stale write matches zero rows
on the server and the provider raises `DbUpdateConcurrencyException`.

**`[Timestamp]` / `IsRowVersion()`** — a `byte[]` row version. CamusDB has no server-side auto-version,
so the provider generates a fresh, strictly-increasing token on every insert and update and sends it
(stored as a `BYTES` column). No manual bookkeeping is required:

```csharp
public class Doc
{
    public string Id      { get; set; } = "";
    public string Body    { get; set; } = "";
    [Timestamp]
    public byte[] Version { get; set; } = [];   // provider-managed row version
}

doc.Body = "edited";
await ctx.SaveChangesAsync();                    // throws DbUpdateConcurrencyException if stale
```

**`[ConcurrencyCheck]`** on a numeric column (`short`, `int`, `long`) — the application increments the
version before `SaveChanges()`:

```csharp
public class Order
{
    public string Id      { get; set; } = "";
    public string Status  { get; set; } = "";
    [ConcurrencyCheck]
    public long Version   { get; set; }
}

order.Status = "shipped";
order.Version++;                                 // manual bump
await ctx.SaveChangesAsync();                    // throws DbUpdateConcurrencyException if stale
```

> **Note on MVCC vs. optimistic concurrency:** the `WHERE`-clause check above is evaluated at write
> time, so a stale `SaveChangesAsync` fails immediately with `DbUpdateConcurrencyException`. This is
> distinct from CamusDB's MVCC write-write conflict detection between two *open explicit transactions*,
> which surfaces at **commit time** (`CommitTransactionAsync`). Use a concurrency token for
> application-level stale-update detection.

### Provider Limitations

- No computed columns.
- No foreign key constraints.
- No `LEFT`/`OUTER JOIN` — CamusDB supports only `INNER JOIN`. `Include`/collection navigations and optional-reference joins do not translate; use an explicit inner `join` projection.
- No `UNION`/`UNION ALL` — LINQ `Union`/`Concat` do not translate; issue separate queries.
- `ALTER COLUMN` only supports toggling nullability (`SET`/`DROP NOT NULL`); changing a column's stored type requires dropping and recreating the column.
- `CHECK` conditions must be deterministic single-row predicates — no subqueries, aggregates, or volatile functions (`now()`, `gen_uuid_v4/v7()`, …); a violated check surfaces as a `CamusException` with code `CADB0303` (wrapped in `DbUpdateException` under EF Core), and a NULL operand makes the predicate pass (SQL three-valued logic).
- `array(T)` columns map for `long[]`, `string[]`, `double[]`, and `bool[]`. They are not indexable and have no SQL literal, so an array can only be written/read as a whole value — it cannot appear in a `Where` predicate or as a default/seed value.
- No `decimal`/exact-numeric store type — use `double` (`float64`), accepting binary floating-point rounding.
- Key CLR types must be one of: `string`, `int`, `long`, `short`, or `Guid`.
- `[ConcurrencyCheck]` is supported on `short`/`int`/`long`; `[Timestamp]`/`IsRowVersion()` is supported on `byte[]` (provider-managed). A stale write raises `DbUpdateConcurrencyException`.
- `WithCache(...)` only takes effect on single-table, autocommit reads; the hint is inert on queries with a join or run inside an explicit transaction (they read live storage).

---

## Run Tests

To run the unit tests, a CamusDB instance must be running locally. After starting it, run:

```shell
dotnet test -l "console;verbosity=normal" --filter "FullyQualifiedName~CamusDB.Client.Tests"
```

## Contribution

CamusDB.Client is an open-source project, and contributions are heartily welcomed! Whether you are looking to fix bugs, add new features, or improve documentation, your efforts and contributions will be appreciated. Check out the CONTRIBUTING.md file for guidelines on how to get started with contributing to CamusDB.Client.

## License

CamusDB.Client is released under the MIT License.
