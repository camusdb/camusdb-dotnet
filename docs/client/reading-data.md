# Reading data

How to run a `SELECT`, stream a large result, and use the query result cache.

## Select Rows

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

## Streaming large results

`ExecuteReaderAsync` loads the whole result into memory. `ExecuteStreamReaderAsync` returns the same reader type, but fetches rows as you call `ReadAsync`. The schema (`FieldCount`, `GetName`, `HasRows`) is available before the first row.

```csharp
await using CamusCommand select = connection.CreateSelectCommand(
    "SELECT * FROM robots WHERE year > @year");

select.Parameters.Add("@year", ColumnType.Integer64, 1900);

// Dispose the reader to release the HTTP response or gRPC call.
await using CamusDataReader reader = await select.ExecuteStreamReaderAsync();

while (await reader.ReadAsync())
{
    string name = reader.GetString(1);
    long   year = reader.GetInt64(3);
}
```

Differences from `ExecuteReaderAsync`:

- **No automatic serializable retry.** Rows can arrive before the autocommit transaction commits, so a late conflict is thrown from `ReadAsync` as a `CamusException`. For retry, use `ExecuteReaderAsync` or an explicit transaction.
- `CacheMetadata` is null, and `RecordsAffected` is `-1`.
- **gRPC:** `CommandTimeout` limits each wait for the next row, not the whole read. The statement runs inline, even after `Prepare`.
- **DML:** a write with a `RETURNING` list streams its rows, sent only after the commit. Other DML returns just the affected-row count. See [RETURNING](writing-data.md#returning).

## Query Result Cache

The result cache (opt-in, in memory, per server node) stores complete `SELECT` results. Put a `{cache=name}` hint right after a table reference. A later query with the same shape, bound values and schema can then be served from memory. Only single-table autocommit reads use it. A hint on a join, a view, or in an explicit transaction is ignored, and `BypassReason` says why.

`CamusCacheHint.Build(name, ttl, strict)` builds the hint:

| Argument | Meaning |
| --- | --- |
| `name` | Cache family name; case-insensitive. Letters, digits and `_ - . :` only, at most 128 characters, else `ArgumentException`. The name is not escaped in SQL, so validate names from user input. |
| `ttl` | Optional time to live per entry; positive, at most `int.MaxValue` ms. |
| `strict` | Optional; checks every hit against live storage. |

```csharp
string hint = CamusCacheHint.Build("recent_orders", ttl: TimeSpan.FromSeconds(30));

await using CamusCommand select = connection.CreateSelectCommand(
    $"SELECT id, total FROM orders {hint} WHERE status = @status ORDER BY total DESC LIMIT 20");

select.Parameters.Add("@status", ColumnType.Integer64, 1);

CamusDataReader reader = await select.ExecuteReaderAsync();

CamusCacheMetadata? cache = reader.CacheMetadata;   // also on select.LastCacheMetadata
if (cache is not null)
    Console.WriteLine($"{cache.Status} ({cache.Name}), age={cache.AgeMs}ms");
```

`CamusCacheMetadata` has `Status` (`Hit`, `Miss`, `Bypass`, `StaleRevalidated`, `EvictedBeforePublish`), `BypassReason`, `Name`, `CachedAtHlc` (hybrid logical clock time of caching) and `AgeMs`. It is `null` for a query without a hint. Both transports report it, on `ExecuteReaderAsync` only.

Evict entries in the current database:

```csharp
await connection.EvictCacheAsync("recent_orders");   // one family
await connection.EvictAllCacheAsync();               // every entry for this database
```
