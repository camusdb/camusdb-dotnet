# Prepared statements

A prepared statement is registered on the server once, then run many times with new values. The server skips parsing it again. REST and gRPC both support it.

## Automatic preparation

The driver prepares a statement the second time it sees the same SQL. You do nothing:

```csharp
// Nothing here mentions preparing. The second and later executions are prepared automatically.
for (int i = 0; i < 100; i++)
{
    await using CamusCommand cmd = connection.CreateCamusCommand(
        "SELECT name FROM robots WHERE year = @year");
    cmd.Parameters.Add("@year", ColumnType.Integer64, 1984);

    await using CamusDataReader reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
        Console.WriteLine(reader.GetString(0));
}
```

EF Core benefits with no setup. The count is shared across connections to one deployment, so a request-scoped `DbContext` also reaches it.

## Explicit preparation

`Prepare()` or `PrepareAsync()` prepares a statement before its first run:

```csharp
await using CamusCommand insert = connection.CreateCamusCommand(
    "INSERT INTO robots (id, name, year) VALUES (@id, @name, @year)");

await insert.PrepareAsync();

foreach (Robot robot in robots)
{
    insert.Parameters.Clear();
    insert.Parameters.Add("@id", ColumnType.Id, robot.Id);
    insert.Parameters.Add("@name", ColumnType.String, robot.Name);
    insert.Parameters.Add("@year", ColumnType.Integer64, robot.Year);

    await insert.ExecuteNonQueryAsync();
}
```

## Settings

| Key | Default | Meaning |
| --- | --- | --- |
| `MaxAutoPrepare` | `128` | Most statements kept prepared. `0` turns automatic preparation off. |
| `AutoPrepareMinUsages` | `2` | Runs before a statement is prepared. |

```csharp
// Prepare on first sight, and keep more statements registered.
CamusConnectionStringBuilder eager = new(
    "Endpoint=http://localhost:5095;Database=test;MaxAutoPrepare=512;AutoPrepareMinUsages=1");

// Turn automatic preparation off entirely. Explicit Prepare() still works.
CamusConnectionStringBuilder off = new(
    "Endpoint=http://localhost:5095;Database=test;MaxAutoPrepare=0");
```

At the limit, the driver drops the least recently used statement. SQL that never repeats, such as concatenated values, then runs unprepared.

`MaxAutoPrepare` counts per deployment, not per connection. The server's own limit is per login, across all its databases. Size `MaxAutoPrepare` for that shared budget.

## Behavior

- Results, transactions and locking are the same as unprepared.
- Parameter names must match placeholders exactly, `@` included.
- Preparable: `SELECT`, `INSERT`, `UPSERT`, `UPDATE`, `DELETE`, `SHOW`, and a query that starts with `(`. On other statements `Prepare()` does nothing.
- Preparing never fails a statement. If the server refuses, the statement runs unprepared.
- The driver re-prepares automatically when a node or gRPC stream forgets a statement.

`CamusConnectionStringBuilder.PreparedStatementCount` and `IsPrepared(sql)` report what is prepared now.
