# Writing data

Inserts, `RETURNING`, and key conflicts.

## Insert Rows

`CreateInsertCommand` takes a table name; each parameter names a column. It works on REST and gRPC.

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

Or run parameterized SQL:

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

Other types: [Data types](data-types.md).

## RETURNING

`RETURNING` makes an `INSERT`, `UPDATE` or `DELETE` return the stored values (with defaults and sequence values) of the rows it wrote.

```csharp
await using CamusCommand insert = connection.CreateCamusCommand("""
    INSERT INTO robots (id, name, year) VALUES (GEN_ID(), @name, @year)
    RETURNING id, name, year * 2 AS doubled
    """);

insert.Parameters.Add("@name", ColumnType.String, "R2-D2");
insert.Parameters.Add("@year", ColumnType.Integer64, 1977);

await using CamusDataReader reader = await insert.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    string id      = reader.GetString(0);
    long   doubled = reader.GetInt64(2);
}

int insertedRows = reader.RecordsAffected; // one row for each inserted row
```

`UPDATE` returns the new row; `DELETE` returns the deleted row. `RETURNING` goes after any `LIMIT`:

```csharp
await using CamusCommand update = connection.CreateCamusCommand(
    "UPDATE robots SET year = year + 1 WHERE year < @year RETURNING id, year");

update.Parameters.Add("@year", ColumnType.Integer64, 1980);

await using CamusDataReader reader = await update.ExecuteReaderAsync();

await using CamusCommand delete = connection.CreateCamusCommand(
    "DELETE FROM robots WHERE year > 2000 LIMIT 10 RETURNING *");
```

- `ExecuteScalarAsync` returns the first column of the first row.
- `ExecuteNonQueryAsync` returns only the count; the server does not send the rows.
- Aggregates, subqueries and sequence functions are not allowed in the list.
- It needs the `SELECT` privilege, also with `ExecuteNonQueryAsync`.
- Only `INSERT` returns rows in a defined (insert) order.
- `UPDATE` cannot return old values (no `OLD`/`NEW`).
- `returning` is reserved; put a column or table with that name in backticks.
- On gRPC, a buffered reply over 4 MiB fails with `CADB0550` and stores nothing. Use `ExecuteStreamReaderAsync` ([streaming](reading-data.md#streaming-large-results)); there `RecordsAffected` is `-1`.

Full rules: `docs/returning.md` in the server repository.

## INSERT … ON CONFLICT and UPSERT

On a unique-key clash, `ON CONFLICT … DO NOTHING` skips the row; `DO UPDATE` updates the stored row. `UPSERT INTO` does the same on the primary key. In `SET`, a bare column is the stored row; `excluded.c` is the proposed row.

```csharp
await using CamusCommand hit = connection.CreateCamusCommand("""
    INSERT INTO counters (k, n) VALUES (@k, 1)
    ON CONFLICT (k) DO UPDATE SET n = counters.n + excluded.n
    RETURNING n
    """);

hit.Parameters.Add("@k", ColumnType.String, "hits");

long hits = (long)(await hit.ExecuteScalarAsync())!; // 1 the first time, then 2, 3, …

await using CamusCommand upsert = connection.CreateCamusCommand(
    "UPSERT INTO counters (k, n) VALUES (@k, @n)");
```

- Both are writes on every `Execute*` method, pipeline and prepared statement, with the connection's autocommit options.
- The count is inserted plus updated rows. A skipped row is not counted and returns no `RETURNING` row.
- `DO UPDATE` and `UPSERT` need the `INSERT` and `UPDATE` privileges.
- When two transactions write the same new key, the second fails with retryable `CADB0502` (`CADB0504` at the lock-wait deadline); autocommit retries it.
- A duplicate on a unique index other than the conflict target fails with `DuplicateUniqueKeyValue`, not retried.
- `CreateInsertCommand` cannot upsert; use SQL.

Full rules: `docs/insert-on-conflict.md` in the server repository.
