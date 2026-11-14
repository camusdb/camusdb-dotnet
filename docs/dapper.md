# CamusDB.Dapper

[Dapper](https://github.com/DapperLib/Dapper) works with `CamusConnection` as is. The `CamusDB.Dapper` package adds the CamusDB types Dapper does not know.

## Installation

```shell
dotnet add package CamusDB.Dapper
```

## Usage

```csharp
using CamusDB.Client;
using CamusDB.Dapper;
using Dapper;

// Once at startup: registers the handlers for CamusObjectIdValue, TimeSpan and CamusInterval
// (interval text), and for the JsonDocument, JsonElement, JsonNode, JsonObject and JsonArray
// document types (bound and read as JSONB).
CamusDapper.Register();

await using CamusConnection connection = new("Endpoint=http://localhost:5095;Database=test");

IEnumerable<Robot> robots = await connection.QueryAsync<Robot>(
    "SELECT * FROM robots WHERE year >= @from ORDER BY year",
    new { from = 1977 });

int inserted = await connection.ExecuteAsync(
    "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), @Name, @Year)",
    new[] { new { Name = "R2-D2", Year = 1977 }, new { Name = "T-800", Year = 1984 } });

long count = await connection.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM robots");
```

Dapper sends a bare name such as `from`; the driver adds the `@`, so it binds `@from`.

Parameters bind by CLR type, per [Data Types](client/data-types.md#data-types): `Guid` as `UUID`, `DateOnly` as `DATE`, an enum as its integer, a `TimeSpan` or `CamusInterval` as interval text for `CAST(@p AS interval)` ([INTERVAL](client/data-types.md#interval)).

## Typed values: `CamusValue`

`CamusValue` binds a column type the CLR type cannot express:

| Value | Binds as |
| --- | --- |
| `CamusValue.Id("65f1…")` | `OID` from hex. A plain `string` binds as `STRING`. |
| `CamusValue.Uuid(guid)` | `UUID`, also for `null`. |
| `CamusValue.Numeric("123…")` | `NUMERIC` from text, for values too large for `decimal` (which binds as `NUMERIC` already). |
| `CamusValue.Jsonb("{…}")` | `JSONB`. Also takes `JsonDocument`, `JsonElement`, `JsonNode`. |
| `CamusValue.Array(values)` | One `ARRAY(T)`, typed by `T`, so empty arrays work. |
| `CamusValue.Vector(floats)` | Float32 vector in `BYTES` (see [Vector search](efcore/querying.md#vector-search)). |
| `new CamusValue(ColumnType.X, value)` | Any column type. |

```csharp
IEnumerable<string> names = await connection.QueryAsync<string>(
    "SELECT name FROM robots WHERE year = ANY(@years)",
    new { years = CamusValue.Array(new long[] { 1977, 2001 }) });
```

`CamusValue` also works in `DynamicParameters`.

## Arrays and `IN` lists

Dapper expands `IN @names` into `IN (@names1, @names2, …)`, which works as is. To read an `ARRAY(T)` column into `T[]` (else it reads as `object[]`), register a type handler:

```csharp
CamusDapper.RegisterArrayTypeHandlers();       // long[], int[], double[], bool[], string[], Guid[]
CamusDapper.RegisterArrayTypeHandler<float>(); // any other element type
```

A `T[]` parameter then binds as one array, so use `= ANY(@values)` instead of `IN`. `List<T>` still expands.

## Limitations

- Dapper buffers whole results. For huge ones, use `ExecuteStreamReaderAsync` with `reader.GetRowParser<T>()`.
- Type handlers are process-wide. Register once at startup.
- `CommandType.StoredProcedure` sends `CALL name(@a, @b, ...)`, with arguments in property order.
