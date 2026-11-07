# Data types

Column types and their .NET mappings, `NUMERIC`, `JSONB`, large-value storage, and identifier quoting.

## Type mapping

| SQL DDL type | `ColumnType` | CLR type(s) | Notes |
| --- | --- | --- | --- |
| `OID` (`OBJECT_ID`) | `Id` | `string`, `CamusObjectIdValue` | Native identifier. A `Guid` is always sent as `UUID`. |
| `UUID` (`GUID`) | `Uuid` | `Guid` | Indexable; sorts by big-endian bytes. `gen_uuid_v4()`/`gen_uuid_v7()` generate values. |
| `INT64` (`INT`, `INTEGER`) | `Integer64` | `long`, `int`, `short`, `byte` | 64-bit signed. |
| `FLOAT64` | `Float64` | `double` | IEEE-754 double. |
| `NUMERIC` (`DECIMAL`) | `Numeric` | `decimal`, `string` | Exact; 38 digits, 9 after the point. See [NUMERIC](#numeric). |
| `FLOAT32` (`REAL`) | `Float32` | `float` | Single precision. |
| `BOOL` (`BOOLEAN`) | `Bool` | `bool` | |
| `STRING` / `STRING(N)` | `String` | `string` | `N`: max length in UTF-16 units; longer is rejected. |
| `DATE` | `Date` | `DateOnly`, `DateTime` | Calendar date, stored as midnight UTC. |
| `DATETIME` (`TIMESTAMP`) | `DateTime` | `DateTime`, `DateTimeOffset` | Converted to UTC; reads as `DateTimeKind.Utc`. |
| `BYTES` (`BLOB`) | `Bytes` | `byte[]` | Base64 in JSON, `0x` hex in SQL. Default max 10 MB. |
| `ARRAY(T)` | `Array` | `IEnumerable` of `T` | One scalar type. Not indexable; no SQL literal. |
| `JSONB` (`JSON`) | `Jsonb` | `JsonDocument`, `JsonElement`, `JsonNode`, `string` | One JSON document. See [JSONB](#jsonb). |

Aliases are in parentheses.

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

Dates are sent as UTC. An array's element type is inferred from its values; set it for an empty array.

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

```csharp
Guid      reference = reader.GetGuid(reader.GetOrdinal("ref"));
byte[]    payload  = reader.GetFieldValue<byte[]>(reader.GetOrdinal("payload"));
float     score    = reader.GetFloat(reader.GetOrdinal("score"));
DateTime  happened = reader.GetDateTime(reader.GetOrdinal("happened"));
DateOnly  day      = reader.GetFieldValue<DateOnly>(reader.GetOrdinal("day"));
object?[] tags     = (object?[])reader.GetValue(reader.GetOrdinal("tags"));
```

REST and gRPC return the same values:

- Array elements keep their type (`DATE`, `BYTES`, `UUID`, `FLOAT64` zero). Over REST an older server omits `elementType`; elements are then typed from JSON, so a date or whole float reads as `Integer64`, bytes as `String`, a UUID as null.
- `FLOAT32` reads back as the stored single-precision value; `GetDouble` widens it exactly.
- `-0.0` keeps its sign in parameters and results.
- `Infinity`, `-Infinity` and `NaN` work as values and parameters. REST carries them as those strings in float cells. An older server refuses them over REST.

### NUMERIC

`NUMERIC` (`DECIMAL`) has fixed precision 38 (total digits) and scale 9 (digits after the point), so `NUMERIC(P, S)` is refused. Range: ±`99999999999999999999999999999.999999999`. Values travel as decimal text, never as doubles.

- A `decimal` parameter binds as `NUMERIC`, also with no `ColumnType` and with `DbType.Decimal`.
- A `string` (or `long`, `int`, finite `double`) typed `ColumnType.Numeric` binds as `NUMERIC`. Use a string for values a `decimal` cannot hold.
- The server rounds past 9 fraction digits half away from zero. Out of range fails with `CADB0417`.
- A `decimal[]` binds as `ARRAY(FLOAT64)`; there is no `ARRAY(NUMERIC)`.
- In a `FLOAT64` column, or compared with `FLOAT64`, a `NUMERIC` becomes a double.
- `GetDecimal`, `GetValue` and `GetFieldValue<decimal>` throw `InvalidCastException` for a value over 28-29 significant digits; they never round. `GetString` returns the exact text. `GetDouble` rounds.

```csharp
insert.Parameters.Add("price", ColumnType.Numeric, 19.99m);
insert.Parameters.Add("total", ColumnType.Numeric, "12345678901234567890.123456789");

decimal price = reader.GetDecimal(0);
string exact = reader.GetString(1);
```

### JSONB

`JSONB` (`JSON`) stores one parsed JSON document. It follows PostgreSQL `jsonb`: operators `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `||`, `-`, `#-`, the `jsonb_*` functions, and GIN indexes (inverted indexes over document keys and values).

- `JsonDocument`, `JsonElement` and `JsonNode` parameters bind as `JSONB` without a `ColumnType`.
- A `string` binds as `JSONB` only when typed `ColumnType.Jsonb`; otherwise it is a `STRING`, which a `jsonb` column parses on write. Text compared with a document compares as a document.
- Invalid JSON fails with `CADB0418`; nesting over 512 levels fails with `CADB0419`.
- There is no `ARRAY(JSONB)`: bind one JSON array. A list of `JsonDocument` fails with `CADB0400`.
- `GetString` and `GetValue` return PostgreSQL text form: a space after each `:` and `,`, keys in stored order (shorter first), numbers with every digit. `GetFieldType` is `string`.
- `GetFieldValue<T>` parses into `JsonDocument`, `JsonElement`, `JsonNode`, `JsonObject` or `JsonArray`; the wrong kind for the last two throws `InvalidCastException`.
- JSON `null` reads as the text `null`; SQL `NULL` reads as `DBNull`. Over REST an older server returns a top-level JSON `null` as SQL `NULL`.
- `CamusJsonb.Format(json)` returns the PostgreSQL text form of any JSON.

```csharp
await connection.CreateCamusCommand("""
    CREATE TABLE events (id OID PRIMARY KEY NOT NULL, payload JSONB NOT NULL)
    """).ExecuteDDLAsync();

// A GIN index serves @>, <@, ?, ?|, ?& and the equality of one key in a WHERE.
await connection.CreateCamusCommand(
    "CREATE INDEX events_payload_gin ON events USING GIN (payload)").ExecuteDDLAsync();

await using CamusCommand insert = connection.CreateInsertCommand("events");
insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("payload", ColumnType.Null, new JsonObject { ["kind"] = "login", ["user"] = new JsonObject { ["id"] = 7 } });
await insert.ExecuteNonQueryAsync();

await using CamusCommand select = connection.CreateCamusCommand(
    "SELECT payload, payload -> 'user' ->> 'id' FROM events WHERE payload @> @filter");
select.Parameters.Add("filter", ColumnType.Jsonb, """{"kind": "login"}""");

await using CamusDataReader reader = await select.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    JsonElement payload = reader.GetFieldValue<JsonElement>(0);   // {"kind": "login", "user": {"id": 7}}
    string userId = reader.GetString(1);                           // "7"
}
```

`ColumnType.Null` means "not set", so the type is inferred.

The GIN index finds candidate rows, which the server then checks. It is not used under `OR` or `NOT`, in a join, or for `@>` with `{}`, `[]` or a scalar. `EXPLAIN` shows `gin-scan(...)` when it is used. If a tool rewrites `?` as a placeholder, use `jsonb_exists`, `jsonb_exists_any` and `jsonb_exists_all`.

## Large values

A server with large-value storage can compress (LZ4) a large `string`, `bytes` or `ARRAY(T)` value, or store it outside the row. Results and parameters do not change. A query that does not name an out-of-row column never fetches it, and updating another column does not rewrite it.

| Strategy | Compresses | Moves out of the row | Use it for |
| --- | --- | --- | --- |
| `EXTENDED` (default) | When it pays | At or above `large_value_threshold_bytes` (2048) | Text, JSON, documents |
| `MAIN` | When it pays | Never | Compressible values that a typical query reads |
| `EXTERNAL` | Never | At or above the threshold | Large incompressible values that queries often skip |
| `PLAIN` | Never | Never | Embeddings that a KNN query reads on every row |

Set it in the column definition or with `SET STORAGE`. `CamusColumnStorage.ToSql()` gives the keyword:

```csharp
await connection.CreateCamusCommand("""
    CREATE TABLE docs (
        id         OID PRIMARY KEY NOT NULL,
        title      STRING,
        body       STRING,
        thumbnail  BYTES STORAGE EXTERNAL,
        embedding  BYTES(3072) STORAGE PLAIN
    )
    """).ExecuteDDLAsync();

await connection.CreateCamusCommand(
    $"ALTER TABLE docs ALTER COLUMN thumbnail SET STORAGE {CamusColumnStorage.Plain.ToSql()}").ExecuteDDLAsync();
```

`SET STORAGE` affects only future writes. `REWRITE STORAGE` converts existing rows. Before you downgrade to a server without large-value storage, run `REWRITE STORAGE INLINE` (all values in-row, uncompressed) on every table.

```csharp
await using CamusCommand rewrite = connection.CreateCamusCommand("ALTER TABLE docs REWRITE STORAGE");
rewrite.CommandTimeout = 3600; // the time is proportional to the table
await rewrite.ExecuteDDLAsync();
```

- The rewrite runs in its own server transactions; `ROLLBACK` does not undo it.
- It is idempotent and resumes after the last committed batch.
- It never overwrites user writes, but a concurrent write can fail with retryable `CADB0502`.
- A 768-dimension float32 embedding (3072 bytes) exceeds the threshold; under `EXTENDED` each KNN batch pays an extra fetch. Use `STORAGE PLAIN`.
- Each out-of-row value is one more key, so inserting or deleting a row with `k` such values costs `k` extra mutations. Bulk inserts fit fewer rows per transaction.
- A read without a snapshot that keeps seeing a row change fails with retryable `CADB0504` (`TransactionMustRetry`), which `SerializableRetryHelper` and `EnableRetryOnFailure` retry.

| Code | Name | Meaning |
| --- | --- | --- |
| `CADB0414` | `ColumnStorageNotApplicable` | Storage strategy on a fixed-length column type. |
| `CADB0540` | `LargeValueCorrupt` | A stored value failed to decompress or its checksum. |
| `CADB0541` | `LargeValueNotResolved` | Server defect; report it. |

## Identifiers and inline literals

Parameter values never go into SQL text. The driver composes SQL only for gRPC admin DDL (`CREATE DATABASE`, `DROP DATABASE`, `SHOW BRANCHES`), EF Core migration and `EnsureCreated` DDL, and cache hints. There:

- identifiers use backticks, and a name containing a backtick is refused (the server does not decode a doubled backtick);
- string literals double their quotes, and a value with a backslash before a quote or at the end is refused (the lexer would end the literal early).

Both throw `ArgumentException` naming the value. This matters for names or seed data built at runtime, such as tenant provisioning.
