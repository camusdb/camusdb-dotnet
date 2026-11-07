# Databases, ping and DDL

Databases, ping, and DDL (schema SQL).

## Database Management

Create a database before use. Default name: the connection string's.

```csharp
await connection.CreateDatabaseAsync(ifNotExists: true);
await connection.DropDatabaseAsync();
await connection.CreateDatabaseAsync("otherdb", ifNotExists: true);
await connection.DropDatabaseAsync("otherdb");
```

## Ping

Returns `1` if up, else `0`; throws if unreachable.

```csharp
await using CamusCommand ping = connection.CreatePingCommand();

int result = await ping.ExecuteNonQueryAsync();
```

## Execute DDL

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

`CHECK` fails (`CADB0303`) only on `FALSE`; `NULL` passes. Named `NOT NULL`: `CADB0301`.

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

## Truncate a table

`TRUNCATE TABLE t` (or `TRUNCATE t`) removes all rows of a table at once. Run it with `ExecuteDDLAsync` or `ExecuteNonQueryAsync`:

```csharp
await connection.CreateCamusCommand("TRUNCATE TABLE robots").ExecuteDDLAsync();
```

`TRUNCATE` runs in its own transaction on the server. A command with a `Transaction` throws `CADB0538` before it sends the statement, and a [pipeline](transactions.md#pipelined-transactions) cannot queue it. For details, see [Empty a table](../efcore/saving-data.md#empty-a-table).

