# C# extensions: procedures, triggers and functions

The server can run C# from an extension assembly that the operator deploys, or from an [inline function](#inline-functions) whose C# body is in the SQL. The driver needs no setup.

## Procedures

`CALL` runs a procedure. `ExecuteNonQueryAsync` returns the rows it changed:

```csharp
await using CamusCommand call = connection.CreateCamusCommand("CALL transfer(@from, @to, @amount)");
call.Parameters.Add("@from", ColumnType.String, "alice");
call.Parameters.Add("@to", ColumnType.String, "bob");
call.Parameters.Add("@amount", ColumnType.Integer64, 30L);

int changedRows = await call.ExecuteNonQueryAsync();
```

With `CommandType.StoredProcedure`, `CommandText` is the name. Parameters pass by position, in collection order:

```csharp
await using CamusCommand call = connection.CreateCamusCommand("transfer");
call.CommandType = CommandType.StoredProcedure;
call.Parameters.Add("from", ColumnType.String, "alice");
call.Parameters.Add("to", ColumnType.String, "bob");
call.Parameters.Add("amount", ColumnType.Integer64, 30L);

await call.ExecuteNonQueryAsync();
```

- A name other than letters, digits and `_` (not starting with a digit) throws `InvalidOperationException`.
- Input parameters only; no output or return value.
- A reader gets no rows; `RecordsAffected` is the count.
- `CALL` is never prepared. It joins the current transaction. An error rolls back its writes.
- `CALL` needs a superuser.
- `CommandType.TableDirect` throws `NotSupportedException`.

## Triggers and functions

A trigger runs on row insert, update or delete. `CREATE TRIGGER` and `DROP TRIGGER` are DDL:

```csharp
await using CamusCommand create = connection.CreateCamusCommand(
    "CREATE TRIGGER audit ON orders AFTER INSERT, UPDATE, DELETE EXTERNAL NAME 'MyCompany.Ext.AuditTrigger'");
await create.ExecuteNonQueryAsync();
```

Call a function like a built-in: `SELECT slugify(title) FROM posts`. `SHOW EXTENSIONS` (superuser) lists loaded assemblies. `CREATE EXTENSION`, `ALTER EXTENSION` and `DROP EXTENSION` are DDL.

## Inline functions

An inline function is a scalar function whose C# body is dollar-quoted text in `CREATE FUNCTION`. No DLL is deployed, and the user needs the `CREATE` privilege on the database, not superuser. The server setting `inline_functions_enabled` must be `true`:

```csharp
await using CamusCommand create = connection.CreateCamusCommand("""
    CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$
        return s.Trim().ToLowerInvariant().Replace(' ', '-');
    $$
    """);
await create.ExecuteNonQueryAsync();

await using CamusCommand query = connection.CreateCamusCommand("SELECT slugify(title) FROM posts");
```

- `CREATE [OR REPLACE] FUNCTION`, `DROP FUNCTION` and `ALTER FUNCTION ... OWNER TO` are DDL. Send them with `ExecuteNonQueryAsync`.
- The driver sends the body as written. A body that holds `$$` takes a tagged quote: `$fn$ ... $fn$`.
- `SHOW FUNCTIONS` and `SHOW CREATE FUNCTION name` return rows.
- With `TraceQueryText=Sanitized`, the body is a literal: `db.query.text` shows `AS ?`.
- Types: `INT64`, `FLOAT64`, `FLOAT32`, `BOOL`, `STRING`, `UUID`, `BYTES`, `DATETIME`, `NUMERIC` and `ARRAY(E)`. Read the result with the usual getters ([Data types](data-types.md)).

## Errors

`CamusException.Code` is `CADB08xx`: `CADB0822` a procedure threw, `CADB0818` a trigger threw, `CADB0817` the node lacks that extension build (not retried). After a serializable conflict, a procedure or trigger can run again. Full rules: `docs/csharp-extensions.md` in the server repository.

Inline functions: `CADB0838` the body does not compile, `CADB0839` the body uses a type or member that is not allowed, `CADB0840` a call went over its time, memory, array or stack budget, `CADB0841` the feature is off, `CADB0842` the name is taken, `CADB0843` no function has that name, `CADB0844` a view or CHECK uses the function, `CADB0845` this node cannot compile it, and `CADB0809` the body threw. The driver retries none of them.
