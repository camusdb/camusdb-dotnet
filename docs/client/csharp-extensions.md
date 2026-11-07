# C# extensions: procedures, triggers and functions

The server can run C# from an extension assembly that the operator deploys. The driver needs no setup.

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

Call a function like a built-in: `SELECT slugify(title) FROM posts`. `SHOW EXTENSIONS` (superuser) lists loaded assemblies.

## Errors

`CamusException.Code` is `CADB08xx`: `CADB0822` a procedure threw, `CADB0818` a trigger threw, `CADB0817` the node lacks that extension build (not retried). After a serializable conflict, a procedure or trigger can run again. Full rules: `docs/csharp-extensions.md` in the server repository.
