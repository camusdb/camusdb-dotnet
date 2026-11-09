# CamusDB Connector for .NET

.NET client libraries for [CamusDB](https://github.com/camusdb/camusdb), a distributed SQL database.

| Package | Use it to |
| --- | --- |
| `CamusDB.Client` | Run SQL from .NET through ADO.NET, the standard .NET database API (`DbConnection`, `DbCommand`, `DbDataReader`). |
| `CamusDB.EntityFrameworkCore` | Use Entity Framework Core (EF Core), the .NET object-relational mapper, with CamusDB. It is built on `CamusDB.Client`. |
| `CamusDB.Dapper` | Use [Dapper](https://github.com/DapperLib/Dapper), a small mapper from SQL rows to C# objects, with the CamusDB types. |
| `CamusDB.OpenTelemetry` | Collect the traces and metrics of the driver with the OpenTelemetry SDK. |

## Quick start

1. Start a CamusDB server. This example uses the REST port `5095` on your computer.
2. Add the package:

   ```shell
   dotnet add package CamusDB.Client
   ```

3. Connect and run a query:

   ```csharp
   using CamusDB.Client;

   CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=test");
   await using CamusConnection connection = new(builder);
   await connection.OpenAsync();

   await connection.CreateDatabaseAsync(ifNotExists: true);   // a database must exist before you use it

   await using CamusCommand command = connection.CreateCamusCommand("SELECT 1");
   await using CamusDataReader reader = await command.ExecuteReaderAsync();
   ```

## Documentation

The links are absolute because NuGet shows this file as the package page.

### CamusDB.Client (ADO.NET)

| Document | Contents |
| --- | --- |
| [Connection](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/connection.md) | Installation, connection string keys, endpoint pools |
| [Authentication](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/authentication.md) | Users, passwords, tokens, TLS, error codes |
| [Transport](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/transport.md) | REST or gRPC, HTTP/2 and HTTP/3 |
| [Learned routing](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/learned-routing.md) | Send statements directly to the node that leads the data |
| [Databases and DDL](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/databases-and-ddl.md) | Create and drop databases, ping, schema statements, truncate |
| [Data types](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/data-types.md) | Column types, NUMERIC, JSONB, large values |
| [Writing data](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/writing-data.md) | INSERT, RETURNING, ON CONFLICT, UPSERT |
| [Reading data](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/reading-data.md) | SELECT, streaming results, query result cache |
| [Transactions](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/transactions.md) | Transactions, isolation, locking, retries |
| [Prepared statements](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/prepared-statements.md) | Automatic and explicit statement preparation |
| [C# extensions](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/csharp-extensions.md) | Stored procedures, triggers, extension functions and inline functions written in C# |
| [Backups](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/backups.md) | Online backups and point-in-time recovery |
| [Node stats](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/node-stats.md) | Disk, CPU and memory of a node |
| [Provider factory](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/client/provider-factory.md) | `DbProviderFactory` and `DbDataSource` |
| [OpenTelemetry](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/opentelemetry.md) | Traces and metrics |

### CamusDB.EntityFrameworkCore (EF Core)

| Document | Contents |
| --- | --- |
| [Configuration](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/configuration.md) | Installation, `UseCamusDB`, retries, isolation defaults |
| [Model and types](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/model-and-types.md) | Entities, type mapping, database lifecycle |
| [Saving data](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/saving-data.md) | Insert, update, delete, bulk update and delete, truncate |
| [Queries](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/querying.md) | LINQ translation, strings, JSON, vector search, joins |
| [Migrations](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/migrations.md) | Schema migrations, comments, large values |
| [Views](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/views.md) | Views and materialized views |
| [Relationships and concurrency](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/relationships-and-concurrency.md) | Foreign keys, concurrency tokens |
| [Functions and limitations](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/efcore/functions-and-limitations.md) | Extension functions, what the provider does not support |

### CamusDB.Dapper

| Document | Contents |
| --- | --- |
| [Dapper](https://github.com/camusdb/camusdb-dotnet/blob/main/docs/dapper.md) | Typed values, arrays, `IN` lists, limitations |

## Run the tests

The tests need a CamusDB server on your computer. Start the server, then run:

```shell
dotnet test -l "console;verbosity=normal" --filter "FullyQualifiedName~CamusDB.Client.Tests"
```

## Contribution

Contributions are welcome: bug fixes, features and documentation. Open an issue or a pull request on GitHub.

## License

MIT License.
