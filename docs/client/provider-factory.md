# Provider factory and data source

The driver's `DbProviderFactory` and `DbDataSource`, for code that uses the abstract ADO.NET types (for example Testcontainers fixtures or health checks).

```csharp
using System.Data.Common;
using CamusDB.Client;

DbProviderFactories.RegisterFactory(CamusProviderFactory.InvariantName, CamusProviderFactory.Instance);

DbProviderFactory factory = DbProviderFactories.GetFactory("CamusDB.Client");
```

`CamusDataSource` creates connections and commands for one connection string:

```csharp
await using CamusDataSource dataSource = new("Endpoint=http://localhost:5095;Database=test");

await using CamusConnection connection = dataSource.CreateConnection();
await connection.OpenAsync();

await using DbCommand command = dataSource.CreateCommand("SELECT name FROM robots");
await using DbDataReader reader = await command.ExecuteReaderAsync();
```

- `factory.CreateConnection()` has no connection string. Set `ConnectionString` once, before use; a different value later throws.
- A command runs on the endpoint, database and credentials of its `Connection`.
- `ChangeDatabase` affects one data source connection only. The connections share one token and one transport.
- The data source constructor throws on a malformed string, or on credentials for a remote `http://` endpoint.
- `factory.CreateConnectionStringBuilder()` returns `null`.
