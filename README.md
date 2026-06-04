# CamusDB Connector for .NET

.NET idiomatic client libraries for [CamusDB](https://github.com/camusdb/camusdb)

CamusDB.Client is the ADO.NET provider for CamusDB. It is the recommended package for regular CamusDB database access from .NET.

## Installation

Install the CamusDB.Client package from NuGet. Add it to your project in the normal way (for example by right-clicking on the project in Visual Studio and choosing "Manage NuGet Packages...").

### Using .NET CLI

```shell
dotnet add package CamusDB.Client --version 0.2.2-alpha
```

### Using NuGet Package Manager

Search for CamusDB.Client and install it from the NuGet package manager UI, or use the Package Manager Console:

```shell
Install-Package CamusDB.Client -Version 0.2.2-alpha
```

## Configuration

Create a `CamusConnectionStringBuilder` with a connection string:

```csharp
using CamusDB.Client;

CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
await using CamusConnection connection = new(builder);

await connection.OpenAsync();
```

Supported connection string keys:

| Key | Required | Description |
| --- | --- | --- |
| `Endpoint` | Yes | Base URL for the CamusDB HTTP endpoint. |
| `Database` | Yes | Database name sent with requests. |

`Endpoint` also supports a comma-separated pool. The client selects endpoints with round-robin routing:

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://localhost:8082,http://localhost:8084,http://localhost:8086;Database=test");
```

When a request fails because an endpoint is unreachable, that endpoint is marked unavailable and skipped by later requests made through the same `CamusConnectionStringBuilder`.

## Usage

### Ping

```csharp
await using CamusCommand ping = connection.CreatePingCommand();

int result = await ping.ExecuteNonQueryAsync();
```

### Execute DDL

```csharp
await using CamusCommand command = connection.CreateCamusCommand("""
    CREATE TABLE robots (
        id ID,
        name STRING,
        type STRING,
        year INTEGER64,
        price FLOAT64,
        enabled BOOL
    )
    """);

bool created = await command.ExecuteDDLAsync();
```

### Insert Rows

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

You can also execute parameterized SQL:

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

### Select Rows

```csharp
await using CamusCommand select = connection.CreateSelectCommand(
    "SELECT * FROM robots WHERE year = @year");

select.Parameters.Add("@year", ColumnType.Integer64, 1977);

CamusDataReader reader = await select.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    string id = reader.GetString(0);
    string name = reader.GetString(1);
    string type = reader.GetString(2);
    long year = reader.GetInt64(3);
}
```

### Transactions

```csharp
CamusTransaction transaction = await connection.BeginTransactionAsync();

await using CamusCommand insert = connection.CreateInsertCommand("robots");
insert.Transaction = transaction;

insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("name", ColumnType.String, "HAL 9000");
insert.Parameters.Add("type", ColumnType.String, "electronic");
insert.Parameters.Add("year", ColumnType.Integer64, 1968);
insert.Parameters.Add("price", ColumnType.Float64, 42.0);
insert.Parameters.Add("enabled", ColumnType.Bool, true);

await insert.ExecuteNonQueryAsync();
await transaction.CommitAsync();
```

Use `await transaction.RollbackAsync()` to roll back instead.

## Run Tests

To run the unit tests, it is necessary to have an instance of CamusDB running on the local machine on the standard port 7141. 
After this, the tests can be run with the following command:

```shell
dotnet test -l "console;verbosity=normal" --filter  "FullyQualifiedName~CamusDB.Client.Tests"
```

## Contribution

CamusDB.Client is an open-source project, and contributions are heartily welcomed! Whether you are looking to fix bugs, add new features, or improve documentation, your efforts and contributions will be appreciated. Check out the CONTRIBUTING.md file for guidelines on how to get started with contributing to CamusDB.Client.

## License

CamusDB.Client is released under the MIT License.
