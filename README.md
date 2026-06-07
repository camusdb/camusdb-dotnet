# CamusDB Connector for .NET

.NET idiomatic client libraries for [CamusDB](https://github.com/camusdb/camusdb)

This repository contains two packages:

| Package | Description |
| --- | --- |
| `CamusDB.Client` | ADO.NET provider — recommended for direct database access from .NET |
| `CamusDB.EntityFrameworkCore` | Entity Framework Core provider built on top of `CamusDB.Client` |

---

## CamusDB.Client (ADO.NET)

### Installation

```shell
dotnet add package CamusDB.Client
```

Or via the Package Manager Console:

```shell
Install-Package CamusDB.Client
```

### Configuration

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

### Usage

#### Ping

```csharp
await using CamusCommand ping = connection.CreatePingCommand();

int result = await ping.ExecuteNonQueryAsync();
```

#### Execute DDL

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

#### Insert Rows

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

#### Select Rows

```csharp
await using CamusCommand select = connection.CreateSelectCommand(
    "SELECT * FROM robots WHERE year = @year");

select.Parameters.Add("@year", ColumnType.Integer64, 1977);

CamusDataReader reader = await select.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    string id   = reader.GetString(0);
    string name = reader.GetString(1);
    string type = reader.GetString(2);
    long   year = reader.GetInt64(3);
}
```

#### Transactions

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

---

## CamusDB.EntityFrameworkCore (EF Core)

### Installation

```shell
dotnet add package CamusDB.EntityFrameworkCore
```

Or via the Package Manager Console:

```shell
Install-Package CamusDB.EntityFrameworkCore
```

### Configuration

Register the provider via `UseCamusDB` in your `DbContext` options:

```csharp
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB("Endpoint=http://localhost:8082;Database=mydb")
    .Options;
```

Or configure it inside `OnConfiguring`:

```csharp
public class AppDbContext : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseCamusDB("Endpoint=http://localhost:8082;Database=mydb");
}
```

#### Sharing an existing connection

Pass an open `CamusConnection` when you want to share a connection or attach to an externally managed transaction:

```csharp
CamusConnection connection = await GetOpenConnectionAsync();

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCamusDB(connection)
    .Options;

await using var ctx = new AppDbContext(options);
await ctx.Database.BeginTransactionAsync();
// ... SaveChangesAsync, then CommitTransactionAsync
```

The `DbContext` does **not** take ownership of the supplied connection and will not close or dispose it.

### Defining a Model

Use standard EF Core data annotations or the fluent API. Map ID columns to the `"id"` store type and call `ValueGeneratedOnAdd()` so the provider generates a client-side ObjectId automatically:

```csharp
public class Robot
{
    public string Id   { get; set; } = "";
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public int    Year { get; set; }
    public double Price   { get; set; }
    public bool   Enabled { get; set; }
}

public class AppDbContext : DbContext
{
    public DbSet<Robot> Robots => Set<Robot>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Robot>(b =>
        {
            b.ToTable("robots");
            b.HasKey(e => e.Id);
            b.Property(e => e.Id)
             .HasColumnType("id")
             .ValueGeneratedOnAdd();   // client-side ObjectId generation
            b.Property(e => e.Name).HasColumnType("string");
            b.Property(e => e.Type).HasColumnType("string");
            b.Property(e => e.Year).HasColumnType("int64");
            b.Property(e => e.Price).HasColumnType("float64");
            b.Property(e => e.Enabled).HasColumnType("bool");
        });
    }
}
```

### CamusDB Type Mapping

| CLR type | CamusDB store type | DDL type |
| --- | --- | --- |
| `string` (ID / PK) | `id` or `oid` | `OID` |
| `Guid` (ID / PK) | `id` or `oid` | `OID` |
| `string` | `string` | `STRING` |
| `bool` | `bool` | `BOOL` |
| `short`, `int`, `long` | `int64` | `INT64` |
| `float`, `double` | `float64` | `FLOAT64` |

Use `HasColumnType("id")` (or the alias `"oid"`) for primary key columns backed by CamusDB ObjectIds. The provider sends the value as an OID on the wire regardless of whether the CLR property is `string` or `Guid`.

### Creating Tables

`EnsureCreated()` creates all tables defined in the model. It is safe to call on a database that already has the tables:

```csharp
await using var ctx = new AppDbContext();
await ctx.Database.EnsureCreatedAsync();
```

### Insert

```csharp
await using var ctx = new AppDbContext();

ctx.Robots.Add(new Robot
{
    Name    = "T-800",
    Type    = "cyborg",
    Year    = 1984,
    Price   = 10.0,
    Enabled = true
});

await ctx.SaveChangesAsync(); // Id is generated automatically
```

### Query

```csharp
await using var ctx = new AppDbContext();

// Key lookup
Robot? robot = await ctx.Robots.FindAsync(id);

// LINQ predicate
List<Robot> active = await ctx.Robots
    .Where(r => r.Enabled && r.Year > 1980)
    .ToListAsync();
```

### Update

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

robot.Price = 99.0;
await ctx.SaveChangesAsync();
```

### Delete

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

ctx.Robots.Remove(robot);
await ctx.SaveChangesAsync();
```

### Migrations

The provider supports EF Core migrations for the following DDL operations:

| Operation | Generated SQL |
| --- | --- |
| Create table | `CREATE TABLE t (col TYPE [PRIMARY KEY NOT NULL \| NOT NULL], ...)` |
| Drop table | `DROP TABLE t` |
| Add column | `ALTER TABLE t ADD COLUMN col TYPE [NOT NULL] [DEFAULT (value)]` |
| Drop column | `ALTER TABLE t DROP COLUMN col` |
| Create index | `CREATE INDEX name ON t (col1, col2)` |
| Create unique index | `CREATE UNIQUE INDEX name ON t (col1, col2)` |
| Drop index | `ALTER TABLE t DROP INDEX name` |
| Raw SQL | passed through as-is |

The provider ships design-time services so the EF tooling can discover the provider automatically. No extra flags are needed:

```shell
dotnet ef migrations add InitialCreate
dotnet ef database update
```

Example migration using the supported operations:

```csharp
public partial class AddStockColumn : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<int>(
            name: "Stock",
            table: "products",
            type: "int64",
            nullable: false,
            defaultValue: 0);

        migrationBuilder.CreateIndex(
            name: "idx_products_name",
            table: "products",
            column: "Name",
            unique: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(name: "idx_products_name", table: "products");
        migrationBuilder.DropColumn(name: "Stock", table: "products");
    }
}
```

### Concurrency Tokens

`[ConcurrencyCheck]` is supported on numeric columns (`short`, `int`, `long`). The application is responsible for incrementing the version column before calling `SaveChanges()` — CamusDB has no server-side auto-increment version type:

```csharp
public class Order
{
    public string Id      { get; set; } = "";
    public string Status  { get; set; } = "";
    [ConcurrencyCheck]
    public long Version   { get; set; }
}

// On update: increment Version manually so EF adds AND Version = @original_version to the WHERE
order.Status = "shipped";
order.Version++;
await ctx.SaveChangesAsync();
```

`[Timestamp]` (byte array row version) is not supported.

> **Note on MVCC concurrency:** CamusDB uses multi-version concurrency control (MVCC). Write-write conflicts between transactions are detected at **commit time**, not during the write phase. This means `SaveChangesAsync` will succeed even when two open transactions have both updated the same row — the conflict surfaces when the second transaction calls `CommitTransactionAsync`. Application-level optimistic concurrency via `[ConcurrencyCheck]` (above) is the recommended pattern for detecting stale updates.

### Provider Limitations

- No computed columns.
- No foreign key constraints.
- No `ALTER COLUMN` — changing a column type requires dropping and recreating the column.
- No `RENAME COLUMN`, `RENAME TABLE`, or `RENAME INDEX`.
- Key CLR types must be one of: `string`, `int`, `long`, `short`, or `Guid`.
- `[ConcurrencyCheck]` is only supported on `short`, `int`, and `long` columns; `[Timestamp]` is not supported.
- MVCC conflict detection occurs at commit time, not during `SaveChangesAsync`. Use application-level version columns with `[ConcurrencyCheck]` for optimistic concurrency.

---

## Run Tests

To run the unit tests, a CamusDB instance must be running locally. After starting it, run:

```shell
dotnet test -l "console;verbosity=normal" --filter "FullyQualifiedName~CamusDB.Client.Tests"
```

## Contribution

CamusDB.Client is an open-source project, and contributions are heartily welcomed! Whether you are looking to fix bugs, add new features, or improve documentation, your efforts and contributions will be appreciated. Check out the CONTRIBUTING.md file for guidelines on how to get started with contributing to CamusDB.Client.

## License

CamusDB.Client is released under the MIT License.
