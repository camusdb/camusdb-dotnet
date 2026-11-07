# EF Core: views

Reading and migrating views and materialized views with EF Core.

## Views

A view is a stored query that CamusDB expands at every reference, so joins, aggregation and `ORDER BY` work through it. Map an entity with `ToView`:

```csharp
modelBuilder.Entity<OpenOrder>().HasNoKey().ToView("open_orders");

var spend = await ctx.OpenOrders
    .GroupBy(o => o.Customer)
    .Select(g => new { Customer = g.Key, Spent = g.Sum(o => o.Total) })
    .ToListAsync();
```

EF Core skips view-mapped types in migrations and `EnsureCreated`. Create the view with the provider's `CreateView`, `DropView` (`ifExists`, `cascade`) and `RenameView`:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateView(
        "open_orders",
        "SELECT id, customer, total FROM orders WHERE status = 'open'");

    // Optional: name the view's own columns, and/or replace an existing definition.
    migrationBuilder.CreateView(
        "order_summary",
        "SELECT id, customer, total FROM orders",
        columns: ["order_id", "who", "amount"]);

    migrationBuilder.RenameView("open_orders", "active_orders");
}

protected override void Down(MigrationBuilder migrationBuilder)
{
    migrationBuilder.DropView("order_summary");
    migrationBuilder.DropView("active_orders", ifExists: true);
}
```

`orReplace: true` emits `CREATE OR REPLACE VIEW`. The ADO.NET client runs the same SQL:

```csharp
await using CamusCommand cmd = connection.CreateCamusCommand(
    "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

await cmd.ExecuteNonQueryAsync();
```

Server rules:

- Every output column needs a name, or the server returns `CADB0400`. Alias expressions: `total + 1 AS total_plus_one`.
- `CREATE OR REPLACE` can only append columns. To change existing ones, drop and recreate.
- Dropping a view or table that a view reads fails with `CADB0530`. `cascade: true` drops dependent views. `DROP TABLE` has no `CASCADE`, so drop those views first.
- `SELECT *` is expanded at creation and cannot mix with other columns. The body cannot use `AS OF SYSTEM TIME` or hints.
- Views are read-only. Keep view types out of `SaveChanges`, for example with `HasNoKey()`.
- `WithCache(...)` has no effect on a view. The response reports `cacheStatus = bypass`, reason `derived-source` (see [Query Result Cache](../client/reading-data.md#query-result-cache)).

## Materialized views

A materialized view stores its query's rows and serves reads from them until you refresh it. You can index, analyze, comment and cache it like a table. Map it with `ToTable`, and keep it out of `SaveChanges`: the server refuses writes.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.Sql(
        "CREATE MATERIALIZED VIEW customer_totals AS " +
        "SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer");
}
```

A refresh returns the number of rows written:

```csharp
await using CamusCommand refresh = connection.CreateCamusCommand(
    "REFRESH MATERIALIZED VIEW customer_totals");

int rows = await refresh.ExecuteNonQueryAsync();
```

- A refresh swaps in the new rows atomically. Readers never block or see partial rows.
- `WITH NO DATA` creates or empties the view without running the query. Reading it then is an error, not an empty result.
- `REFRESH … CONCURRENTLY` and incremental refresh are not supported.
