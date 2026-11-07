# EF Core: saving data

Writing entities with `SaveChanges`, and bulk writes with `ExecuteUpdate`/`ExecuteDelete`.

## Insert

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

`INSERT … RETURNING` reads back server-generated values, such as `HasDefaultValue` or `HasDefaultValueSql` properties the entity did not set:

```csharp
modelBuilder.Entity<Robot>(b =>
{
    b.Property(e => e.CreatedAt).HasDefaultValueSql("now()");
    b.Property(e => e.Serial).HasDefaultValueSql("nextval('robot_serial')").ValueGeneratedOnAdd();
});

ctx.Robots.Add(robot);
await ctx.SaveChangesAsync(); // robot.CreatedAt and robot.Serial now have the stored values
```

`UseSequence` and `UseHiLo` draw the value on the client when the entity is added. For a server-drawn value, use `HasDefaultValueSql("nextval('…')")`.

## Update

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

robot.Price = 99.0;
await ctx.SaveChangesAsync();
```

An update reads back unwritten `ValueGeneratedOnAddOrUpdate()` properties; with no `ON UPDATE` defaults, they keep their stored value.

## Delete

```csharp
await using var ctx = new AppDbContext();

Robot robot = await ctx.Robots.FindAsync(id)
    ?? throw new InvalidOperationException("Not found");

ctx.Robots.Remove(robot);
await ctx.SaveChangesAsync();
```

## Bulk Update and Delete

These run one `UPDATE`/`DELETE` without loading entities:

```csharp
int paid = await ctx.Orders
    .Where(o => o.Status == "open" && o.Total > 100)
    .ExecuteUpdateAsync(s => s
        .SetProperty(o => o.Status, "paid")
        .SetProperty(o => o.Total, o => o.Total * 2));

int removed = await ctx.Orders
    .Where(o => ctx.Customers.Any(c => c.Id == o.CustomerId && c.Name == "bob"))
    .ExecuteDeleteAsync();
```

- A filter on the target table alone becomes the `WHERE`. Any other filter (navigation, `Any(...)`, join, `OrderBy`/`Take`/`Skip`) becomes `WHERE <pk> IN (SELECT ...)`, which needs a single-column primary key.
- `SetProperty` values may use constants, parameters and the row's own columns. Reading another table throws `InvalidOperationException`.
- Tracked entities and `[Timestamp]` row versions are not updated; set the version in `SetProperty` if needed.

## Empty a table

`Database.TruncateTable` and `Database.TruncateTableAsync` run `TRUNCATE TABLE` on one table. The server drops all rows at once and reads or deletes no row, so the cost does not depend on the table size. Use it instead of `RemoveRange` on a whole table: `RemoveRange` loads every row and can go over the mutation limit of a transaction.

```csharp
await ctx.Database.TruncateTableAsync("orders");   // the driver quotes the name
ctx.ChangeTracker.Clear();                         // tracked entities do not change
```

- The table keeps its columns, indexes and grants.
- Call it outside `BeginTransaction`. In an explicit transaction it throws `CADB0538`, because a rollback cannot undo a truncate.
- It returns no row count.
- The server keeps the old rows for its retention period. `SHOW ORPHAN TABLES` lists them, and `CREATE TABLE t_before RELINK TO '<id>'` restores them as a different table.

