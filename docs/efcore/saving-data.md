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

- A filter on the target table alone becomes the `WHERE`. Any other filter (navigation, `Any(...)`, join, `OrderBy`/`Take`/`Skip`) becomes `WHERE <pk> IN (SELECT ...)` when the table has a single-column primary key. Every server version runs these two forms.
- A `SetProperty` value can read another table: a column of a joined table, or a subquery on the row. The provider then writes `UPDATE t AS a SET ... FROM ... WHERE ...` (server 2026-11-14 or later):

```csharp
await (from o in ctx.Orders
       join c in ctx.Customers on o.CustomerId equals c.Id
       where c.Region == "eu"
       select new { o, c })
    .ExecuteUpdateAsync(s => s.SetProperty(x => x.o.Region, x => x.c.Region));
// UPDATE `orders` AS `o` SET `region` = `c`.`region` FROM `customers` AS `c` WHERE `o`.`customerid` = `c`.`id` AND `c`.`region` = 'eu'
```

- With `FROM`, a target row that two or more joined rows match fails with `CamusException` `CADB0400`, and nothing is written. SQL Server and PostgreSQL update such a row with one of the matches. Group the other table first, so that each target row matches one row.
- A table with a composite primary key uses the same alias form for a filter that needs a subquery: `UPDATE t AS a ... FROM ...` or `DELETE FROM t AS a WHERE EXISTS (...)` (server 2026-11-14 or later).
- An outer join in such a statement throws `InvalidOperationException`.
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

