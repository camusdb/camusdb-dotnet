# EF Core: extension functions and limitations

Calling server-side C# extensions from EF Core, and what the provider does not support.

## Extension functions

Map a C# function that the server loads ([C# extensions](../client/csharp-extensions.md)), or an [inline function](../client/csharp-extensions.md#inline-functions), with `HasDbFunction`:

```csharp
public class AppDbContext : DbContext
{
    public static string Slugify(string value) => throw new NotSupportedException();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDbFunction(typeof(AppDbContext).GetMethod(nameof(Slugify))!).HasName("slugify");
    }
}

List<Post> posts = await ctx.Posts.Where(p => AppDbContext.Slugify(p.Title) == "hello-world").ToListAsync();
// WHERE slugify(`p`.`title`) = 'hello-world'
```

The server has no schemas, so the name is written bare. It must be a plain identifier (a letter or `_`, then letters, digits or `_`), or translation throws `InvalidOperationException`.

Run a procedure with `ctx.Database.ExecuteSqlAsync($"CALL transfer({from}, {to}, {amount})")`. Create a trigger or an inline function with `migrationBuilder.Sql("CREATE TRIGGER ...")` or `migrationBuilder.Sql("CREATE FUNCTION ... AS $$ ... $$")` (no model API). The migration sends the text as one command, so the `;` characters in a body are safe.

## Provider Limitations

- No computed columns.
- Keys must be `string`, `int`, `long`, `short` or `Guid`.
- `[ConcurrencyCheck]` works on `short`/`int`/`long`; `[Timestamp]`/`IsRowVersion()` on `byte[]` (provider-generated). A stale write throws `DbUpdateConcurrencyException`.
- Foreign keys run only `NO ACTION`/`RESTRICT` on the server; `Cascade`, `SetNull` and `SetDefault` affect tracked dependents only ([Relationships](relationships-and-concurrency.md#relationships-and-foreign-keys)). No foreign-key cycles.
- `ALTER COLUMN` changes only nullability, storage strategy and comment. A migration that changes a column type throws `NotSupportedException`. To change a type, drop and recreate the column.
- `CHECK` must be a deterministic single-row predicate: no subqueries, aggregates or volatile functions (`now()`, `gen_uuid_v4/v7()`). A violation throws `CamusException` `CADB0303` (in `DbUpdateException`). A `NULL` operand passes.
- `array(T)` maps `long[]`, `string[]`, `double[]`, `bool[]`. It is read and written whole only: not in a `Where`, a default or seed data.
- A `NUMERIC` with more than 28–29 significant digits throws `InvalidCastException` when read into a `decimal`.
- No lateral joins or window functions: some `SelectMany` and per-row `Take`/`Skip` shapes throw ([Joins](querying.md#joins)).
- `WithCache(...)` has no effect on a join, a view, or a query in an explicit transaction. A materialized view caches.
- Views are read-only and not scaffolded. A materialized view changes only by `REFRESH`.
- `ExecuteUpdate` cannot set a value from another table. With a join, correlated subquery or `Take`/`Skip`, `ExecuteUpdate`/`ExecuteDelete` need a single-column key ([Bulk Update and Delete](saving-data.md#bulk-update-and-delete)).
