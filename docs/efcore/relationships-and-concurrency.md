# EF Core: relationships and concurrency

How EF Core relationships become foreign keys, and how concurrency tokens detect stale updates.

## Relationships and Foreign Keys

Each relationship becomes a `FOREIGN KEY` constraint (a rule that a child row's key must match a parent row), in migrations and `EnsureCreated`. The server checks it at the end of each statement.

```csharp
modelBuilder.Entity<Customer>()
    .HasMany(c => c.Orders)
    .WithOne()
    .HasForeignKey(o => o.CustomerId);
```

```sql
CREATE TABLE IF NOT EXISTS `Orders` (
`Id` INT64 NOT NULL,
`CustomerId` INT64 NOT NULL,
PRIMARY KEY (`Id`),
KEY `IX_Orders_CustomerId` (`CustomerId`),
CONSTRAINT `FK_Orders_Customers_CustomerId` FOREIGN KEY (`CustomerId`) REFERENCES `Customers` (`Id`)
)
```

- **Delete behavior.** The server has no cascade (deleting children with their parent). `Cascade` (EF's default for required relationships), `SetNull` and `SetDefault` become `NO ACTION`; `Restrict` becomes `ON DELETE RESTRICT`. EF Core still deletes or nulls tracked dependents. Deleting a parent with untracked dependents fails with `CADB0305`, so load or delete them first.
- **Index.** The server needs an index on the foreign key columns. Migrations put EF's `IX_…` index in `CREATE TABLE`. Under `EnsureCreated`, the server creates `~fk_{constraint}`.
- **Alternate keys.** A foreign key can reference only a primary key or unique index. `HasAlternateKey` or `HasPrincipalKey` keys become a `UNIQUE KEY`.
- **Existing rows.** `AddForeignKey` checks every row. An orphan row fails it with `CADB0304`, and nothing changes.
- **Cycles.** A cycle across two or more tables fails with `CADB0416`. A self-reference is fine.

Violations throw `CamusException` (inside `DbUpdateException` under `SaveChanges`):

| Code | Meaning |
| --- | --- |
| `CADB0304` | An inserted or updated child has no parent, or `AddForeignKey` found a row without a parent. |
| `CADB0305` | A delete removes a parent key that a child row still holds. |
| `CADB0306` | An update changes a parent key that a child row still holds. |
| `CADB0416` | The constraint closes a cycle of two or more tables. |
| `CADB0530` | A schema change that the constraint blocks: `DROP TABLE` or `TRUNCATE` of a referenced table, `DROP COLUMN` of a referencing or a referenced column, `DROP INDEX` of an index that the constraint uses. |

To create no constraints, turn them off. `AddForeignKey` and `DropForeignKey` then emit nothing:

```csharp
optionsBuilder.UseCamusDB(connectionString, o => o.UseForeignKeyConstraints(false));
```

A database from an older provider version has no foreign keys, and new migrations do not add them. Add one with `migrationBuilder.AddForeignKey(...)` after fixing orphan rows. Remove any `DropForeignKey` step for a key the database lacks.

## Concurrency Tokens

A concurrency token is a column whose loaded value EF Core adds to the `UPDATE`'s `WHERE`. If the row changed meanwhile, `SaveChanges` throws `DbUpdateConcurrencyException`.

`[Timestamp]` / `IsRowVersion()` on a `byte[]` (a `BYTES` column): the provider sets a new increasing value on each insert and update.

```csharp
public class Doc
{
    public string Id      { get; set; } = "";
    public string Body    { get; set; } = "";
    [Timestamp]
    public byte[] Version { get; set; } = [];   // provider-managed row version
}

doc.Body = "edited";
await ctx.SaveChangesAsync();                    // throws DbUpdateConcurrencyException if stale
```

`[ConcurrencyCheck]` on a `short`, `int` or `long`: you increment it before `SaveChanges()`.

```csharp
public class Order
{
    public string Id      { get; set; } = "";
    public string Status  { get; set; } = "";
    [ConcurrencyCheck]
    public long Version   { get; set; }
}

order.Status = "shipped";
order.Version++;                                 // manual bump
await ctx.SaveChangesAsync();                    // throws DbUpdateConcurrencyException if stale
```

The token check fails at write time. A write-write conflict between two explicit transactions fails later, at `CommitTransactionAsync`.
