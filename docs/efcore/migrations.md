# EF Core: migrations

Supported migration operations and their SQL, comments, column storage, and the rights a migrator needs.

## Migrations

A migration is a C# class EF Core generates from a model change. The EF tools need no extra flags:

```shell
dotnet ef migrations add InitialCreate
dotnet ef database update
```

| Operation | Generated SQL |
| --- | --- |
| Create table | `CREATE TABLE t (col TYPE [NOT NULL], ..., PRIMARY KEY (col1, ...))` |
| Drop / rename table | `DROP TABLE t` / `ALTER TABLE t RENAME TO new_name` |
| Add column | `ALTER TABLE t ADD COLUMN col TYPE [NOT NULL] [DEFAULT (value)]` |
| Drop / rename column | `ALTER TABLE t DROP COLUMN col` / `RENAME COLUMN old TO new` |
| Alter column nullability | `ALTER TABLE t ALTER COLUMN col SET NOT NULL` / `DROP NOT NULL` |
| Add check constraint / foreign key | `ALTER TABLE t ADD CONSTRAINT name CHECK (expr)` / `FOREIGN KEY (col) REFERENCES p (col) [ON DELETE RESTRICT]` |
| Drop check constraint or foreign key | `ALTER TABLE t DROP CONSTRAINT name` |
| Create index, unique index, or unique constraint (alternate key) | `CREATE [UNIQUE] INDEX name ON t (col1, ...)` |
| Drop index or unique constraint | `ALTER TABLE t DROP INDEX name` |
| Rename index | `ALTER TABLE t RENAME INDEX old TO new` |
| Table / column / index comment | `COMMENT ON TABLE t` / `COLUMN t.col` / `INDEX t.name IS '...'` |
| Column storage strategy | inline `STORAGE X` on create / add column; `ALTER TABLE t ALTER COLUMN col SET STORAGE X` |
| Rewrite stored rows (`migrationBuilder.RewriteStorage`) | `ALTER TABLE t REWRITE STORAGE [INLINE]` |
| Empty a table (`migrationBuilder.TruncateTable`) | `TRUNCATE TABLE t` |
| Raw SQL | passed through as-is |

`AlterColumn` can change only nullability, storage strategy or comment.

`migrationBuilder.TruncateTable("t")` runs outside the migration transaction, because CamusDB refuses `TRUNCATE` in a transaction (`CADB0538`). If a later step fails, the table stays empty. A truncate has no automatic reverse, so write `Down` for it yourself (see [Empty a table](saving-data.md#empty-a-table)). Run `ANALYZE` on the table after you fill it again, because the optimizer has no statistics for the new rows.

`HasCheckConstraint` emits `CONSTRAINT name CHECK (expr)` inside `CREATE TABLE`. `DropCheckConstraint` also drops a named `NOT NULL` constraint.

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

        migrationBuilder.AddCheckConstraint(
            name: "ck_products_stock",
            table: "products",
            sql: "Stock >= 0");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropCheckConstraint(name: "ck_products_stock", table: "products");
        migrationBuilder.DropIndex(name: "idx_products_name", table: "products");
        migrationBuilder.DropColumn(name: "Stock", table: "products");
    }
}
```

## Comments

`HasComment` (or `[Comment]`) works on tables, columns and indexes:

```csharp
modelBuilder.Entity<User>(b =>
{
    b.ToTable("users", t => t.HasComment("Application users"));
    b.Property(u => u.Id).HasComment("Internal user identifier");
    b.Property(u => u.Email).HasComment("Unique login email address");
    b.HasIndex(u => u.Email, "email_idx").IsUnique().HasComment("Lookup by login email");
});
```

```sql
CREATE TABLE IF NOT EXISTS `users` (
`id` OID NOT NULL COMMENT 'Internal user identifier',
`email` STRING NOT NULL COMMENT 'Unique login email address',
PRIMARY KEY (`id`)
) COMMENT 'Application users';
CREATE UNIQUE INDEX IF NOT EXISTS `email_idx` ON `users` (`email`);
COMMENT ON INDEX `users`.`email_idx` IS 'Lookup by login email';
```

Removing a comment emits `IS NULL` (not `IS ''`). Generation fails on a comment with a control character, a backslash before a quote, a trailing backslash, or over 65 535 characters.

The primary key index cannot have a comment. A database comment needs raw SQL: `migrationBuilder.Sql("COMMENT ON DATABASE app IS '...'")`.

## Large values

`HasStorage` sets the [storage strategy](../client/data-types.md#large-values) (how a large value is stored) of a `string`, `byte[]` or array property:

```csharp
modelBuilder.Entity<Doc>(b =>
{
    b.Property(d => d.Body);                                                  // EXTENDED, the default
    b.Property(d => d.Thumbnail).HasStorage(CamusColumnStorage.External);
    b.Property(d => d.Embedding).HasMaxLength(3072).HasStorage(CamusColumnStorage.Plain);
});
```

```sql
CREATE TABLE IF NOT EXISTS `docs` (
`Id` OID NOT NULL,
`Body` STRING NOT NULL,
`Thumbnail` BYTES STORAGE EXTERNAL,
`Embedding` BYTES(3072) STORAGE PLAIN,
PRIMARY KEY (`Id`)
);
```

- `IsVector` sets `STORAGE PLAIN`, the size and a dimension check in one call. See [Vector search](querying.md#vector-search).
- No `HasStorage` means no clause.
- Removing a strategy emits `SET STORAGE EXTENDED`.
- `HasStorage` on another column type fails model validation (server code `CADB0414`).

`SET STORAGE` affects future writes only. To convert stored rows, add `RewriteStorage` by hand. It runs outside the migration transaction:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.AlterColumn<byte[]>(
        name: "Embedding", table: "docs", type: "bytes", maxLength: 3072, nullable: true,
        oldClrType: typeof(byte[]), oldType: "bytes", oldMaxLength: 3072, oldNullable: true)
        .Annotation("Camus:ColumnStorage", CamusColumnStorage.Plain)
        .OldAnnotation("Camus:ColumnStorage", CamusColumnStorage.Extended);

    migrationBuilder.RewriteStorage("docs");            // ALTER TABLE `docs` REWRITE STORAGE
}
```

Outside a migration, use `context.Database.RewriteStorageAsync("docs")` (`inline: true` reverts to inline). Raise `Database.SetCommandTimeout(...)` for a large table.

## Migrating without superuser rights

`MigrateAsync()` and `EnsureCreatedAsync()` send `CREATE DATABASE IF NOT EXISTS` only if `SHOW DATABASE` returns `CADB0010` (does not exist). So an account without superuser rights (the right to create databases) can migrate an existing database:

```sql
GRANT SELECT, INSERT, CREATE TABLE, ALTER, INDEX ON shop.* TO migrator;
```

- Add `DROP`, `UPDATE` or `DELETE` if a migration drops objects or changes rows.
- If the database does not exist, `CREATE DATABASE` fails with `CADB0517`. Create it as a superuser first.
- Other errors from the check, such as a failed login, propagate.

The provider takes no migration lock. Run one migrator at a time.
