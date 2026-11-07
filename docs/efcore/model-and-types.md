# EF Core: model and type mapping

Defining entities, type mapping, and creating the database from the model.

## Defining a Model

Map a string key to the `"id"` store type (`OID`, CamusDB's 12-byte ObjectId). With `ValueGeneratedOnAdd()`, the client generates it:

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

The server enforces a `HasCheckConstraint` on every insert and update. A rejected row throws `DbUpdateException` with an inner `CamusException` code `CADB0303`:

```csharp
b.ToTable("robots", t => t.HasCheckConstraint("ck_robots_price", "price >= 0"));
```

## CamusDB Type Mapping

| CLR type | CamusDB store type | DDL type |
| --- | --- | --- |
| `string` (ID / PK) | `id` or `oid` | `OID` |
| `Guid` | `uuid` (alias `guid`) | `UUID` |
| `string` | `string` | `STRING` |
| `string` + `HasMaxLength(n)` | `string` | `STRING(n)` |
| `bool` | `bool` | `BOOL` |
| `short`, `int`, `long` | `int64` | `INT64` |
| `float` | `float32` | `FLOAT32` |
| `double` | `float64` | `FLOAT64` |
| `decimal` | `numeric` (alias `decimal`) | `NUMERIC` |
| `byte[]` | `bytes` (alias `blob`) | `BYTES` |
| `DateOnly` | `date` | `DATE` |
| `DateTime`, `DateTimeOffset` | `datetime` (alias `timestamp`) | `DATETIME` |
| `long[]`, `string[]`, `double[]`, `bool[]` | `array(int64/string/float64/bool)` | `ARRAY(T)` |
| `JsonDocument`, `JsonElement`, `JsonNode`, `JsonObject`, `JsonArray` | `jsonb` (alias `json`) | `JSONB` |
| `string` + `HasColumnType("jsonb")` | `jsonb` (alias `json`) | `JSONB` |

A property maps by its CLR type, or by an explicit `HasColumnType(...)`.

- **`Guid`** maps to `UUID` without `HasColumnType`; it is indexable and ordered by big-endian bytes. Do not give it `HasColumnType("id")`: an `OID` column refuses GUID values.
- **`decimal`** maps to `NUMERIC` with fixed precision 38 and scale 9; `HasPrecision` changes nothing. A `decimal` constant becomes `NUMERIC '9.99'`, which compares exactly and can use an index. A bare `9.99` is `FLOAT64`.
- **`DateTime`** is converted to UTC, stored as UTC ticks, and read back as `DateTimeKind.Utc`.
- **`byte[]`** travels as base64 in JSON; SQL literals use `0x` hex.

A migration snapshot from before 0.13.3 maps a plain `Guid` to `id`, and CamusDB cannot change it to `uuid`. Drop and re-add that column (it holds only NULLs).

### JSONB

A `string` with `HasColumnType("jsonb")` holds the document text. Values bind as `JSONB`; constants and defaults become `JSONB '…'`. They read back in PostgreSQL text form ([JSONB](../client/data-types.md#jsonb)), with canonical spacing and key order.

A change inside a `JsonNode` is saved. A `JsonDocument` or `JsonElement` is immutable: replace it. EF Core's `ToJson()` mapping is not supported.

`HasMethod("gin")` makes a GIN (inverted) index: migrations emit `CREATE INDEX … USING GIN (col)`, or `INVERTED INDEX` in `CREATE TABLE`. It has one column, never unique.

```csharp
modelBuilder.Entity<Event>(b =>
{
    b.Property(e => e.Payload);                      // JsonDocument → JSONB
    b.HasIndex(e => e.Payload).HasMethod("gin");     // CREATE INDEX … USING GIN (Payload)
});
```

### Arrays

Arrays map to native `ARRAY(T)` columns, not EF's JSON primitive collections. No `HasColumnType` is needed:

```csharp
b.Property(e => e.Tags);   // long[]  → ARRAY(INT64)
```

Arrays are not indexable and have no SQL literal, so they cannot appear in a `Where(...)` predicate, a default value or seed data.

## Database and Table Lifecycle

`EnsureCreatedAsync()` creates the database if missing, then every model table that does not exist yet. It is safe to repeat, also without superuser rights on an existing database ([Migrating without superuser rights](migrations.md#migrating-without-superuser-rights)). Principal tables come first; relationships become [foreign keys](relationships-and-concurrency.md#relationships-and-foreign-keys). Model indexes are not created.

```csharp
await using var ctx = new AppDbContext();
await ctx.Database.EnsureCreatedAsync();
```

`EnsureDeletedAsync()` drops the database:

```csharp
await ctx.Database.EnsureDeletedAsync();
```
