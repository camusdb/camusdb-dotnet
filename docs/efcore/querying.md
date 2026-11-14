# EF Core: queries

LINQ that runs on the server: string, regex, JSON and vector functions, joins, set operations and caching.

## Query

```csharp
await using var ctx = new AppDbContext();

// Key lookup
Robot? robot = await ctx.Robots.FindAsync(id);

// LINQ predicate
List<Robot> active = await ctx.Robots
    .Where(r => r.Enabled && r.Year > 1980)
    .ToListAsync();
```

Translated to SQL: `Where`, `OrderBy`/`ThenBy`, `Skip`/`Take`, `Distinct`, `Count`, `Sum`, `Average`, `Min`, `Max`, `Any`, `GroupBy` with an aggregate, [joins](#joins) and correlated subqueries (`Where(x => x.Items.Any(...))`).

These `string` members run on the server:

| LINQ | CamusDB function |
| --- | --- |
| `s.StartsWith(x)` / `s.EndsWith(x)` / `s.Contains(x)` | `starts_with` / `ends_with` / `contains` |
| `s.StartsWith(x, StringComparison.OrdinalIgnoreCase)` (and other `…IgnoreCase`) | wrapped in `lower(...)` |
| `s.ToUpper()` / `s.ToLower()` | `upper` / `lower` |
| `s.Trim()` / `s.TrimStart()` / `s.TrimEnd()` | `trim` / `ltrim` / `rtrim` |
| `s.Replace(a, b)` | `replace` |
| `s.Length` | `length` |

`x` may be a column or parameter; no `LIKE` escaping. A non-constant `StringComparison` compares case-sensitively.

### Dates and intervals

Date arithmetic translates to SQL `interval` arithmetic (server 2026-11-13 or later). All values are UTC, so a day is always 24 hours.

| LINQ | SQL |
| --- | --- |
| `DateTime.UtcNow`, `DateTimeOffset.UtcNow` | `now()`, the start of the transaction |
| `d.AddYears(n)` … `d.AddMicroseconds(n)` | `d + INTERVAL 'P-7D'` for a constant, else `d + (n * INTERVAL 'P1D')` |
| `d.Add(span)`, `d.Subtract(span)`, `d + span`, `d - span` | `d + CAST(@span AS interval)` |
| `a - b`, `a.Subtract(b)` (two `DateTime` or `DateTimeOffset`) | `a - b`, read back as a `TimeSpan` |
| `span1 + span2`, `span * 2.0`, `span / 2.0`, `span.Negate()` | interval arithmetic |
| `TimeSpan.FromDays(x)` … `FromMicroseconds(x)` with a column | `x * INTERVAL 'P1D'` |
| `day.AddDays(n)` (`DateOnly`) | `day + n` |
| `day.AddMonths(n)`, `day.AddYears(n)` (`DateOnly`) | `CAST(day + INTERVAL 'P1M' AS date)` |

```csharp
TimeSpan limit = TimeSpan.FromHours(1);

var slow = await ctx.Jobs
    .Where(j => j.Started > DateTime.UtcNow.AddDays(-7) && j.Finished - j.Started > limit)
    .Select(j => new { j.Name, Length = j.Finished - j.Started })
    .ToListAsync();
// WHERE `j`.`started` > (now() + INTERVAL 'P-7D')
//   AND (`j`.`finished` - `j`.`started`) > CAST(@limit AS interval)
```

- A month added to the 31st moves back to the last day of the month, as in .NET: January 31 plus one month is February 29 in 2024.
- A fraction of a day goes to the server, which moves it into the time: `AddDays(1.5)` adds 1 day 12 hours.
- Not translated: `DateTime.Now` and `DateTime.Today` (local time), `TimeSpan.TotalDays` and the other `Total…` and component members (the server reads whole seconds only), and `span1 / span2`.

### String concatenation

`a + b` and `string.Concat(...)` translate to `||` (server 2026-10-12 or later; older ones return `CADB0400`). Numbers and dates are cast to `string`. EF Core wraps nullable operands in `COALESCE(x, '')`, so `"a" + null` is `"a"`, as in C#.

```csharp
// First is required, Last is nullable.
var names = await ctx.People.Select(p => p.First + " " + p.Last).ToListAsync();
// SELECT (`p`.`first` || ' ') || COALESCE(`p`.`last`, '') FROM `people` AS `p`
```

An interpolated string (`$"{a} {b}"`, which C# turns into `string.Format`) translates the same way:

```csharp
var ada = await ctx.People.Where(p => $"{p.First} {p.Last}" == "Ada Lovelace").ToListAsync();
// WHERE ((`p`.`first` || ' ') || COALESCE(`p`.`last`, '')) = 'Ada Lovelace'
```

`string.Format` translates only with a constant format of `{0}` placeholders, literal text and `{{`/`}}`, and string or integer arguments (`null` gives `""`). A specifier or alignment (`{0:N2}`, `{0,5}`), or a `double`, `decimal`, date, `Guid` or `bool` argument, is culture-dependent and does not translate: the final `Select` runs it on the client, a `Where`, `OrderBy` or join throws `InvalidOperationException`. Use `+` with an explicit conversion.

### String ordering (`string.Compare` / `CompareTo`)

Keyset pagination (fetching the next page by comparing with the last row seen) translates to a plain comparison:

```csharp
// emits: WHERE `seq` < @seq OR (`seq` = @seq AND `code` < @code)
query.Where(e => e.Seq < cursorSeq ||
                 (e.Seq == cursorSeq && string.Compare(e.Code, cursorCode, StringComparison.Ordinal) < 0));
```

`string.Compare(a, b)`, `(a, b, ignoreCase)`, `(a, b, StringComparison)` and `a.CompareTo(b)` translate only when compared with `0` (`< <= > >= == !=`, either side); the server has no `CASE`. `…IgnoreCase` forms wrap both sides in `lower(...)`. Ordering is always ordinal.

### Regular expressions

`Regex` static calls and `EF.Functions.Regexp*` map to the server's regex functions (PostgreSQL semantics):

| LINQ | CamusDB |
| --- | --- |
| `Regex.IsMatch(s, pattern)` | `regexp_like(s, pattern)` (the `~` operator) |
| `Regex.IsMatch(s, pattern, RegexOptions.IgnoreCase)` | `regexp_like(s, pattern, 'i')` (the `~*` operator) |
| `Regex.Replace(s, pattern, repl)` | `regexp_replace(s, pattern, repl, 'g')` (every match, like .NET) |
| `EF.Functions.RegexpLike(s, pattern[, flags])` | `regexp_like` |
| `EF.Functions.RegexpReplace(s, pattern, repl[, flags])` | `regexp_replace` (first match; `"g"` for all) |
| `EF.Functions.RegexpCount(s, pattern[, flags])` | `regexp_count` |
| `EF.Functions.RegexpSubstr(s, pattern[, flags])` | `regexp_substr` |
| `EF.Functions.RegexpInstr(s, pattern[, flags])` | `regexp_instr` |

```csharp
using System.Text.RegularExpressions;

var valid = await ctx.People
    .Where(p => Regex.IsMatch(p.Name, "^[A-Z]"))                                   // name ~ '^[A-Z]'
    .Where(p => EF.Functions.RegexpLike(p.Email, "^[^@]+@[^@]+\\.[a-z]+$", "i"))   // email ~* '…'
    .ToListAsync();
```

Flags: `i` case-insensitive, `c` case-sensitive, `m`/`n` multiline, `s` dot matches newline, `x` extended, `g` replace all. `RegexOptions` must be a constant of `IgnoreCase`, `Multiline`, `Singleline` or `IgnorePatternWhitespace` (`Compiled` and `CultureInvariant` are ignored); otherwise the call does not translate. The same patterns work in `HasCheckConstraint` (`"email ~* '^[^@]+@[^@]+$'"`). The server has no `regexp_matches` or `regexp_split_to_table`.

### JSON documents

`EF.Functions.Json*` map to the [JSONB](../client/data-types.md#jsonb) operators. A document is a `jsonb` column, JSON text, `JsonDocument`, `JsonElement` or `JsonNode`; a value next to a `jsonb` column binds as `jsonb`. A GIN index (on a document's keys and values, `HasMethod("gin")`) serves them at the top of a `Where` when the other argument has no column.

| LINQ | CamusDB |
| --- | --- |
| `EF.Functions.JsonContains(json, contained)` | `jsonb_contains` (the `@>` operator) |
| `EF.Functions.JsonContained(contained, json)` | `jsonb_contained` (the `<@` operator) |
| `EF.Functions.JsonExists(json, key)` | `jsonb_exists` (the `?` operator) |
| `EF.Functions.JsonExistAny(json, keys)` | `jsonb_exists_any` (the `?\|` operator) |
| `EF.Functions.JsonExistAll(json, keys)` | `jsonb_exists_all` (the `?&` operator) |
| `EF.Functions.JsonTypeof(json)` | `jsonb_typeof` |

```csharp
var logins = await ctx.Events
    .Where(e => EF.Functions.JsonContains(e.Payload, """{"kind": "login"}"""))   // jsonb_contains(payload, JSONB '…')
    .Where(e => EF.Functions.JsonExists(e.Payload, "user"))
    .ToListAsync();
```

To project, group, sort or compare a value inside a document, use the value helpers (a path is the keys from the root to a nested value).

| LINQ | CamusDB | Result |
| --- | --- | --- |
| `EF.Functions.JsonValueText(json, key \| index)` | `jsonb_object_field_text` (the `->>` operator) | `string?` |
| `EF.Functions.JsonValue(json, key \| index)` | `jsonb_object_field` (the `->` operator) | `JsonElement?` |
| `EF.Functions.JsonPathText(json, "a", "b", …)` | `jsonb_extract_path_text(json, 'a', 'b', …)` | `string?` |
| `EF.Functions.JsonPath(json, "a", "b", …)` | `jsonb_extract_path(json, 'a', 'b', …)` | `JsonElement?` |

```csharp
var byReason = await ctx.Events
    .Where(e => EF.Functions.JsonValueText(e.Payload, "kind") == "login")            // served by the GIN index
    .GroupBy(e => EF.Functions.JsonPathText(e.Payload, "recommendation", "reason"))
    .Select(g => new { Reason = g.Key, Count = g.Count() })
    .ToListAsync();

var byRank = await ctx.Events
    .OrderBy(e => (long?)Convert.ToInt64(EF.Functions.JsonValueText(e.Payload, "rank")))   // CAST(… AS int64)
    .ToListAsync();
```

- `NULL`: a missing key, an index past the end, a key on a non-object, an index on a non-array. A negative index counts from the end.
- `…Text` helpers return a string without quotes, and other values as canonical JSON (`1.50`, `true`, `{"z": [1]}`). JSON `null` gives `NULL`.
- `JsonValue`/`JsonPath` return `jsonb` (JSON `null` is a `JsonElement` of kind `Null`), usable in another helper: `EF.Functions.JsonContains(EF.Functions.JsonValue(e.Payload, "user")!, """{"id": 7}""")`.
- A path element is a key or an array index as decimal text. A path written in place sends one argument per key, so commas and braces in a key are safe. A `string[]` variable binds as one array parameter of `jsonb_path_get[_text]` (`#>`, `#>>`). An empty path gives the whole document.

**Only one shape uses a GIN index:** `JsonValueText(column, key) == text` at the top level of a `Where`, with constant or variable `key` and `text`. Other shapes read every row. `JsonElement` has no `==`; compare a member with `JsonContains`, which the index serves.

`Convert.ToInt32`, `ToInt64`, `ToDouble`, `ToDecimal` and `ToBoolean` of a string translate to `CAST(text AS int64 | float64 | numeric | bool)`. Text of the wrong type fails with `CADB0400` (`'1.50'` is not an `int64`). Cast to a nullable type if the member can be missing.

Member access on a CLR document (`RootElement.GetProperty("kind")`, a `JsonNode` indexer) does not translate.

### Vector search

Vector search ranks rows by distance between a stored embedding (a float vector representing an input) and a query vector. CamusDB stores it in `BYTES` as packed little-endian float32 (see `CamusVector` in `CamusDB.Client`) and computes exact distances. It shares the row's transaction, and a tenant filter is a plain `WHERE`.

Declare the property (`float[]` or `byte[]`) with `IsVector(dimensions)`:

```csharp
public class DocumentChunk
{
    public string Id { get; set; } = "";
    public long TenantId { get; set; }
    public string Content { get; set; } = "";
    public float[] Embedding { get; set; } = [];
}

modelBuilder.Entity<DocumentChunk>(b =>
{
    b.ToTable("chunks");
    b.HasKey(c => c.Id);
    b.HasIndex(c => c.TenantId);
    b.Property(c => c.Embedding).IsRequired().IsVector(1536, loadWithEntity: false);
});
```

`IsVector` sets:

- `BYTES(dimensions × 4)`, a maximum width.
- `STORAGE PLAIN`, so a scan does not fetch embeddings over 2 KB out of row. `HasStorage` overrides it.
- `CHECK (vector_dims(col) = dimensions)`, named `CK_{table}_{column}_dims`. `NULL` passes; use `IsRequired()`.
- For `float[]`, a converter and element-wise comparer that save in-place edits (`chunk.Embedding[0] = 1f`).

```sql
CREATE TABLE IF NOT EXISTS `chunks` (
`Id` STRING NOT NULL,
`TenantId` INT64 NOT NULL,
`Content` STRING NOT NULL,
`Embedding` BYTES(6144) NOT NULL STORAGE PLAIN,
PRIMARY KEY (`Id`),
CONSTRAINT `CK_chunks_Embedding_dims` CHECK (vector_dims(`Embedding`) = 1536)
);
```

Rank by distance and always add `Take(k)`: the server then keeps a top-k instead of sorting every row. The column converter packs a `float[]` query vector; for `byte[]`, use `CamusVector.ToBytes`.

```csharp
float[] question = await embeddingGenerator.GenerateVectorAsync(text);

var hits = await ctx.Chunks
    .Where(c => c.TenantId == tenantId)
    .OrderBy(c => EF.Functions.CosineDistance(c.Embedding, question))
    .Select(c => new { c.Id, c.Content, Distance = EF.Functions.CosineDistance(c.Embedding, question) })
    .Take(8)
    .ToListAsync();
```

```sql
SELECT `c`.`Id`, `c`.`Content`, cosine_distance(`c`.`Embedding`, @question) AS `Distance`
FROM `chunks` AS `c`
WHERE `c`.`TenantId` = @tenantId
ORDER BY cosine_distance(`c`.`Embedding`, @question)
LIMIT @p
```

| Function | SQL | Nearest is | Order with |
| --- | --- | --- | --- |
| `EF.Functions.L2Distance(a, b)` | `l2_distance` | smallest | `OrderBy` |
| `EF.Functions.CosineDistance(a, b)` | `cosine_distance` (0 to 2) | smallest | `OrderBy` |
| `EF.Functions.InnerProduct(a, b)` | `inner_product` | **largest** | **`OrderByDescending`** |
| `EF.Functions.VectorDims(v)` | `vector_dims` | — | — |

`InnerProduct` with `OrderBy` silently gives the *least* similar rows.

**Vectors not loaded with the entity.** With `IsVector(n, loadWithEntity: false)` (default `true`), an entity query selects `NULL` for the vector and the property is `null`. This saves about 6 KB per 1536-dimension row.

```csharp
var page = await ctx.Chunks.OrderBy(c => c.Id).Take(200).ToListAsync();
// SELECT `c`.`Id`, `c`.`Content`, NULL AS `Embedding`, `c`.`TenantId` FROM `chunks` AS `c` …
```

- A `Where`, `OrderBy` or projection (`Select(c => new { c.Id, c.Embedding })`) naming the vector reads it.
- `SaveChanges` never writes a `null` vector over the stored one, also for `ctx.Update(detachedEntity)`.
- The property must be required, and not a key or concurrency token; the model validator enforces this.
- A collection `Include`, split query, nested collection or `Distinct()` reads it too.

**Limits.** Cost grows with rows × dimensions for the rows the `WHERE` admits, so filter selectively. There is no approximate (ANN) index and no full-text ranking for hybrid search. Costs: server repo `docs/vector-search.md`.

### Joins

An outer join (`LEFT`/`RIGHT`) also returns unmatched rows. Outer and cross joins need server support.

| LINQ | SQL |
| --- | --- |
| `join … on … equals …`, `Join(...)`, a required navigation | `INNER JOIN` |
| `Include(...)` of a collection or an optional reference, an optional navigation (`o.Customer!.Name`), `join … into g from x in g.DefaultIfEmpty()`, `LeftJoin(...)` | `LEFT JOIN` |
| `RightJoin(...)` | `RIGHT JOIN` |
| `from a in A from b in B` (no correlation) | `CROSS JOIN` |

```csharp
// Customers with their orders; a customer with no order gets an empty collection.
var customers = await ctx.Customers.Include(c => c.Orders).ToListAsync();

// Every customer, with null for the total of a customer that has no order.
var rows = await ctx.Customers
    .LeftJoin(ctx.Orders, c => (long?)c.Id, o => o.CustomerId,
              (c, o) => new { c.Name, Total = (long?)o!.Total })
    .ToListAsync();
```

An unmatched row gets `NULL` for the other side: a `null` entity, a `null` projection member, or no `Include` element. For `RightJoin`, a `Where` on the left operand runs before the join, so unmatched right rows stay; the left operand can be a join.

With no lateral joins or window functions, these throw `InvalidOperationException`:

- A `SelectMany` or join correlated outside an equality key, e.g. `from o in ctx.Orders.Where(o => o.Total > c.Limit + 1)`. Use an equality key or separate queries.
- `Take`/`Skip`/`First` per row: `Include(c => c.Orders.OrderBy(...).Take(n))`, `SelectMany` over `c.Orders.Take(n)`, a per-group `First()`. Load it separately and `Take` on the client.

`FULL JOIN` is not supported.

### Set operations

A set operation combines two queries' rows (needs server `UNION`/`INTERSECT`/`EXCEPT` support).

| LINQ | SQL |
| --- | --- |
| `Union(...)` | `UNION` |
| `Concat(...)` | `UNION ALL` |
| `Intersect(...)` | `INTERSECT` |
| `Except(...)` | `EXCEPT` |

```csharp
// Customers in NYC, or with an order over 100, each once.
var keys = await ctx.Customers.Where(c => c.City == "NYC").Select(c => c.Id)
    .Union(ctx.Orders.Where(o => o.Total > 100).Select(o => o.CustomerId))
    .ToListAsync();
```

`OrderBy`/`Skip`/`Take` after the operator apply to all rows; each operand can have its own. Mixed operators keep their order. A set operation can be a `Contains` subquery. Rows compare position by position, and `NULL` equals `NULL`.

## Query Result Cache

`WithCache(name, ttl, strict)` adds a `{cache=…}` hint; the server answers a later identical query from its [query result cache](../client/reading-data.md#query-result-cache). `ttl` defaults to the server's; `strict: true` validates each hit against storage. Only single-table autocommit reads cache: a join, a view, or an explicit transaction reads live storage.

```csharp
using CamusDB.EntityFrameworkCore;

// Cache with the server's default TTL
List<Order> recent = await ctx.Orders
    .Where(o => o.Status == 1)
    .OrderByDescending(o => o.Total)
    .Take(20)
    .WithCache("recent_orders")
    .ToListAsync();

// Per-entry TTL override and strict per-hit validation
List<Order> hot = await ctx.Orders
    .Where(o => o.Status == 1)
    .WithCache("hot_orders", ttl: TimeSpan.FromSeconds(30), strict: true)
    .ToListAsync();
```

Evict with `EvictCacheAsync` / `EvictAllCacheAsync` on the `CamusConnection`.
