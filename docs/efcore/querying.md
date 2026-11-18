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

Translated to SQL: `Where`, `OrderBy`/`ThenBy`, `Skip`/`Take`, `Distinct`, `Count`, `Sum`, `Average`, `Min`, `Max`, `Any`, `GroupBy` with an aggregate, [joins](#joins), [inline and parameter collections](#inline-and-parameter-collections) and correlated subqueries (`Where(x => x.Items.Any(...))`).

These `string` members run on the server:

| LINQ | CamusDB function |
| --- | --- |
| `s.StartsWith(x)` / `s.EndsWith(x)` / `s.Contains(x)` | `starts_with` / `ends_with` / `contains` |
| `s.StartsWith(x, StringComparison.OrdinalIgnoreCase)` (and other `…IgnoreCase`) | wrapped in `lower(...)` |
| `s.ToUpper()` / `s.ToLower()` | `upper` / `lower` |
| `s.Trim()` / `s.TrimStart()` / `s.TrimEnd()` | `trim` / `ltrim` / `rtrim` |
| `s.Replace(a, b)` | `replace` |
| `s.Length` | `length` |
| `s.Substring(i)` / `s.Substring(i, n)` | `substring(s, i + 1)` / `substring(s, i + 1, n)` |
| `s.IndexOf(x)` (`string` or `char`) | `strpos(s, x) - 1` |
| `s[i]` | `substring(s, i + 1, 1)` |
| `s.Trim(c)`, `s.TrimStart('a', 'b')`, `s.TrimEnd(…)` | `trim(s, 'ab')` / `ltrim` / `rtrim` |
| `s.Replace('a', 'b')`, `s.Contains('a')`, `s.StartsWith('a')`, `s.EndsWith('a')` | the same functions, with the `char` as a one-character string |
| `string.IsNullOrWhiteSpace(s)` | `s IS NULL OR regexp_like(s, '^\s*$')` |
| `x.ToString()`, `Convert.ToString(x)` | `CAST(x AS string)`; for a `bool`, `CASE WHEN x THEN 'True' ELSE 'False' END` |
| `int.Parse(s)`, `long.Parse`, `double.Parse`, `decimal.Parse`, `bool.Parse`, `Guid.Parse` | `CAST(s AS int64 / float64 / numeric / bool / uuid)` |
| `Guid.NewGuid()` | `gen_uuid_v4()` |

`x` may be a column or parameter; no `LIKE` escaping. A non-constant `StringComparison` compares case-sensitively.

- .NET throws when `Substring` or `s[i]` goes past the end of the string. The server returns the part that exists, or an empty string.
- A `char` is stored as a string of one character. A `char` property maps to a `string` column.
- `x.ToString()` translates for `int`, `long`, `short`, `double`, `Guid`, `char` and `bool`. It does not translate for `decimal`, because the server drops the trailing zeros that .NET keeps (`1.50` is `'1.5'`). It does not translate for `float`, or for the date types, whose .NET text depends on the culture.
- `Parse` fails on the server with `CADB0400` for a text that is not a value of the type. .NET throws `FormatException`.
- `TrimStart` and the other trims with a `char[]` translate only when the array is a constant, such as `s.TrimStart('a', 'b')`.

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
The parts of a date run on the server too:

| LINQ | SQL |
| --- | --- |
| `d.Year`, `Month`, `Day`, `Hour`, `Minute`, `Second`, `Millisecond`, `DayOfWeek`, `DayOfYear` | `date_part('year', d)` … `date_part('dow', d)`, `date_part('doy', d)` |
| `day.Year`, `Month`, `Day`, `DayOfWeek`, `DayOfYear` (`DateOnly`) | `date_part(…, day)` |
| `d.Date` | `date_trunc('day', d)` |
| `d.TimeOfDay` | `d - date_trunc('day', d)`, read back as a `TimeSpan` |
| `DateOnly.FromDateTime(d)` | `CAST(d AS date)` |

The parts are the UTC parts. `Second` is the whole seconds, `Millisecond` is the fraction of the second (0–999), and `DayOfWeek` counts from 0 = Sunday, as in .NET. `DateTimeOffset` parts do not translate.

A `DateTime`, `DateOnly` or `DateTimeOffset` constant in a query is written as `CAST('2024-01-02T03:04:05.0000000Z' AS datetime)` or `CAST('2024-01-02' AS date)`, in UTC. Earlier versions of the provider wrote `TIMESTAMP '…'`, which the server does not parse.

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

### Math functions

The `Math` methods translate to the server's numeric functions. The trigonometric functions need a server from 2026-11-13 or later.

| LINQ | CamusDB |
| --- | --- |
| `Math.Abs(x)` | `abs(x)` |
| `Math.Ceiling(x)`, `Math.Floor(x)`, `Math.Truncate(x)` | `ceil(x)`, `floor(x)`, `trunc(x)` |
| `Math.Round(x, MidpointRounding.AwayFromZero)` | `round(x)` |
| `Math.Round(x, n, MidpointRounding.AwayFromZero)` | `round(x, n)` |
| `Math.Sign(x)` | `sign(x)` |
| `Math.Sqrt(x)`, `Math.Exp(x)`, `Math.Pow(x, y)` | `sqrt(x)`, `exp(x)`, `pow(x, y)` |
| `Math.Log(x)`, `Math.Log(x, b)`, `Math.Log10(x)` | `ln(x)`, `log(b, x)`, `log10(x)` |
| `Math.Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan` | `sin`, `cos`, `tan`, `asin`, `acos`, `atan` |
| `Math.Atan2(y, x)` | `atan2(y, x)` |
| `double.DegreesToRadians(x)`, `double.RadiansToDegrees(x)` | `radians(x)`, `degrees(x)` |

```csharp
var near = await ctx.Places
    .Where(p => Math.Sin(double.DegreesToRadians(p.Latitude)) > 0.5)   // sin(radians(`p`.`latitude`)) > 0.5
    .ToListAsync();
```

- `abs`, `ceil`, `floor`, `trunc` and `round` translate for `double` and `decimal`. `Math.Abs` and `Math.Sign` also translate for `short`, `int` and `long`. The other functions translate for `double`.
- `Math.Max(a, b)` and `Math.Min(a, b)` are `greatest(a, b)` and `least(a, b)`. Nested calls become one call: `Math.Max(Math.Max(a, b), c)` is `greatest(a, b, c)`. The server leaves a `NULL` argument out, and the result is `NULL` only when every argument is `NULL`. These need a server from 2026-11-17 or later. Earlier versions of the provider wrote a `CASE` expression.
- `EF.Functions.Random()` is `random()`. Earlier versions of the provider wrote `RAND()`, which the server does not have.
- `Math.Round` without `MidpointRounding.AwayFromZero` does not translate. .NET rounds a midpoint to the even number, and the server rounds it away from zero.
- `Math.Sign(decimal)` does not translate, because the server returns a `numeric` for it.
- `MathF` (`float`) does not translate.
- A value out of the domain fails on the server with `CADB0400`, for example `Math.Asin(2)` or `Math.Log(0)`. .NET returns `NaN` for these.

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
| `from a in A from b in B.Where(b => b.X > a.Y).Take(n)` (a correlation that is not an equality key) | `CROSS JOIN LATERAL (…)` |
| the same with `.DefaultIfEmpty()`, or a projected collection with `Take`/`Skip` | `LEFT JOIN LATERAL (…) ON TRUE` |
| `Take`/`Skip`/`First` for each row: `Include(c => c.Orders.OrderBy(...).Take(n))`, `SelectMany` over `c.Orders.Take(n)`, a per-group `First()` | `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` in a derived table |

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

```csharp
// The two largest orders of each customer, also for a customer with no order.
var top = await ctx.Customers
    .Select(c => new { c.Name, Top = c.Orders.Where(o => o.Total > c.Limit).OrderByDescending(o => o.Total).Take(2).ToList() })
    .ToListAsync();   // LEFT JOIN LATERAL (SELECT … ORDER BY … LIMIT 2) AS `o0` ON TRUE
```

- The server runs a `LATERAL` join as a nested loop. The body runs once for each left row, and left rows with the same values share one run. The server keeps the written join order. `LATERAL` needs a server from 2026-11-15 or later.
- `ROW_NUMBER()` needs a server from 2026-10-30 or later. When the inner sequence has no order and no key (a keyless entity), the provider writes `OVER (PARTITION BY …)` with no `ORDER BY`, and the row that `First()` or `Take` keeps is not defined.
- Earlier versions of the provider threw `InvalidOperationException` for these shapes.

`FULL JOIN` is not supported.

### Inline and parameter collections

An array in the query that is used for more than `Contains` becomes a `VALUES` list: an inline array (`new[] { c.Id, c.ParentId }`) or a parameter array (`ids`). `ids.Contains(x)` stays `x IN (@ids1, @ids2, …)`.

```csharp
long[] ids = [1, 2, 3];
var rows = await ctx.Customers.Where(c => ids.Any(i => i > c.Id)).ToListAsync();
// WHERE EXISTS (SELECT 1 FROM (VALUES (@ids1), (@ids2), (@ids3)) AS `i` (`Value`) WHERE `i`.`Value` > `c`.`Id`)

var largest = await ctx.Customers.Select(c => new[] { c.Id, c.RegionId, 4L }.Where(v => v < 9).Max()).ToListAsync();
// SELECT (SELECT MAX(`v`.`Value`) FROM (VALUES (`c`.`Id`), (`c`.`RegionId`), (4)) AS `v` (`Value`) WHERE `v`.`Value` < 9) …
```

- `new[] { a, b }.Max()` and `.Min()` with no filter are `greatest(a, b)` and `least(a, b)`.
- An empty parameter array reads no rows; the provider does not send a `VALUES` list for it.
- An item that applies an operator to a parameter, such as `c.Id - offset`, is written as ``CAST(`c`.`Id` - @offset AS int64)``. The server gives such an item the type `string` in a `VALUES` column, so the cast gives it its real type.
- A `VALUES` list needs a server from 2026-11-16 or later. Earlier versions of the provider wrote `SELECT … UNION ALL VALUES …`, which the server does not parse.

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
