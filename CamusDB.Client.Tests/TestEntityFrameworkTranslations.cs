/**
 * This file is part of CamusDB
 *
 * Coverage for the LINQ members that the provider translates onto the server's functions: the date and
 * time parts, the string members (Substring, IndexOf, the indexer, the char overloads,
 * IsNullOrWhiteSpace), ToString and Parse, Guid.NewGuid, EF.Functions.Random, Math.Max and Math.Min,
 * array Length/Count/Any/Contains, byte[].Length, and the date literal forms. The offline tests pin the
 * SQL text (ToQueryString opens no connection); the live test compares each server result with the .NET
 * result for the same value. The live test needs a server on :5095.
 */

using System.Linq.Expressions;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkTranslations
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static TranslationContext NewContext() =>
        new(new DbContextOptionsBuilder<TranslationContext>().UseCamusDB(ConnString).Options);

    private static string WhereSql(Expression<Func<TranslationRow, bool>> predicate)
    {
        using TranslationContext ctx = NewContext();
        string sql = ctx.Rows.Where(predicate).ToQueryString().Replace("\r\n", "\n");
        return sql[sql.IndexOf("WHERE ", StringComparison.Ordinal)..];
    }

    public static TheoryData<string, Expression<Func<TranslationRow, bool>>> Predicates()
    {
        char variable = 'q';
        return new()
        {
            { "WHERE date_part('year', `e`.`at`) = 2024", r => r.At.Year == 2024 },
            { "WHERE date_part('millisecond', `e`.`at`) = 678", r => r.At.Millisecond == 678 },
            { "WHERE date_part('dow', `e`.`at`) = 2", r => r.At.DayOfWeek == DayOfWeek.Tuesday },
            { "WHERE date_part('doy', `e`.`day`) = 60", r => r.Day.DayOfYear == 60 },
            { "WHERE date_trunc('day', `e`.`at`) = CAST('2024-03-05T00:00:00.0000000Z' AS datetime)", r => r.At.Date == new DateTime(2024, 3, 5, 0, 0, 0, DateTimeKind.Utc) },
            { "WHERE (`e`.`at` - date_trunc('day', `e`.`at`)) > INTERVAL 'PT1H'", r => r.At.TimeOfDay > TimeSpan.FromHours(1) },
            { "WHERE CAST(`e`.`at` AS date) = CAST('2024-03-05' AS date)", r => DateOnly.FromDateTime(r.At) == new DateOnly(2024, 3, 5) },
            { "WHERE substring(`e`.`name`, 3) = 'x'", r => r.Name.Substring(2) == "x" },
            { "WHERE substring(`e`.`name`, `e`.`n` + 1, 5) = 'x'", r => r.Name.Substring(r.N, 5) == "x" },
            { "WHERE (strpos(`e`.`name`, 'World') - 1) = 8", r => r.Name.IndexOf("World") == 8 },
            { "WHERE (strpos(`e`.`name`, @variable) - 1) = 8", r => r.Name.IndexOf(variable) == 8 },
            { "WHERE substring(`e`.`name`, 3, 1) = 'H'", r => r.Name[2] == 'H' },
            // name is a required column, so EF Core drops the IS NULL test.
            { "WHERE regexp_like(`e`.`name`, '^\\s*$')", r => string.IsNullOrWhiteSpace(r.Name) },
            { "WHERE trim(`e`.`name`, 'x') = 'a'", r => r.Name.Trim('x') == "a" },
            { "WHERE rtrim(`e`.`name`, 'xd') = 'a'", r => r.Name.TrimEnd('x', 'd') == "a" },
            { "WHERE ltrim(`e`.`name`, @variable) = 'a'", r => r.Name.TrimStart(variable) == "a" },
            { "WHERE replace(`e`.`name`, 'o', '0') = 'a'", r => r.Name.Replace('o', '0') == "a" },
            { "WHERE contains(`e`.`name`, @variable)", r => r.Name.Contains(variable) },
            { "WHERE starts_with(`e`.`name`, 'x')", r => r.Name.StartsWith('x') },
            { "WHERE CAST(`e`.`n` AS string) = '3'", r => r.N.ToString() == "3" },
            { "WHERE CAST(`e`.`x` AS string) = '2.5'", r => Convert.ToString(r.X) == "2.5" },
            { "WHERE CAST(`e`.`g` AS string) = 'a'", r => r.G.ToString() == "a" },
            { "WHERE CAST(`e`.`numtext` AS int64) = 42", r => int.Parse(r.NumText) == 42 },
            { "WHERE CAST(`e`.`guidtext` AS uuid) = `e`.`g`", r => Guid.Parse(r.GuidText) == r.G },
            { "WHERE `e`.`g` <> gen_uuid_v4()", r => r.G != Guid.NewGuid() },
            { "WHERE random() < 2.0", r => EF.Functions.Random() < 2.0 },
            { "WHERE cardinality(`e`.`tags`) = 3", r => r.Tags.Length == 3 },
            { "WHERE cardinality(`e`.`tags`) = 3", r => r.Tags.Count() == 3 },
            { "WHERE cardinality(`e`.`tags`) > 0", r => r.Tags.Any() },
            { "WHERE array_contains(`e`.`tags`, 2)", r => r.Tags.Contains(2L) },
            { "WHERE array_contains(`e`.`names`, `e`.`name`)", r => r.Names.Contains(r.Name) },
            // Names is mapped with Property, so on EF Core 11 it is a scalar column, not a primitive collection.
            { "WHERE cardinality(`e`.`names`) = 2", r => r.Names.Count() == 2 },
            { "WHERE cardinality(`e`.`names`) = 2", r => r.Names.LongCount() == 2L },
            { "WHERE cardinality(`e`.`names`) > 0", r => r.Names.Any() },
            { "WHERE array_contains(`e`.`names`, 'b')", r => r.Names.Contains("b") },
            { "WHERE octet_length(`e`.`data`) = 4", r => r.Data.Length == 4 },
        };
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public void PredicateTranslatesToServerFunctions(string expected, Expression<Func<TranslationRow, bool>> predicate)
        => Assert.Equal(expected, WhereSql(predicate).Replace("\n", " "));

    [Fact]
    public void BoolToStringWritesTheDotNetText()
        => Assert.Equal(
            "WHERE CASE\n    WHEN `e`.`b` THEN 'True'\n    ELSE 'False'\nEND = 'True'",
            WhereSql(r => r.B.ToString() == "True"));

    [Fact]
    public void MaxAndMinAreGreatestAndLeast()
    {
        Assert.Equal("WHERE greatest(`e`.`x`, 1.0) > 2.0", WhereSql(r => Math.Max(r.X, 1.0) > 2));

        // Nested calls are one list, and so are Max and Min over an inline array.
        Assert.Equal("WHERE least(`e`.`n`, 2, 7) = 2", WhereSql(r => Math.Min(Math.Min(r.N, 2), 7) == 2));
        Assert.Equal("WHERE greatest(`e`.`n`, 2, 7) = 7", WhereSql(r => new[] { r.N, 2, 7 }.Max() == 7));
    }

    [Fact]
    public void ArrayElementQueriesOtherThanContainsCountAnyAreRefused()
    {
        using TranslationContext ctx = NewContext();

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Rows.Where(r => r.Tags.Count(t => t > 2) > 0).ToQueryString());

        Assert.Contains("elements of an array column", ex.Message);
    }

    [Fact]
    public void ParameterListContainsStaysAnInList()
    {
        string[] names = ["a", "b"];

        Assert.Equal("WHERE `e`.`name` IN (@names1, @names2)", WhereSql(r => names.Contains(r.Name)));
    }

    [Fact]
    public void DateLiteralsAreCastsOfUtcText()
    {
        Assert.Equal("WHERE `e`.`at` > CAST('2024-01-02T03:04:05.0060000Z' AS datetime)",
            WhereSql(r => r.At > new DateTime(2024, 1, 2, 3, 4, 5, 6, DateTimeKind.Utc)));
        Assert.Equal("WHERE `e`.`day` > CAST('2024-01-02' AS date)",
            WhereSql(r => r.Day > new DateOnly(2024, 1, 2)));
    }

    private static readonly DateTime At = new(2024, 3, 5, 13, 14, 15, 678, DateTimeKind.Utc);

    private static readonly Guid G = new("2b1e8e1c-7e0b-4f4e-9b3b-3c2d1a0f9e8d");

    [Fact]
    public async Task TranslationsGiveTheDotNetResultsOnTheServer()
    {
        await using TranslationContext ctx = NewContext();
        await ctx.Database.EnsureCreatedAsync();

        string tag = Guid.NewGuid().ToString("n");
        TranslationRow row = new()
        {
            Id = Guid.NewGuid().ToString("n"), Tag = tag, Name = "xxHello Worldxx", At = At, Day = new DateOnly(2024, 2, 29),
            X = 2.5, N = 3, B = true, G = G, NumText = "42", GuidText = G.ToString(), Tags = [1, 2, 3], Names = ["a", "b"], Data = [1, 2, 3, 4],
        };
        ctx.Rows.Add(row);
        ctx.Rows.Add(new TranslationRow
        {
            Id = Guid.NewGuid().ToString("n"), Tag = tag, Name = " \t\n ", At = At, Day = row.Day, NumText = "0", GuidText = G.ToString(),
        });
        await ctx.SaveChangesAsync();

        IQueryable<TranslationRow> rows = ctx.Rows.AsNoTracking().Where(r => r.Tag == tag && r.N == 3);

        var dates = await rows.Select(r => new
        {
            r.At.Year, r.At.Month, r.At.Day, r.At.Hour, r.At.Minute, r.At.Second, r.At.Millisecond,
            r.At.DayOfWeek, r.At.DayOfYear, r.At.Date, r.At.TimeOfDay,
            DayYear = r.Day.Year, DayOfYearOfDate = r.Day.DayOfYear, DayOfWeekOfDate = r.Day.DayOfWeek,
            FromDateTime = DateOnly.FromDateTime(r.At),
        }).SingleAsync();

        Assert.Equal((At.Year, At.Month, At.Day, At.Hour, At.Minute, At.Second, At.Millisecond),
            (dates.Year, dates.Month, dates.Day, dates.Hour, dates.Minute, dates.Second, dates.Millisecond));
        Assert.Equal(At.DayOfWeek, dates.DayOfWeek);
        Assert.Equal(At.DayOfYear, dates.DayOfYear);
        Assert.Equal(At.Date, dates.Date);
        Assert.Equal(At.TimeOfDay, dates.TimeOfDay);
        Assert.Equal((2024, 60, DayOfWeek.Thursday), (dates.DayYear, dates.DayOfYearOfDate, dates.DayOfWeekOfDate));
        Assert.Equal(DateOnly.FromDateTime(At), dates.FromDateTime);

        var strings = await rows.Select(r => new
        {
            From = r.Name.Substring(2), Part = r.Name.Substring(2, 5), Index = r.Name.IndexOf("World"), CharIndex = r.Name.IndexOf('W'),
            Missing = r.Name.IndexOf("zz"), Third = r.Name[2], Trim = r.Name.Trim('x'), TrimStart = r.Name.TrimStart('x'),
            TrimEnd = r.Name.TrimEnd('x', 'd'), Replaced = r.Name.Replace('o', '0'), HasW = r.Name.Contains('W'),
            StartsX = r.Name.StartsWith('x'), EndsY = r.Name.EndsWith('y'),
        }).SingleAsync();

        string name = row.Name;
        Assert.Equal(name.Substring(2), strings.From);
        Assert.Equal(name.Substring(2, 5), strings.Part);
        Assert.Equal(name.IndexOf("World", StringComparison.Ordinal), strings.Index);
        Assert.Equal(name.IndexOf('W'), strings.CharIndex);
        Assert.Equal(-1, strings.Missing);
        Assert.Equal(name[2], strings.Third);
        Assert.Equal(name.Trim('x'), strings.Trim);
        Assert.Equal(name.TrimStart('x'), strings.TrimStart);
        Assert.Equal(name.TrimEnd('x', 'd'), strings.TrimEnd);
        Assert.Equal(name.Replace('o', '0'), strings.Replaced);
        Assert.Equal((true, true, false), (strings.HasW, strings.StartsX, strings.EndsY));

        var conversions = await rows.Select(r => new
        {
            N = r.N.ToString(), X = r.X.ToString(), B = r.B.ToString(), G = r.G.ToString(), C = Convert.ToString(r.N),
            Parsed = int.Parse(r.NumText), ParsedGuid = Guid.Parse(r.GuidText),
            Max = Math.Max(r.X, 1.0), Min = Math.Min(r.N, 2), Nested = Math.Max(Math.Max(r.N, 1), 7),
            Random = EF.Functions.Random(), NewGuid = Guid.NewGuid(),
            TagCount = r.Tags.Length, TagCount2 = r.Tags.Count(), HasTags = r.Tags.Any(), HasTwo = r.Tags.Contains(2L),
            HasB = r.Names.Contains("b"), Bytes = r.Data.Length,
        }).SingleAsync();

        Assert.Equal(("3", "2.5", "True", G.ToString(), "3"), (conversions.N, conversions.X, conversions.B, conversions.G, conversions.C));
        Assert.Equal((42, G), (conversions.Parsed, conversions.ParsedGuid));
        Assert.Equal((2.5, 2, 7), (conversions.Max, conversions.Min, conversions.Nested));
        Assert.InRange(conversions.Random, 0.0, 1.0);
        Assert.NotEqual(Guid.Empty, conversions.NewGuid);
        Assert.NotEqual(G, conversions.NewGuid);
        Assert.Equal((3, 3, true, true, true, 4),
            (conversions.TagCount, conversions.TagCount2, conversions.HasTags, conversions.HasTwo, conversions.HasB, conversions.Bytes));

        // A filter cannot fall back to the client, so these prove that the server runs each translation.
        IQueryable<TranslationRow> tagged = ctx.Rows.Where(r => r.Tag == tag);
        Assert.Equal(1, await tagged.CountAsync(r => string.IsNullOrWhiteSpace(r.Name)));
        Assert.Equal(1, await tagged.CountAsync(r => r.At.DayOfWeek == DayOfWeek.Tuesday && r.At.Millisecond == 678 && r.N == 3));
        Assert.Equal(1, await tagged.CountAsync(r => r.Name.Substring(2, 5) == "Hello" && r.Name.IndexOf('W') == 8 && r.Name[0] == 'x'));
        Assert.Equal(1, await tagged.CountAsync(r => r.Tags.Contains(3L) && r.Tags.Length == 3 && r.Data.Length == 4));
        Assert.Equal(1, await tagged.CountAsync(r => r.Names.Contains("b") && r.Names.Count() == 2 && r.Names.Any()));
        Assert.Equal(1, await tagged.CountAsync(r => r.B.ToString() == "True" && int.Parse(r.NumText) == 42));
        Assert.Equal(2, await tagged.CountAsync(r => r.At.Date == new DateTime(2024, 3, 5, 0, 0, 0, DateTimeKind.Utc) && r.Day < new DateOnly(2024, 3, 1)));
        Assert.Equal(1, await tagged.CountAsync(r => Math.Max(r.X, 1.0) == 2.5));
        Assert.Equal(1, await tagged.CountAsync(r => Math.Min(Math.Min(r.N, 2), 7) == 2 && new[] { r.N, 2, 7 }.Max() == 7));
        Assert.Equal(2, await tagged.CountAsync(r => r.G != Guid.NewGuid() && EF.Functions.Random() < 2.0));
    }

    private sealed class TranslationContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<TranslationRow> Rows => Set<TranslationRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<TranslationRow>(b =>
            {
                b.ToTable("ef_translations_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id");
                b.Property(e => e.Tag).HasColumnName("tag");
                b.Property(e => e.Name).HasColumnName("name");
                b.Property(e => e.At).HasColumnName("at");
                b.Property(e => e.Day).HasColumnName("day");
                b.Property(e => e.X).HasColumnName("x");
                b.Property(e => e.N).HasColumnName("n");
                b.Property(e => e.B).HasColumnName("b");
                b.Property(e => e.G).HasColumnName("g");
                b.Property(e => e.NumText).HasColumnName("numtext");
                b.Property(e => e.GuidText).HasColumnName("guidtext");
                // Tags is a primitive collection on every EF Core version. From EF Core 11, Property maps an
                // array as a scalar column, so Names covers that form (CamusArrayMethodTranslator).
                b.PrimitiveCollection(e => e.Tags).HasColumnName("tags");
                b.Property(e => e.Names).HasColumnName("names");
                b.Property(e => e.Data).HasColumnName("data");
            });
        }
    }

    public sealed class TranslationRow
    {
        public string Id { get; set; } = "";
        public string Tag { get; set; } = "";
        public string Name { get; set; } = "";
        public DateTime At { get; set; }
        public DateOnly Day { get; set; }
        public double X { get; set; }
        public int N { get; set; }
        public bool B { get; set; }
        public Guid G { get; set; }
        public string NumText { get; set; } = "";
        public string GuidText { get; set; } = "";
        public long[] Tags { get; set; } = [];
        public string[] Names { get; set; } = [];
        public byte[] Data { get; set; } = [];
    }
}
