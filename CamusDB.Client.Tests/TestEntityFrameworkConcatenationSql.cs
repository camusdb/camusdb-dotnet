/**
 * This file is part of CamusDB
 *
 * Offline (no server) coverage for string concatenation: the SQL the provider emits for a + b,
 * string.Concat and string interpolation (string.Format). CamusDB joins strings with ||, and refuses
 * + for two strings (CADB0400). ToQueryString compiles the query without opening a connection.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkConcatenationSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static SqlContext NewContext() =>
        new(new DbContextOptionsBuilder<SqlContext>().UseCamusDB(ConnString).Options);

    [Fact]
    public void StringPlusBecomesConcatenation()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => p.First + "!").ToQueryString();

        Assert.Contains("`p`.`first` || '!'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(" + ", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullableOperandIsCoalesced()
    {
        using SqlContext ctx = NewContext();

        // C# "a" + null is "a"; || gives NULL for a NULL operand, so EF's COALESCE must stay.
        string sql = ctx.People.Select(p => p.First + " " + p.Last).ToQueryString();

        Assert.Contains("(`p`.`first` || ' ') || COALESCE(`p`.`last`, '')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StringConcatBecomesConcatenation()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => string.Concat(p.First, " ", p.Last)).ToQueryString();

        Assert.Contains("`p`.`first` || (' ' || COALESCE(`p`.`last`, ''))", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ANumberIsCastBeforeConcatenation()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => p.First + p.Age).ToQueryString();

        Assert.Contains("`p`.`first` || CAST(`p`.`age` AS string)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ConcatenationInAPredicateIsParenthesized()
    {
        using SqlContext ctx = NewContext();
        string name = "ab";

        string sql = ctx.People.Where(p => p.First + p.Last == name).ToQueryString();

        Assert.Contains("(`p`.`first` || COALESCE(`p`.`last`, '')) = @name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void NumericAddIsUnchanged()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => p.Age + 1).ToQueryString();

        Assert.Contains("`p`.`age` + 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("||", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InterpolationBecomesConcatenation()
    {
        using SqlContext ctx = NewContext();
        string name = "Ada Lovelace";

        // In an expression tree, $"{a} {b}" is string.Format("{0} {1}", a, b).
        string sql = ctx.People.Where(p => $"{p.First} {p.Last}" == name).ToQueryString();

        Assert.Contains("((`p`.`first` || ' ') || COALESCE(`p`.`last`, '')) = @name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InterpolationCastsAnIntegerAndCoalescesANullableOne()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => $"{p.First}#{p.Age}/{p.Score}").ToQueryString();

        Assert.Contains("CAST(`p`.`age` AS string)", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(CAST(`p`.`score` AS string), '')", sql, StringComparison.Ordinal);
        Assert.Contains("'#'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InterpolationWithManyArgumentsTranslates()
    {
        using SqlContext ctx = NewContext();

        // Four or more arguments use the params object[] overload.
        string sql = ctx.People.Select(p => $"{p.First}{p.Last}{p.Age}{p.Id}{p.First}").ToQueryString();

        Assert.Contains("CAST(`p`.`Id` AS string)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT `p`.`Id`, ", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ASinglePlaceholderIsCoalesced()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => $"{p.Last}").ToQueryString();

        Assert.Contains("COALESCE(`p`.`last`, '')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BracesAreLiteral()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.People.Select(p => $"{{{p.First}}}").ToQueryString();

        Assert.Contains("('{' || `p`.`first`) || '}'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AFormatStringIsNotTranslated()
    {
        using SqlContext ctx = NewContext();

        // {0:D4} has no SQL equivalent: the Where cannot translate, and the final projection runs on the client.
        Assert.Throws<InvalidOperationException>(() => ctx.People.Where(p => $"{p.Age:D4}" == "0036").ToQueryString());
        Assert.DoesNotContain("||", ctx.People.Select(p => $"{p.Age:D4}").ToQueryString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ANonIntegerNumberIsNotTranslated()
    {
        using SqlContext ctx = NewContext();

        // The text of a double depends on the culture in C#, so it stays on the client.
        Assert.Throws<InvalidOperationException>(() => ctx.People.Where(p => $"{p.First}{p.Rating}" == "x").ToQueryString());
    }

    [Theory]
    [InlineData("{0} {1}", 2, "[0]| |[1]")]
    [InlineData("a{0}b", 1, "a|[0]|b")]
    [InlineData("{{x}}", 0, "{x}")]
    [InlineData("{{{0}}}", 1, "{|[0]|}")]
    [InlineData("{0}{0}", 1, "[0]|[0]")]
    [InlineData("", 0, "")]
    public void ParseFormatSplitsLiteralsAndPlaceholders(string format, int count, string expected)
    {
        List<(string? Literal, int Index)> parts = [];

        Assert.True(CamusSqlTranslatingExpressionVisitor.TryParseFormat(format, count, parts));
        Assert.Equal(expected, string.Join("|", parts.Select(p => p.Literal ?? $"[{p.Index}]")));
    }

    [Theory]
    [InlineData("{0:N2}", 1)]
    [InlineData("{0,5}", 1)]
    [InlineData("{ 0}", 1)]
    [InlineData("{1}", 1)]
    [InlineData("{0", 1)]
    [InlineData("a}b", 0)]
    [InlineData("{}", 1)]
    public void ParseFormatRefusesWhatSqlCannotMatch(string format, int count)
        => Assert.False(CamusSqlTranslatingExpressionVisitor.TryParseFormat(format, count, []));

    private sealed class SqlContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Person> People => Set<Person>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Person>(b =>
            {
                b.ToTable("people");
                b.HasKey(p => p.Id);
                b.Property(p => p.First).HasColumnName("first");
                b.Property(p => p.Last).HasColumnName("last");
                b.Property(p => p.Age).HasColumnName("age");
                b.Property(p => p.Score).HasColumnName("score");
                b.Property(p => p.Rating).HasColumnName("rating");
            });
    }

    private sealed class Person
    {
        public long Id { get; set; }

        public string First { get; set; } = "";

        public string? Last { get; set; }

        public int Age { get; set; }

        public int? Score { get; set; }

        public double Rating { get; set; }
    }
}
