/**
 * This file is part of CamusDB
 *
 * Offline (no server) coverage for string concatenation: the SQL the provider emits for a + b and
 * string.Concat. CamusDB joins strings with ||, and refuses + for two strings (CADB0400). ToQueryString
 * compiles the query without opening a connection.
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
            });
    }

    private sealed class Person
    {
        public long Id { get; set; }

        public string First { get; set; } = "";

        public string? Last { get; set; }

        public int Age { get; set; }
    }
}
