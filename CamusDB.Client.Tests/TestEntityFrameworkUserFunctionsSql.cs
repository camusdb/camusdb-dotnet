/**
 * This file is part of CamusDB
 *
 * Offline (no server) checks of the SQL the provider emits for a function mapped with HasDbFunction,
 * which is how a C# extension function (a [CamusFunction] method loaded by the server) is called from
 * LINQ. The server parses a function name as a plain identifier only, so the name must not be delimited.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkUserFunctionsSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static FunctionsContext NewContext() =>
        new(new DbContextOptionsBuilder<FunctionsContext>().UseCamusDB(ConnString).Options);

    [Fact]
    public void MappedFunctionIsEmittedWithoutDelimiters()
    {
        using FunctionsContext ctx = NewContext();

        string sql = ctx.Posts.Where(p => FunctionsContext.Slugify(p.Title) == "hello-world").ToQueryString();

        Assert.Contains("slugify(`u`.`title`)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("`slugify`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MappedFunctionInProjection()
    {
        using FunctionsContext ctx = NewContext();

        string sql = ctx.Posts.Select(p => FunctionsContext.Greet(p.Title, "hi")).ToQueryString();

        Assert.Contains("greet(`u`.`title`, 'hi')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SchemaOfMappedFunctionIsDropped()
    {
        using FunctionsContext ctx = NewContext();

        string sql = ctx.Posts.Select(p => FunctionsContext.RandomToken()).ToQueryString();

        Assert.Contains("random_token()", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ext.", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FunctionNameThatIsNotAPlainIdentifierIsRefused()
    {
        using BadNameContext ctx = new(new DbContextOptionsBuilder<BadNameContext>().UseCamusDB(ConnString).Options);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Posts.Select(p => BadNameContext.Dashed(p.Title)).ToQueryString());

        Assert.Contains("my-func", ex.Message, StringComparison.Ordinal);
    }

    private class FunctionsContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<UfPost> Posts => Set<UfPost>();

        public static string Slugify(string value) => throw new NotSupportedException();

        public static string Greet(string? name, string greeting) => throw new NotSupportedException();

        public static string RandomToken() => throw new NotSupportedException();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<UfPost>(b =>
            {
                b.ToTable("uf_posts_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Title).HasColumnName("title").HasMaxLength(128);
            });

            mb.HasDbFunction(typeof(FunctionsContext).GetMethod(nameof(Slugify))!).HasName("slugify");
            mb.HasDbFunction(typeof(FunctionsContext).GetMethod(nameof(Greet))!).HasName("greet");
            mb.HasDbFunction(typeof(FunctionsContext).GetMethod(nameof(RandomToken))!).HasName("random_token").HasSchema("ext");
        }
    }

    private class BadNameContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<UfPost> Posts => Set<UfPost>();

        public static string Dashed(string value) => throw new NotSupportedException();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<UfPost>(b =>
            {
                b.ToTable("uf_posts_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Title).HasColumnName("title").HasMaxLength(128);
            });

            mb.HasDbFunction(typeof(BadNameContext).GetMethod(nameof(Dashed))!).HasName("my-func");
        }
    }

    private class UfPost
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
    }
}
