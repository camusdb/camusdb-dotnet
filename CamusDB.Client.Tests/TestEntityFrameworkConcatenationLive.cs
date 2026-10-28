/**
 * This file is part of CamusDB
 *
 * Live coverage for LINQ string concatenation on both transports: EF Core writes it as || (see
 * TestEntityFrameworkConcatenationSql), and the results must match C#, where "a" + null is "a".
 * Requires a server with the || operator, so the tests are opt-in: set CAMUSDB_TEST_CONCATENATION=true.
 * The REST endpoint comes from CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint
 * from CAMUSDB_TEST_GRPC_ENDPOINT (default localhost:5096).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkConcatenationLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_CONCATENATION"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    // One fixed table: EF Core builds one model for each context type. Each test tags its rows.
    private const string Table = "concat_people";

    /// <summary>Adds (Ada, Lovelace, 36) and (Alan, NULL, 41) under a new tag, and returns the tag.</summary>
    private static async Task<string> SeedAsync(string protocol)
    {
        await using (CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol))))
        {
            await connection.OpenAsync();
            await connection.CreateDatabaseAsync("test", ifNotExists: true);

            await using CamusCommand create = connection.CreateCamusCommand(
                $"CREATE TABLE IF NOT EXISTS {Table} (id STRING NOT NULL, tag STRING NOT NULL, first STRING NOT NULL, last STRING, age INT64 NOT NULL, PRIMARY KEY (id))");
            await create.ExecuteDDLAsync();
        }

        string tag = Guid.NewGuid().ToString("n");

        await using PeopleContext ctx = NewContext(protocol);
        ctx.People.Add(new Person { Id = Guid.NewGuid().ToString("n"), Tag = tag, First = "Ada", Last = "Lovelace", Age = 36 });
        ctx.People.Add(new Person { Id = Guid.NewGuid().ToString("n"), Tag = tag, First = "Alan", Last = null, Age = 41 });
        await ctx.SaveChangesAsync();

        return tag;
    }

    private static PeopleContext NewContext(string protocol)
        => new(new DbContextOptionsBuilder<PeopleContext>().UseCamusDB(ConnString(protocol)).Options);

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ProjectionMatchesCSharp(string protocol)
    {
        if (!Enabled)
            return;

        string tag = await SeedAsync(protocol);
        await using PeopleContext ctx = NewContext(protocol);
        IQueryable<Person> people = ctx.People.Where(p => p.Tag == tag).OrderBy(p => p.First);

        // Before 0.16.8 the provider wrote +, and the server refused it with CADB0400.
        Assert.Equal(["Ada Lovelace", "Alan "], await people.Select(p => p.First + " " + p.Last).ToListAsync());
        Assert.Equal(["Ada:Lovelace", "Alan:"], await people.Select(p => string.Concat(p.First, ":", p.Last)).ToListAsync());
        Assert.Equal(["Ada36", "Alan41"], await people.Select(p => p.First + p.Age).ToListAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ConcatenationFiltersAndOrders(string protocol)
    {
        if (!Enabled)
            return;

        string tag = await SeedAsync(protocol);
        await using PeopleContext ctx = NewContext(protocol);
        IQueryable<Person> people = ctx.People.Where(p => p.Tag == tag);

        string full = "AdaLovelace";
        Assert.Equal(["Ada"], await people.Where(p => p.First + p.Last == full).Select(p => p.First).ToListAsync());

        // A NULL last name counts as '' in the predicate too, as in C#.
        Assert.Equal(["Alan"], await people.Where(p => p.First + p.Last == "Alan").Select(p => p.First).ToListAsync());

        // "Alan" (NULL + "Alan") sorts before "LovelaceAda".
        Assert.Equal(["Alan", "Ada"], await people.OrderBy(p => p.Last + p.First).Select(p => p.First).ToListAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task InterpolationMatchesCSharp(string protocol)
    {
        if (!Enabled)
            return;

        string tag = await SeedAsync(protocol);
        await using PeopleContext ctx = NewContext(protocol);
        IQueryable<Person> people = ctx.People.Where(p => p.Tag == tag);

        // Before 0.16.9 an interpolated string (string.Format) in a Where did not translate.
        string full = "Ada Lovelace";
        Assert.Equal(["Ada"], await people.Where(p => $"{p.First} {p.Last}" == full).Select(p => p.First).ToListAsync());

        // A NULL argument is "", as in C#.
        Assert.Equal(["Alan"], await people.Where(p => $"{p.First} {p.Last}" == "Alan ").Select(p => p.First).ToListAsync());

        // Integers, a negative one too, and literal braces. The SQL result must equal the C# result.
        List<Person> stored = await people.OrderBy(p => p.First).ToListAsync();
        List<string> expected = stored.Select(p => $"{{{p.First}}}#{p.Age}/{-p.Age}:{p.Last}").ToList();

        List<string> translated = await people.OrderBy(p => p.First)
            .Select(p => $"{{{p.First}}}#{p.Age}/{-p.Age}:{p.Last}")
            .ToListAsync();

        Assert.Equal(["{Ada}#36/-36:Lovelace", "{Alan}#41/-41:"], expected);
        Assert.Equal(expected, translated);

        // Ordered by an interpolation: "Alan" + "" sorts before "Ada" + "Lovelace" when the last name comes first.
        Assert.Equal(["Alan", "Ada"], await people.OrderBy(p => $"{p.Last}{p.First}").Select(p => p.First).ToListAsync());
    }

    private sealed class PeopleContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Person> People => Set<Person>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Person>(b =>
            {
                b.ToTable(Table);
                b.HasKey(p => p.Id);
                b.Property(p => p.Id).HasColumnName("id").ValueGeneratedNever();
                b.Property(p => p.Tag).HasColumnName("tag");
                b.Property(p => p.First).HasColumnName("first");
                b.Property(p => p.Last).HasColumnName("last");
                b.Property(p => p.Age).HasColumnName("age");
            });
    }

    private sealed class Person
    {
        public string Id { get; set; } = "";

        public string Tag { get; set; } = "";

        public string First { get; set; } = "";

        public string? Last { get; set; }

        public long Age { get; set; }
    }
}
