/**
 * This file is part of CamusDB
 *
 * Regression coverage for parameterized-collection `.Contains(...)` translation. EF's relational
 * default (ParameterTranslationMode.Parameter) passes the collection as a single array-valued
 * parameter expanded via an OPENJSON-style construct, which the CamusDB provider does not implement
 * — it produced a DbParameter with a null type mapping and a NullReferenceException while EF built
 * the command. The provider now defaults to MultipleParameters (IN (@p0, @p1, …)); these tests pin
 * that a `local.Contains(entity.Column)` filter round-trips.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkContains
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<PeopleContext> Options() =>
        new DbContextOptionsBuilder<PeopleContext>().UseCamusDB(ConnString).Options;

    private static async Task<string> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using var ctx = new PeopleContext(Options());
        await ctx.Database.EnsureCreatedAsync();

        foreach (string name in new[] { "alice", "bob", "carol", "dave" })
            ctx.People.Add(new Person { Id = CamusObjectIdGenerator.GenerateAsString(), Name = name, Tag = tag });

        await ctx.SaveChangesAsync();
        return tag;
    }

    [Fact]
    public async Task ContainsOnParameterizedArrayFiltersRows()
    {
        string tag = await SeedAsync();
        await using var ctx = new PeopleContext(Options());

        // The array is a captured local → EF parameterizes it. This is the shape that used to NRE.
        string[] wanted = ["alice", "carol", "zzz-missing"];

        Dictionary<string, Person> found = await ctx.People
            .Where(p => p.Tag == tag && wanted.Contains(p.Name))
            .ToDictionaryAsync(p => p.Name, StringComparer.Ordinal);

        Assert.Equal(2, found.Count);
        Assert.True(found.ContainsKey("alice"));
        Assert.True(found.ContainsKey("carol"));
    }

    [Fact]
    public async Task ContainsOnEmptyParameterizedArrayReturnsNothing()
    {
        string tag = await SeedAsync();
        await using var ctx = new PeopleContext(Options());

        string[] wanted = [];

        List<string> names = await ctx.People
            .Where(p => p.Tag == tag && wanted.Contains(p.Name))
            .Select(p => p.Name)
            .ToListAsync();

        Assert.Empty(names);
    }

    private class PeopleContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Person> People => Set<Person>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Person>(b =>
            {
                b.ToTable("contains_people_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
        }
    }

    private class Person
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
    }
}
