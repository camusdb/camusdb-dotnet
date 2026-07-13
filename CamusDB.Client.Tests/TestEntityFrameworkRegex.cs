/**
 * This file is part of CamusDB
 *
 * End-to-end tests for the EF regex translations against a running CamusDB server. Covers the
 * BCL Regex static methods (Regex.IsMatch → regexp_like, Regex.Replace → regexp_replace 'g') and
 * the EF.Functions.Regexp* helpers (regexp_like/replace/count/substr/instr).
 *
 * Each test tags its rows with a unique per-run marker and filters on it, so tables reused across
 * runs by EnsureCreated don't cross-contaminate assertions.
 */

using System.Text.RegularExpressions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkRegex
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<RegexContext> Options() =>
        new DbContextOptionsBuilder<RegexContext>().UseCamusDB(ConnString).Options;

    // Seeds four people with distinct name/email shapes, all stamped with a unique run tag.
    private async Task<string> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using var ctx = new RegexContext(Options());
        await ctx.Database.EnsureCreatedAsync();

        void Add(string name, string email) => ctx.People.Add(new Person
        {
            Id = CamusObjectIdGenerator.GenerateAsString(), Name = name, Email = email, Tag = tag,
        });

        Add("Alice", "alice@example.com");
        Add("bob", "BOB@example.org");
        Add("Carol123", "carol@test.net");
        Add("dave", "not-an-email");
        await ctx.SaveChangesAsync();
        return tag;
    }

    private static async Task<List<string>> Names(RegexContext ctx, string tag,
        System.Linq.Expressions.Expression<Func<Person, bool>> pred)
        => await ctx.People.Where(p => p.Tag == tag).Where(pred).OrderBy(p => p.Name).Select(p => p.Name).ToListAsync();

    [Fact]
    public async Task TestRegexIsMatch()
    {
        var tag = await SeedAsync();
        await using var ctx = new RegexContext(Options());

        // Anchored, case-sensitive: only names starting with an uppercase letter.
        Assert.Equal(["Alice", "Carol123"], await Names(ctx, tag, p => Regex.IsMatch(p.Name, "^[A-Z]")));
        // Names containing digits.
        Assert.Equal(["Carol123"], await Names(ctx, tag, p => Regex.IsMatch(p.Name, "[0-9]+")));
    }

    [Fact]
    public async Task TestRegexIsMatchIgnoreCase()
    {
        var tag = await SeedAsync();
        await using var ctx = new RegexContext(Options());

        // Case-insensitive: matches both "bob" and "BOB@..." isn't in Name; only "bob" here.
        Assert.Equal(["bob"], await Names(ctx, tag, p => Regex.IsMatch(p.Name, "^BOB$", RegexOptions.IgnoreCase)));
    }

    [Fact]
    public async Task TestRegexReplaceReplacesAllMatches()
    {
        var tag = await SeedAsync();
        await using var ctx = new RegexContext(Options());

        // Regex.Replace maps to regexp_replace with the 'g' flag → every digit stripped, not just the first.
        var scrubbed = await ctx.People.Where(p => p.Tag == tag && p.Name == "Carol123")
            .Select(p => Regex.Replace(p.Name, "[0-9]", "#"))
            .SingleAsync();

        Assert.Equal("Carol###", scrubbed);
    }

    [Fact]
    public async Task TestEfFunctionsRegexpLike()
    {
        var tag = await SeedAsync();
        await using var ctx = new RegexContext(Options());

        // A plausible-email predicate against the email column, case-insensitive.
        var withEmail = await ctx.People
            .Where(p => p.Tag == tag && EF.Functions.RegexpLike(p.Email, "^[^@]+@[^@]+\\.[a-z]+$", "i"))
            .OrderBy(p => p.Name).Select(p => p.Name).ToListAsync();

        Assert.Equal(["Alice", "Carol123", "bob"], withEmail);
    }

    [Fact]
    public async Task TestEfFunctionsRegexpReplaceCountSubstrInstr()
    {
        var tag = await SeedAsync();
        await using var ctx = new RegexContext(Options());

        var row = await ctx.People.Where(p => p.Tag == tag && p.Name == "Carol123")
            .Select(p => new
            {
                Replaced = EF.Functions.RegexpReplace(p.Name, "[0-9]+", "_"),          // first run of digits → "_"
                Digits = EF.Functions.RegexpCount(p.Name, "[0-9]"),                    // 3 digits
                FirstDigit = EF.Functions.RegexpSubstr(p.Name, "[0-9]+"),              // "123"
                Position = EF.Functions.RegexpInstr(p.Name, "[0-9]"),                  // 1-based position of first digit → 6
            })
            .SingleAsync();

        Assert.Equal("Carol_", row.Replaced);
        Assert.Equal(3, row.Digits);
        Assert.Equal("123", row.FirstDigit);
        Assert.Equal(6, row.Position);
    }

    private class RegexContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Person> People => Set<Person>();

        protected override void OnModelCreating(ModelBuilder mb) => mb.Entity<Person>(b =>
        {
            b.ToTable("regex_people");
            b.HasKey(e => e.Id);
            b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
            b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
            b.Property(e => e.Email).HasColumnName("email").HasMaxLength(128);
            b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
        });
    }

    private class Person
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Email { get; set; } = "";
        public string Tag { get; set; } = "";
    }
}
