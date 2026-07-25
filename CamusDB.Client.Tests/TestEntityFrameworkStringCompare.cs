/**
 * This file is part of CamusDB
 *
 * Regression coverage for `string.Compare(a, b) <op> 0` / `a.CompareTo(b) <op> 0` translation — the
 * shape keyset (cursor) pagination is written with. It previously failed to translate ("Translation of
 * method 'string.Compare' failed"), because the usual provider trick of emitting a three-valued
 * CASE WHEN scalar is unavailable: CamusDB's SQL dialect has no CASE. CamusSqlTranslatingExpressionVisitor
 * instead collapses the compare-then-test-against-zero pair into a direct comparison.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkStringCompare
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<EventContext> Options() =>
        new DbContextOptionsBuilder<EventContext>().UseCamusDB(ConnString).Options;

    /// <summary>Seeds four rows whose codes sort a &lt; b &lt; c &lt; d, all sharing one tag.</summary>
    private static async Task<string> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using EventContext ctx = new(Options());
        await ctx.Database.EnsureCreatedAsync();

        foreach (string code in new[] { "aaa", "bbb", "ccc", "ddd" })
            ctx.Events.Add(new Event { Id = CamusObjectIdGenerator.GenerateAsString(), Code = code, Tag = tag });

        await ctx.SaveChangesAsync();
        return tag;
    }

    [Fact]
    public async Task CompareOrdinalLessThanZeroFiltersRows()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        string cursor = "ccc";

        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag && string.Compare(e.Code, cursor, StringComparison.Ordinal) < 0)
            .Select(e => e.Code)
            .ToListAsync();

        Assert.Equal(["aaa", "bbb"], codes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task CompareOrdinalGreaterOrEqualZeroFiltersRows()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        string cursor = "ccc";

        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag && string.Compare(e.Code, cursor, StringComparison.Ordinal) >= 0)
            .Select(e => e.Code)
            .ToListAsync();

        Assert.Equal(["ccc", "ddd"], codes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task CompareToTranslates()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        string cursor = "bbb";

        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag && e.Code.CompareTo(cursor) > 0)
            .Select(e => e.Code)
            .ToListAsync();

        Assert.Equal(["ccc", "ddd"], codes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ZeroOnTheLeftFlipsTheOperator()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        string cursor = "bbb";

        // 0 < Compare(code, cursor)  ==  code > cursor
        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag && 0 < string.Compare(e.Code, cursor, StringComparison.Ordinal))
            .Select(e => e.Code)
            .ToListAsync();

        Assert.Equal(["ccc", "ddd"], codes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task OrdinalIgnoreCaseLowersBothOperands()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        string cursor = "CCC";

        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag && string.Compare(e.Code, cursor, StringComparison.OrdinalIgnoreCase) < 0)
            .Select(e => e.Code)
            .ToListAsync();

        Assert.Equal(["aaa", "bbb"], codes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task KeysetPaginationShapeTranslates()
    {
        string tag = await SeedAsync();
        await using EventContext ctx = new(Options());

        // The reported failure: a tie-breaking cursor predicate combining a scalar column with a
        // string comparison on the id/code column.
        long cursorSeq = 2;
        string cursorCode = "ccc";

        List<string> codes = await ctx.Events
            .Where(e => e.Tag == tag &&
                        (e.Seq < cursorSeq ||
                         (e.Seq == cursorSeq && string.Compare(e.Code, cursorCode, StringComparison.Ordinal) < 0)))
            .Select(e => e.Code)
            .ToListAsync();

        Assert.NotNull(codes);   // the assertion under test is that this translates and executes at all
    }

    private class EventContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Event> Events => Set<Event>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Event>(b =>
            {
                b.ToTable("strcompare_events_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Code).HasColumnName("code").HasMaxLength(64);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
                b.Property(e => e.Seq).HasColumnName("seq");
            });
        }
    }

    private class Event
    {
        public string Id { get; set; } = "";
        public string Code { get; set; } = "";
        public string Tag { get; set; } = "";
        public long Seq { get; set; }
    }
}
