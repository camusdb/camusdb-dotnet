/**
 * This file is part of CamusDB
 *
 * Offline (no server) companion to TestEntityFrameworkStringCompare: asserts the SQL the provider
 * *emits* for `string.Compare(...) <op> 0`. ToQueryString compiles the query without opening a
 * connection, so these pin the rewrite — the compare-then-test-against-zero pair collapsing into a
 * single relational comparison, with no CASE (a construct CamusDB's dialect does not have).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkStringCompareSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static SqlContext NewContext() =>
        new(new DbContextOptionsBuilder<SqlContext>().UseCamusDB(ConnString).Options);

    [Fact]
    public void CompareOrdinalBecomesDirectComparison()
    {
        using SqlContext ctx = NewContext();
        string cursor = "ccc";

        string sql = ctx.Events.Where(e => string.Compare(e.Code, cursor, StringComparison.Ordinal) < 0).ToQueryString();

        Assert.DoesNotContain("CASE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("`code` < @cursor", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CompareToBecomesDirectComparison()
    {
        using SqlContext ctx = NewContext();
        string cursor = "bbb";

        string sql = ctx.Events.Where(e => e.Code.CompareTo(cursor) > 0).ToQueryString();

        Assert.DoesNotContain("CASE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(">", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ZeroOnTheLeftFlipsTheOperator()
    {
        using SqlContext ctx = NewContext();
        string cursor = "bbb";

        // 0 < Compare(code, cursor) must emit `code > cursor`, not `code < cursor`.
        string flipped = ctx.Events.Where(e => 0 < string.Compare(e.Code, cursor, StringComparison.Ordinal)).ToQueryString();
        string direct = ctx.Events.Where(e => string.Compare(e.Code, cursor, StringComparison.Ordinal) > 0).ToQueryString();

        Assert.Equal(direct, flipped);
    }

    [Fact]
    public void OrdinalIgnoreCaseLowersBothOperands()
    {
        using SqlContext ctx = NewContext();
        string cursor = "CCC";

        string sql = ctx.Events.Where(e => string.Compare(e.Code, cursor, StringComparison.OrdinalIgnoreCase) < 0).ToQueryString();

        Assert.Contains("lower(", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void KeysetPaginationShapeTranslates()
    {
        using SqlContext ctx = NewContext();
        long cursorSeq = 2;
        string cursorCode = "ccc";

        // The reported failure: a tie-breaking cursor predicate. Only assertion that matters is that
        // ToQueryString does not throw InvalidOperationException("could not be translated").
        string sql = ctx.Events
            .Where(e => e.Seq < cursorSeq ||
                        (e.Seq == cursorSeq && string.Compare(e.Code, cursorCode, StringComparison.Ordinal) < 0))
            .ToQueryString();

        Assert.DoesNotContain("CASE", sql, StringComparison.OrdinalIgnoreCase);
    }

    private class SqlContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<SqlEvent> Events => Set<SqlEvent>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<SqlEvent>(b =>
            {
                b.ToTable("strcompare_events_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Code).HasColumnName("code").HasMaxLength(64);
                b.Property(e => e.Seq).HasColumnName("seq");
            });
        }
    }

    private class SqlEvent
    {
        public string Id { get; set; } = "";
        public string Code { get; set; } = "";
        public long Seq { get; set; }
    }
}
