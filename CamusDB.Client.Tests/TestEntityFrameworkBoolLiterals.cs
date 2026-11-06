/**
 * This file is part of CamusDB
 *
 * A bool constant must reach the server as TRUE or FALSE. EF's default BoolTypeMapping prints 1 and 0,
 * which are Integer64 to CamusDB: `WHERE 0` is refused with "expected Bool, got: Integer64", and an
 * empty `local.Contains(column)` reduces to exactly that. The Sql tests compile the query without a
 * connection and pin the text; the Live tests prove that the server accepts it.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkBoolLiterals
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static FlagContext NewContext() =>
        new(new DbContextOptionsBuilder<FlagContext>().UseCamusDB(ConnString).Options);

    private static async Task<string> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using FlagContext ctx = NewContext();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Flags.Add(new Flag { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Active = true });
        ctx.Flags.Add(new Flag { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Active = false });

        await ctx.SaveChangesAsync();
        return tag;
    }

    [Fact]
    public void Sql_AlwaysFalseFilterPrintsFalse()
    {
        using FlagContext ctx = NewContext();

        string sql = ctx.Flags.Where(f => false).ToQueryString();

        Assert.Contains("WHERE FALSE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Sql_EmptyContainsPrintsFalse()
    {
        using FlagContext ctx = NewContext();
        string[] wanted = [];

        string sql = ctx.Flags.Where(f => wanted.Contains(f.Tag)).ToQueryString();

        Assert.Contains("WHERE FALSE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Sql_ProjectedConstantPrintsTrue()
    {
        using FlagContext ctx = NewContext();

        string sql = ctx.Flags.Select(f => new { f.Id, Flag = true }).ToQueryString();

        Assert.Contains("TRUE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Live_AlwaysFalseFilterReturnsNothing()
    {
        string tag = await SeedAsync();
        await using FlagContext ctx = NewContext();

        Assert.Empty(await ctx.Flags.Where(f => false).Where(f => f.Tag == tag).ToListAsync());
    }

    [Fact]
    public async Task Live_NegatedEmptyContainsReturnsEveryRow()
    {
        string tag = await SeedAsync();
        await using FlagContext ctx = NewContext();
        string[] wanted = [];

        Assert.Equal(2, await ctx.Flags.Where(f => f.Tag == tag && !wanted.Contains(f.Tag)).CountAsync());
    }

    [Fact]
    public async Task Live_ConditionalOverBoolConstantsFiltersRows()
    {
        string tag = await SeedAsync();
        await using FlagContext ctx = NewContext();

        List<Flag> rows = await ctx.Flags.Where(f => f.Tag == tag && (f.Active ? true : false)).ToListAsync();

        Assert.True(Assert.Single(rows).Active);
    }

    [Fact]
    public async Task Live_ProjectedConstantReadsBackAsBool()
    {
        string tag = await SeedAsync();
        await using FlagContext ctx = NewContext();

        var rows = await ctx.Flags.Where(f => f.Tag == tag).Select(f => new { f.Id, Flag = true }).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.True(row.Flag));
    }

    [Fact]
    public async Task Live_ExecuteUpdateAssignsBoolConstant()
    {
        string tag = await SeedAsync();
        await using FlagContext ctx = NewContext();

        await ctx.Flags.Where(f => f.Tag == tag).ExecuteUpdateAsync(s => s.SetProperty(f => f.Active, true));

        Assert.Equal(2, await ctx.Flags.Where(f => f.Tag == tag && f.Active).CountAsync());
    }

    private class FlagContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Flag> Flags => Set<Flag>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Flag>(b =>
            {
                b.ToTable("bool_literal_flags_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
                b.Property(e => e.Active).HasColumnName("active");
            });
        }
    }

    private class Flag
    {
        public string Id { get; set; } = "";
        public string Tag { get; set; } = "";
        public bool Active { get; set; }
    }
}
