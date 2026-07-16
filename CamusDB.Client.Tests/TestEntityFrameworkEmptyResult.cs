/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

/// <summary>
/// Server-backed regression coverage for the empty-result schema gap: querying an empty table under
/// <c>EnableRetryOnFailure</c> routes through EF Core's <c>BufferedDataReader</c>, which reads the full
/// column schema before any row. The server's <c>columns</c> block + positional rows let the reader
/// report that schema for zero rows, so the query returns an empty list instead of throwing
/// "The underlying reader doesn't have as many fields as expected. Expected: N, actual: 0".
/// </summary>
public class TestEntityFrameworkEmptyResult : BaseTest
{
    private class Widget
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public long Quantity { get; set; }
    }

    private class WidgetContext(DbContextOptions options, string table) : DbContext(options)
    {
        private readonly string tableName = table;
        public DbSet<Widget> Widgets => Set<Widget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Widget>(b =>
            {
                b.ToTable(tableName);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnType("string");
                b.Property(e => e.Quantity).HasColumnType("int64");
            });
        }
    }

    private static WidgetContext NewContext(string table, bool retry)
    {
        DbContextOptionsBuilder<WidgetContext> builder = new();
        builder.UseCamusDB("Endpoint=http://localhost:5095;Database=test", camus =>
        {
            if (retry)
                camus.EnableRetryOnFailure();
        });
        return new WidgetContext(builder.Options, table);
    }

    private static string UniqueTable() => "ef_empty_" + CamusObjectIdGenerator.Generate();

    [Fact]
    public async Task EmptyTableQueryWithRetryReturnsEmpty()
    {
        string table = UniqueTable();
        using WidgetContext ctx = NewContext(table, retry: true);
        await ctx.Database.EnsureCreatedAsync();

        List<Widget> rows = await ctx.Widgets.ToListAsync();

        Assert.Empty(rows);
    }

    [Fact]
    public async Task EmptyTableQueryWithoutRetryReturnsEmpty()
    {
        string table = UniqueTable();
        using WidgetContext ctx = NewContext(table, retry: false);
        await ctx.Database.EnsureCreatedAsync();

        List<Widget> rows = await ctx.Widgets.ToListAsync();

        Assert.Empty(rows);
    }

    [Fact]
    public async Task PopulatedTableQueryWithRetryRoundTripsRows()
    {
        string table = UniqueTable();
        using WidgetContext ctx = NewContext(table, retry: true);
        await ctx.Database.EnsureCreatedAsync();

        ctx.Widgets.Add(new Widget { Id = CamusObjectIdGenerator.Generate().ToString(), Name = "bolt", Quantity = 7 });
        ctx.Widgets.Add(new Widget { Id = CamusObjectIdGenerator.Generate().ToString(), Name = "nut", Quantity = 3 });
        await ctx.SaveChangesAsync();

        List<Widget> rows = await ctx.Widgets.OrderBy(w => w.Name).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("bolt", rows[0].Name);
        Assert.Equal(7, rows[0].Quantity);
        Assert.Equal("nut", rows[1].Name);
        Assert.Equal(3, rows[1].Quantity);
    }
}
