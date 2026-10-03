/**
 * This file is part of CamusDB
 *
 * End-to-end ExecuteUpdate/ExecuteDelete tests against a running CamusDB server. Covers both shapes the
 * provider writes: the direct UPDATE/DELETE for a single-table filter, and the primary-key IN subquery
 * for navigation filters, joins and Take. TestEntityFrameworkExecuteUpdateSql pins the SQL text.
 *
 * Each test tags its rows with a unique per-run marker and filters on it, so tables reused across
 * runs by EnsureCreated don't cross-contaminate assertions.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkExecuteUpdate
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<BulkContext> Options() =>
        new DbContextOptionsBuilder<BulkContext>().UseCamusDB(ConnString).Options;

    // Seeds alice (orders totals 1, 2, 3) and bob (order total 4), all stamped with a unique run tag.
    private static async Task<string> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using var ctx = new BulkContext(Options());
        await ctx.Database.EnsureCreatedAsync();

        string alice = CamusObjectIdGenerator.GenerateAsString();
        string bob = CamusObjectIdGenerator.GenerateAsString();
        ctx.Customers.Add(new BulkCustomer { Id = alice, Name = "alice", Tag = tag });
        ctx.Customers.Add(new BulkCustomer { Id = bob, Name = "bob", Tag = tag });
        for (int total = 1; total <= 3; total++)
            ctx.Orders.Add(new BulkOrder { Id = CamusObjectIdGenerator.GenerateAsString(), CustomerId = alice, Total = total, Status = "open", Tag = tag });
        ctx.Orders.Add(new BulkOrder { Id = CamusObjectIdGenerator.GenerateAsString(), CustomerId = bob, Total = 4, Status = "open", Tag = tag });
        await ctx.SaveChangesAsync();
        return tag;
    }

    private static async Task<List<BulkOrder>> OrdersAsync(string tag)
    {
        await using var ctx = new BulkContext(Options());
        return await ctx.Orders.AsNoTracking().Where(o => o.Tag == tag).OrderBy(o => o.Total).ToListAsync();
    }

    [Fact]
    public async Task UpdateConstantAndSelfReference()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders.Where(o => o.Tag == tag && o.Total >= 2)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "paid").SetProperty(o => o.Total, o => o.Total * 10));

        Assert.Equal(3, rows);
        var orders = await OrdersAsync(tag);
        Assert.Equal([1, 20, 30, 40], orders.Select(o => o.Total));
        Assert.Equal(["open", "paid", "paid", "paid"], orders.Select(o => o.Status));
    }

    [Fact]
    public async Task UpdateWithNavigationFilter()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders
            .Where(o => o.Tag == tag && ctx.Customers.Any(c => c.Id == o.CustomerId && c.Name == "alice"))
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "alice"));

        Assert.Equal(3, rows);
        var orders = await OrdersAsync(tag);
        Assert.Equal(["alice", "alice", "alice", "open"], orders.Select(o => o.Status));
    }

    [Fact]
    public async Task UpdateWithJoinFilter()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await (from o in ctx.Orders
                          join c in ctx.Customers on o.CustomerId equals c.Id
                          where o.Tag == tag && c.Name == "bob"
                          select o)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Total, o => o.Total + 100));

        Assert.Equal(1, rows);
        var orders = await OrdersAsync(tag);
        Assert.Equal([1, 2, 3, 104], orders.Select(o => o.Total));
    }

    [Fact]
    public async Task UpdateWithOrderByTake()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders.Where(o => o.Tag == tag).OrderByDescending(o => o.Total).Take(2)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "top"));

        Assert.Equal(2, rows);
        var orders = await OrdersAsync(tag);
        Assert.Equal(["open", "open", "top", "top"], orders.Select(o => o.Status));
    }

    [Fact]
    public async Task UpdateWithoutFilter()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        // Rewrites every row to itself, so it is safe on a table that other runs share.
        int rows = await ctx.Customers.ExecuteUpdateAsync(s => s.SetProperty(c => c.Name, c => c.Name));

        Assert.True(rows >= 2);
        Assert.Equal(2, await ctx.Customers.CountAsync(c => c.Tag == tag && (c.Name == "alice" || c.Name == "bob")));
    }

    [Fact]
    public async Task DeleteWithFilter()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders.Where(o => o.Tag == tag && o.Total < 3).ExecuteDeleteAsync();

        Assert.Equal(2, rows);
        Assert.Equal([3, 4], (await OrdersAsync(tag)).Select(o => o.Total));
    }

    [Fact]
    public async Task DeleteWithNavigationFilter()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders
            .Where(o => o.Tag == tag && ctx.Customers.Any(c => c.Id == o.CustomerId && c.Name == "bob"))
            .ExecuteDeleteAsync();

        Assert.Equal(1, rows);
        Assert.Equal([1, 2, 3], (await OrdersAsync(tag)).Select(o => o.Total));
    }

    [Fact]
    public async Task DeleteWithOrderByTake()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        int rows = await ctx.Orders.Where(o => o.Tag == tag).OrderBy(o => o.Total).Take(1).ExecuteDeleteAsync();

        Assert.Equal(1, rows);
        Assert.Equal([2, 3, 4], (await OrdersAsync(tag)).Select(o => o.Total));
    }

    [Fact]
    public async Task UpdateRollsBackWithTransaction()
    {
        string tag = await SeedAsync();
        await using var ctx = new BulkContext(Options());

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            int rows = await ctx.Orders.Where(o => o.Tag == tag).ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "gone"));
            Assert.Equal(4, rows);
            await tx.RollbackAsync();
        }

        Assert.All(await OrdersAsync(tag), o => Assert.Equal("open", o.Status));
    }

    private class BulkContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<BulkCustomer> Customers => Set<BulkCustomer>();
        public DbSet<BulkOrder> Orders => Set<BulkOrder>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<BulkCustomer>(b =>
            {
                b.ToTable("bulk_customers_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
            mb.Entity<BulkOrder>(b =>
            {
                b.ToTable("bulk_orders_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.CustomerId).HasColumnName("customerid").HasColumnType("id");
                b.Property(e => e.Total).HasColumnName("total");
                b.Property(e => e.Status).HasColumnName("status").HasMaxLength(32);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
        }
    }

    private class BulkCustomer
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
    }

    private class BulkOrder
    {
        public string Id { get; set; } = "";
        public string CustomerId { get; set; } = "";
        public long Total { get; set; }
        public string Status { get; set; } = "";
        public string Tag { get; set; } = "";
    }
}
