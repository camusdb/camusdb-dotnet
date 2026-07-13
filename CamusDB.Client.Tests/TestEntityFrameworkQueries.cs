/**
 * This file is part of CamusDB
 *
 * End-to-end query-translation tests against a running CamusDB server. Covers the LINQ shapes the
 * provider translates: aggregates, GroupBy, inner joins, correlated subqueries, and the string
 * functions mapped onto CamusDB's native scalar/predicate functions.
 *
 * Each test tags its rows with a unique per-run marker and filters on it, so tables reused across
 * runs by EnsureCreated don't cross-contaminate assertions.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkQueries
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<ShopContext> Options() =>
        new DbContextOptionsBuilder<ShopContext>().UseCamusDB(ConnString).Options;

    // Seeds two customers (alice/NYC, bob/LA) + three orders, all stamped with a unique run tag.
    private async Task<(string tag, string aliceId, string bobId)> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        await using var ctx = new ShopContext(Options());
        await ctx.Database.EnsureCreatedAsync();

        string alice = CamusObjectIdGenerator.GenerateAsString();
        string bob = CamusObjectIdGenerator.GenerateAsString();
        ctx.Customers.Add(new Customer { Id = alice, Name = "alice", City = "NYC", Tag = tag });
        ctx.Customers.Add(new Customer { Id = bob, Name = "bob", City = "LA", Tag = tag });
        ctx.Orders.Add(new Order { Id = CamusObjectIdGenerator.GenerateAsString(), CustomerId = alice, Total = 10, Status = "paid", Tag = tag });
        ctx.Orders.Add(new Order { Id = CamusObjectIdGenerator.GenerateAsString(), CustomerId = alice, Total = 20, Status = "paid", Tag = tag });
        ctx.Orders.Add(new Order { Id = CamusObjectIdGenerator.GenerateAsString(), CustomerId = bob, Total = 5, Status = "open", Tag = tag });
        await ctx.SaveChangesAsync();
        return (tag, alice, bob);
    }

    [Fact]
    public async Task TestAggregates()
    {
        var (tag, _, _) = await SeedAsync();
        await using var ctx = new ShopContext(Options());

        Assert.Equal(3, await ctx.Orders.Where(o => o.Tag == tag).CountAsync());
        Assert.Equal(35, await ctx.Orders.Where(o => o.Tag == tag).SumAsync(o => o.Total));
        Assert.Equal(20, await ctx.Orders.Where(o => o.Tag == tag).MaxAsync(o => o.Total));
        Assert.True(await ctx.Orders.Where(o => o.Tag == tag).AnyAsync(o => o.Total > 15));
    }

    [Fact]
    public async Task TestGroupByAggregate()
    {
        var (tag, _, _) = await SeedAsync();
        await using var ctx = new ShopContext(Options());

        var byStatus = await ctx.Orders.Where(o => o.Tag == tag)
            .GroupBy(o => o.Status)
            .Select(g => new { g.Key, N = g.Count(), T = g.Sum(o => o.Total) })
            .ToListAsync();

        Assert.Equal(2, byStatus.Count);
        Assert.Equal(2, byStatus.Single(x => x.Key == "paid").N);
        Assert.Equal(30, byStatus.Single(x => x.Key == "paid").T);
        Assert.Equal(5, byStatus.Single(x => x.Key == "open").T);
    }

    [Fact]
    public async Task TestInnerJoin()
    {
        var (tag, _, _) = await SeedAsync();
        await using var ctx = new ShopContext(Options());

        var rows = await (from o in ctx.Orders.Where(o => o.Tag == tag)
                          join c in ctx.Customers on o.CustomerId equals c.Id
                          select new { o.Total, c.Name }).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal(30, rows.Where(r => r.Name == "alice").Sum(r => r.Total));
    }

    [Fact]
    public async Task TestCorrelatedSubquery()
    {
        var (tag, aliceId, _) = await SeedAsync();
        await using var ctx = new ShopContext(Options());

        // Customers (in this run) that have an order over 15 → only alice.
        var names = await ctx.Customers
            .Where(c => c.Tag == tag && ctx.Orders.Any(o => o.CustomerId == c.Id && o.Total > 15))
            .Select(c => c.Name)
            .ToListAsync();

        Assert.Equal(["alice"], names);
    }

    [Fact]
    public async Task TestStringFunctions()
    {
        var (tag, _, _) = await SeedAsync();
        await using var ctx = new ShopContext(Options());

        Assert.Equal(["alice"], await Names(ctx, tag, c => c.Name.StartsWith("ali")));
        Assert.Equal(["alice"], await Names(ctx, tag, c => c.Name.EndsWith("ice")));
        Assert.Equal(["alice"], await Names(ctx, tag, c => c.Name.Contains("lic")));
        Assert.Equal(["alice"], await Names(ctx, tag, c => c.Name.StartsWith("ALI", StringComparison.OrdinalIgnoreCase)));
        Assert.Equal(["alice"], await Names(ctx, tag, c => c.Name.ToUpper() == "ALICE"));
        Assert.Equal(["bob"], await Names(ctx, tag, c => c.Name.Length == 3));

        static async Task<List<string>> Names(ShopContext ctx, string tag, System.Linq.Expressions.Expression<Func<Customer, bool>> pred)
            => await ctx.Customers.Where(c => c.Tag == tag).Where(pred).OrderBy(c => c.Name).Select(c => c.Name).ToListAsync();
    }

    private class ShopContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(b =>
            {
                b.ToTable("shop_customers_v2");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.City).HasColumnName("city").HasMaxLength(64);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
                b.HasMany(e => e.Orders).WithOne().HasForeignKey(o => o.CustomerId);
            });
            mb.Entity<Order>(b =>
            {
                b.ToTable("shop_orders_v2");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.CustomerId).HasColumnName("customerid").HasColumnType("id");
                b.Property(e => e.Total).HasColumnName("total");
                b.Property(e => e.Status).HasColumnName("status").HasMaxLength(32);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
        }
    }

    private class Customer
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string City { get; set; } = "";
        public string Tag { get; set; } = "";
        public List<Order> Orders { get; set; } = [];
    }

    private class Order
    {
        public string Id { get; set; } = "";
        public string CustomerId { get; set; } = "";
        public long Total { get; set; }
        public string Status { get; set; } = "";
        public string Tag { get; set; } = "";
    }
}
