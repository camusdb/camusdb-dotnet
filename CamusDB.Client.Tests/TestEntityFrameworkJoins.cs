/**
 * This file is part of CamusDB
 *
 * End-to-end join tests against a running CamusDB server: the LINQ shapes that EF Core translates to
 * LEFT, RIGHT and CROSS JOIN, and the padded rows (NULL right columns) they read back. The model is
 * TestEntityFrameworkJoinsSql.JoinContext; TestEntityFrameworkJoinsSql pins the SQL.
 *
 * Each test seeds its own rows with a unique run tag and random keys, and filters on the tag.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using static CamusDB.Client.Tests.TestEntityFrameworkJoinsSql;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkJoins
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static JoinContext NewContext() =>
        new(new DbContextOptionsBuilder<JoinContext>().UseCamusDB(ConnString).Options);

    // One region (north). alice (north) has orders 10 and 20, bob (no region) has order 5, carol (north)
    // has no order, and order 7 has no customer.
    private static async Task<(string tag, long baseId)> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        long b = Random.Shared.NextInt64(1, long.MaxValue / 2);

        await using JoinContext ctx = NewContext();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Regions.Add(new JoinRegion { Id = b + 1, Name = "north", Tag = tag });
        ctx.Customers.Add(new JoinCustomer { Id = b + 10, Name = "alice", RegionId = b + 1, Tag = tag });
        ctx.Customers.Add(new JoinCustomer { Id = b + 11, Name = "bob", RegionId = null, Tag = tag });
        ctx.Customers.Add(new JoinCustomer { Id = b + 12, Name = "carol", RegionId = b + 1, Tag = tag });
        ctx.Orders.Add(new JoinOrder { Id = b + 100, CustomerId = b + 10, Total = 10, Tag = tag });
        ctx.Orders.Add(new JoinOrder { Id = b + 101, CustomerId = b + 10, Total = 20, Tag = tag });
        ctx.Orders.Add(new JoinOrder { Id = b + 102, CustomerId = b + 11, Total = 5, Tag = tag });
        ctx.Orders.Add(new JoinOrder { Id = b + 103, CustomerId = null, Total = 7, Tag = tag });
        await ctx.SaveChangesAsync();

        return (tag, b);
    }

    [Fact]
    public async Task TestIncludeCollection()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        List<JoinCustomer> customers = await ctx.Customers.Where(c => c.Tag == tag)
            .Include(c => c.Orders)
            .OrderBy(c => c.Name)
            .ToListAsync();

        Assert.Equal(["alice", "bob", "carol"], customers.Select(c => c.Name));
        Assert.Equal([10L, 20L], customers[0].Orders.Select(o => o.Total).Order());
        Assert.Equal([5L], customers[1].Orders.Select(o => o.Total));
        Assert.Empty(customers[2].Orders);
    }

    [Fact]
    public async Task TestIncludeOptionalReferenceAndCollection()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        List<JoinCustomer> customers = await ctx.Customers.Where(c => c.Tag == tag)
            .Include(c => c.Region)
            .Include(c => c.Orders)
            .ToListAsync();

        Assert.Equal(3, customers.Count);
        Assert.Null(customers.Single(c => c.Name == "bob").Region);
        Assert.Equal("north", customers.Single(c => c.Name == "carol").Region!.Name);
        Assert.Equal(2, customers.Single(c => c.Name == "alice").Orders.Count);
    }

    [Fact]
    public async Task TestOptionalNavigationProjection()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await ctx.Orders.Where(o => o.Tag == tag)
            .Select(o => new { o.Total, Name = o.Customer!.Name })
            .ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Null(rows.Single(r => r.Total == 7).Name);
        Assert.Equal("bob", rows.Single(r => r.Total == 5).Name);
    }

    [Fact]
    public async Task TestGroupJoinDefaultIfEmpty()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await (from c in ctx.Customers.Where(c => c.Tag == tag)
                          join o in ctx.Orders on c.Id equals o.CustomerId into g
                          from o in g.DefaultIfEmpty()
                          select new { c.Name, Total = (long?)o!.Total }).ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Equal([(long?)null], rows.Where(r => r.Name == "carol").Select(r => r.Total));
    }

    [Fact]
    public async Task TestLeftJoinOperator()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await ctx.Customers.Where(c => c.Tag == tag)
            .LeftJoin(ctx.Orders, c => (long?)c.Id, o => o.CustomerId, (c, o) => new { c.Name, Total = (long?)o!.Total })
            .ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Equal(30, rows.Where(r => r.Name == "alice").Sum(r => r.Total));
        Assert.Null(rows.Single(r => r.Name == "carol").Total);
    }

    [Fact]
    public async Task TestLeftJoinWhereRightIsNull()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        List<string> names = await (from c in ctx.Customers.Where(c => c.Tag == tag)
                                    join o in ctx.Orders on c.Id equals o.CustomerId into g
                                    from o in g.DefaultIfEmpty()
                                    where o == null
                                    select c.Name).ToListAsync();

        Assert.Equal(["carol"], names);
    }

    [Fact]
    public async Task TestRightJoinKeepsFilterOnLeftOperand()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        // The Where on customers must run before the join: order 7 (no customer) is padded, not removed.
        var rows = await ctx.Customers.Where(c => c.Tag == tag)
            .RightJoin(ctx.Orders.Where(o => o.Tag == tag), c => (long?)c.Id, o => o.CustomerId,
                (c, o) => new { Name = c!.Name, o.Total })
            .ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Null(rows.Single(r => r.Total == 7).Name);
        Assert.Equal("bob", rows.Single(r => r.Total == 5).Name);
    }

    [Fact]
    public async Task TestRightJoinAfterJoin()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await ctx.Customers.Where(c => c.Tag == tag)
            .Join(ctx.Regions, c => c.RegionId, r => (long?)r.Id, (c, r) => new { c.Id, Region = r.Name })
            .RightJoin(ctx.Orders.Where(o => o.Tag == tag), x => (long?)x.Id, o => o.CustomerId,
                (x, o) => new { Region = x!.Region, o.Total })
            .ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Null(rows.Single(r => r.Total == 5).Region); // bob has no region
        Assert.Null(rows.Single(r => r.Total == 7).Region); // no customer
        Assert.Equal(["north", "north"], rows.Where(r => r.Total >= 10).Select(r => r.Region));
    }

    [Fact]
    public async Task TestCrossJoin()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await (from c in ctx.Customers.Where(c => c.Tag == tag)
                          from r in ctx.Regions.Where(r => r.Tag == tag)
                          select new { c.Name, Region = r.Name }).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.Equal("north", r.Region));
    }

    [Fact]
    public async Task TestInnerThenLeftJoinChain()
    {
        var (tag, _) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        var rows = await (from o in ctx.Orders.Where(o => o.Tag == tag)
                          join c in ctx.Customers on o.CustomerId equals c.Id
                          join r in ctx.Regions on c.RegionId equals r.Id into rg
                          from r in rg.DefaultIfEmpty()
                          select new { o.Total, c.Name, Region = r!.Name }).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Null(rows.Single(r => r.Name == "bob").Region);
        Assert.All(rows.Where(r => r.Name == "alice"), r => Assert.Equal("north", r.Region));
    }

    [Fact]
    public async Task TestExecuteDeleteThroughOptionalNavigation()
    {
        var (tag, b) = await SeedAsync();
        await using JoinContext ctx = NewContext();

        // Orders of customers with no region: the filter needs a LEFT JOIN, so the provider selects the
        // keys in an IN subquery.
        int deleted = await ctx.Orders.Where(o => o.Tag == tag && o.Customer != null && o.Customer.Region == null)
            .ExecuteDeleteAsync();

        Assert.Equal(1, deleted);
        Assert.Equal([b + 100, b + 101, b + 103],
            await ctx.Orders.Where(o => o.Tag == tag).Select(o => o.Id).OrderBy(id => id).ToListAsync());
    }
}
