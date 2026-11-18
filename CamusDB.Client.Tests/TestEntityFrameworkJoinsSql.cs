/**
 * This file is part of CamusDB
 *
 * Offline (no server) companion to TestEntityFrameworkJoins: asserts the join SQL the provider emits.
 * ToQueryString compiles the query without opening a connection.
 *
 * The relational base keeps a Where on the left operand of a RightJoin in the statement's WHERE, which
 * runs after the padding and so removes the padded rows. The provider moves that operand into a derived
 * table, which also turns a join chain on the left into the one table CamusDB's RIGHT JOIN needs.
 *
 * EF Core's CROSS APPLY / OUTER APPLY are written as CROSS JOIN LATERAL (…) / LEFT JOIN LATERAL (…) ON TRUE,
 * ROW_NUMBER() uses the server's window syntax, and an inline or parameter collection that a query reads as
 * rows is a (VALUES …) AS v (cols) list.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkJoinsSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static JoinContext NewContext() =>
        new(new DbContextOptionsBuilder<JoinContext>().UseCamusDB(ConnString).Options);

    [Fact]
    public void IncludeCollectionIsLeftJoin()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers.Include(c => c.Orders).ToQueryString();

        Assert.Contains("LEFT JOIN `jn_orders` AS `j0` ON `j`.`Id` = `j0`.`CustomerId`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void GroupJoinDefaultIfEmptyIsLeftJoin()
    {
        using JoinContext ctx = NewContext();

        string sql = (from c in ctx.Customers
                      join o in ctx.Orders on c.Id equals o.CustomerId into g
                      from o in g.DefaultIfEmpty()
                      select new { c.Name, Total = (long?)o!.Total }).ToQueryString();

        Assert.Contains("LEFT JOIN `jn_orders` AS `j0` ON `j`.`Id` = `j0`.`CustomerId`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RightJoinOnPlainTableStaysDirect()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers
            .RightJoin(ctx.Orders, c => (long?)c.Id, o => o.CustomerId, (c, o) => new { c!.Name, o.Total })
            .ToQueryString();

        Assert.Equal(
            "SELECT `j`.`Name`, `j0`.`Total`\n" +
            "FROM `jn_customers` AS `j`\n" +
            "RIGHT JOIN `jn_orders` AS `j0` ON `j`.`Id` = `j0`.`CustomerId`",
            sql);
    }

    [Fact]
    public void RightJoinMovesFilteredLeftOperandIntoDerivedTable()
    {
        using JoinContext ctx = NewContext();
        string tag = "t1";

        string sql = ctx.Customers.Where(c => c.Tag == tag)
            .RightJoin(ctx.Orders, c => (long?)c.Id, o => o.CustomerId, (c, o) => new { c!.Name, o.Total })
            .ToQueryString();

        Assert.Equal(
            "-- @tag='t1'\n" +
            "SELECT `j1`.`Name`, `j0`.`Total`\n" +
            "FROM (\n" +
            "    SELECT `j`.`Id`, `j`.`Name`\n" +
            "    FROM `jn_customers` AS `j`\n" +
            "    WHERE `j`.`Tag` = @tag\n" +
            ") AS `j1`\n" +
            "RIGHT JOIN `jn_orders` AS `j0` ON `j1`.`Id` = `j0`.`CustomerId`",
            sql);
    }

    [Fact]
    public void RightJoinMovesJoinedLeftOperandIntoDerivedTable()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers
            .Join(ctx.Regions, c => c.RegionId, r => (long?)r.Id, (c, r) => new { c.Id, Region = r.Name })
            .RightJoin(ctx.Orders, x => (long?)x.Id, o => o.CustomerId, (x, o) => new { x!.Region, o.Total })
            .ToQueryString();

        Assert.DoesNotContain("`jn_regions` AS `j0` ON `j`.`RegionId` = `j0`.`Id`\nRIGHT JOIN", sql, StringComparison.Ordinal);
        Assert.Contains("    INNER JOIN `jn_regions` AS `j0` ON `j`.`RegionId` = `j0`.`Id`\n) AS `s`\nRIGHT JOIN", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void UncorrelatedSelectManyIsCrossJoin()
    {
        using JoinContext ctx = NewContext();

        string sql = (from c in ctx.Customers
                      from r in ctx.Regions
                      select new { c.Name, Region = r.Name }).ToQueryString();

        Assert.Contains("CROSS JOIN `jn_regions` AS `j0`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CrossApplyIsCrossJoinLateral()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers
            .SelectMany(c => ctx.Orders.Where(o => o.Total > c.Id + 1).OrderBy(o => o.Total).Take(2), (c, o) => new { c.Name, o.Total })
            .ToQueryString();

        Assert.Equal(
            "SELECT `j`.`Name`, `j1`.`Total`\n" +
            "FROM `jn_customers` AS `j`\n" +
            "CROSS JOIN LATERAL (\n" +
            "    SELECT `j0`.`Total`\n" +
            "    FROM `jn_orders` AS `j0`\n" +
            "    WHERE `j0`.`Total` > (`j`.`Id` + 1)\n" +
            "    ORDER BY `j0`.`Total`\n" +
            "    LIMIT 2\n" +
            ") AS `j1`",
            sql);
    }

    [Fact]
    public void OuterApplyIsLeftJoinLateralOnTrue()
    {
        using JoinContext ctx = NewContext();

        string sql = (from c in ctx.Customers
                      from o in ctx.Orders.Where(o => o.Total > c.Id + 1).DefaultIfEmpty()
                      select new { c.Name, Total = (long?)o!.Total }).ToQueryString();

        Assert.Equal(
            "SELECT `j`.`Name`, `j1`.`Total`\n" +
            "FROM `jn_customers` AS `j`\n" +
            "LEFT JOIN LATERAL (\n" +
            "    SELECT `j0`.`Total`\n" +
            "    FROM `jn_orders` AS `j0`\n" +
            "    WHERE `j0`.`Total` > (`j`.`Id` + 1)\n" +
            ") AS `j1` ON TRUE",
            sql);
    }

    [Fact]
    public void FilteredIncludeWithTakeIsRowNumber()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers.Include(c => c.Orders.OrderBy(o => o.Total).Take(1)).ToQueryString();

        Assert.Contains(
            "ROW_NUMBER() OVER (PARTITION BY `j0`.`CustomerId` ORDER BY `j0`.`Total`) AS `row`",
            sql, StringComparison.Ordinal);
        Assert.Contains("WHERE `j1`.`row` <= 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RowNumberOverKeylessSequenceHasNoOrderBy()
    {
        using JoinContext ctx = NewContext();

        // A keyless sequence has no order and no key, so EF orders by a constant. The provider leaves that
        // ordering out, rather than writing ORDER BY (SELECT 1).
        string sql = ctx.Customers
            .SelectMany(c => ctx.OrderRows.Where(k => k.CustomerId == c.Id).Take(1), (c, k) => new { c.Name, k.Total })
            .ToQueryString();

        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY `j0`.`CustomerId`) AS `row`", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InlineCollectionIsValuesList()
    {
        using JoinContext ctx = NewContext();

        string sql = ctx.Customers.Where(c => new[] { c.Id, 5L }.Count(v => v > 2) > 1).Select(c => c.Name).ToQueryString();

        Assert.Equal(
            "SELECT `j`.`Name`\n" +
            "FROM `jn_customers` AS `j`\n" +
            "WHERE (\n" +
            "    SELECT COUNT(*)\n" +
            "    FROM (VALUES (`j`.`Id`), (5)) AS `v` (`Value`)\n" +
            "    WHERE `v`.`Value` > 2) > 1",
            sql);
    }

    [Fact]
    public void ValuesItemWithParameterOperatorIsCast()
    {
        using JoinContext ctx = NewContext();
        long b = 1000;

        // The server types `j`.`Id` - @b as a string in a VALUES column, so the item is cast to its type,
        // in every row. A bare parameter needs no cast. EF Core itself casts some items of the first row
        // (the constant 11 below).
        string first = ctx.Customers.Where(c => new[] { c.Id - b, 11L }.Count(v => v > 10) == 2).ToQueryString();
        string second = ctx.Customers.Where(c => new[] { 11L, c.Id - b, b }.Count(v => v > 10) == 2).ToQueryString();

        Assert.Contains("FROM (VALUES (CAST(`j`.`Id` - @b AS int64)), (11)) AS `v` (`Value`)", first, StringComparison.Ordinal);
        Assert.Contains(
            "FROM (VALUES (CAST(11 AS int64)), (CAST(`j`.`Id` - @b AS int64)), (@b)) AS `v` (`Value`)",
            second, StringComparison.Ordinal);
    }

    [Fact]
    public void ParameterCollectionIsValuesListOfParameters()
    {
        using JoinContext ctx = NewContext();
        long[] ids = [6, 15];

        string sql = ctx.Orders.Where(o => ids.Any(t => t > o.Total)).Select(o => o.Id).ToQueryString();

        Assert.Contains("FROM (VALUES (@ids1), (@ids2)) AS `i` (`Value`)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InlineCollectionInSelectManyIsLateralValues()
    {
        using JoinContext ctx = NewContext();

        string sql = (from c in ctx.Customers
                      from v in new[] { c.Id, c.RegionId ?? 0 }.Where(x => x > 1)
                      select new { c.Name, v }).ToQueryString();

        Assert.Equal(
            "SELECT `j`.`Name`, `v0`.`Value` AS `v`\n" +
            "FROM `jn_customers` AS `j`\n" +
            "CROSS JOIN LATERAL (\n" +
            "    SELECT `v`.`Value`\n" +
            "    FROM (VALUES (`j`.`Id`), (COALESCE(`j`.`RegionId`, 0))) AS `v` (`Value`)\n" +
            "    WHERE `v`.`Value` > 1\n" +
            ") AS `v0`",
            sql);
    }

    internal class JoinContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
        public DbSet<JoinOrder> Orders => Set<JoinOrder>();
        public DbSet<JoinRegion> Regions => Set<JoinRegion>();

        // The orders table read as a keyless entity: EF has no key to order a per-row Take by.
        public DbSet<JoinOrderRow> OrderRows => Set<JoinOrderRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<JoinRegion>(b =>
            {
                b.ToTable("jn_regions");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.Name).HasMaxLength(64);
                b.Property(e => e.Tag).HasMaxLength(64);
            });
            mb.Entity<JoinCustomer>(b =>
            {
                b.ToTable("jn_customers");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.Name).HasMaxLength(64);
                b.Property(e => e.Tag).HasMaxLength(64);
                b.HasOne(e => e.Region).WithMany().HasForeignKey(e => e.RegionId);
                b.HasMany(e => e.Orders).WithOne(o => o.Customer).HasForeignKey(o => o.CustomerId);
            });
            mb.Entity<JoinOrder>(b =>
            {
                b.ToTable("jn_orders");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.Tag).HasMaxLength(64);
            });
            mb.Entity<JoinOrderRow>(b =>
            {
                b.HasNoKey();
                b.ToView("jn_orders");
            });
        }
    }

    internal class JoinOrderRow
    {
        public long? CustomerId { get; set; }
        public long Total { get; set; }
        public string Tag { get; set; } = "";
    }

    internal class JoinRegion
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
    }

    internal class JoinCustomer
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public long? RegionId { get; set; }
        public JoinRegion? Region { get; set; }
        public string Tag { get; set; } = "";
        public List<JoinOrder> Orders { get; set; } = [];
    }

    internal class JoinOrder
    {
        public long Id { get; set; }
        public long? CustomerId { get; set; }
        public JoinCustomer? Customer { get; set; }
        public long Total { get; set; }
        public string Tag { get; set; } = "";
    }
}
