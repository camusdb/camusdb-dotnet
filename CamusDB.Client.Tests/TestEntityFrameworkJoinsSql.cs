/**
 * This file is part of CamusDB
 *
 * Offline (no server) companion to TestEntityFrameworkJoins: asserts the join SQL the provider emits.
 * ToQueryString compiles the query without opening a connection.
 *
 * The relational base keeps a Where on the left operand of a RightJoin in the statement's WHERE, which
 * runs after the padding and so removes the padded rows. The provider moves that operand into a derived
 * table, which also turns a join chain on the left into the one table CamusDB's RIGHT JOIN needs. The
 * shapes CamusDB cannot run (APPLY, ROW_NUMBER) fail at translation with a message that names the cause.
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
    public void LateralJoinIsRefusedAtTranslation()
    {
        using JoinContext ctx = NewContext();

        var query = from c in ctx.Customers
                    from o in ctx.Orders.Where(o => o.Total > c.Id + 1).DefaultIfEmpty()
                    select new { c.Name, Total = (long?)o!.Total };

        InvalidOperationException e = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("lateral join", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RowNumberIsRefusedAtTranslation()
    {
        using JoinContext ctx = NewContext();

        var query = ctx.Customers.Include(c => c.Orders.OrderBy(o => o.Total).Take(1));

        InvalidOperationException e = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("ROW_NUMBER()", e.Message, StringComparison.Ordinal);
    }

    internal class JoinContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
        public DbSet<JoinOrder> Orders => Set<JoinOrder>();
        public DbSet<JoinRegion> Regions => Set<JoinRegion>();

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
        }
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
