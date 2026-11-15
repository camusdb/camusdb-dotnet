/**
 * This file is part of CamusDB
 *
 * Offline (no server) companion to TestEntityFrameworkExecuteUpdate: asserts the SQL the provider emits
 * for ExecuteUpdate/ExecuteDelete. ExecuteUpdate has no ToQueryString, so interceptors suppress the
 * connection open and the command, and keep the command text.
 *
 * The provider writes the plain UPDATE/DELETE (no target alias, bare target columns, a mandatory WHERE)
 * in two shapes: the direct form, and the primary-key IN subquery. It writes the alias form
 * (UPDATE t AS a ... FROM ..., DELETE FROM t AS a ...) only when a new value reads another table or the
 * filter needs the IN subquery on a table with no single-column key. These tests pin all three.
 */

using System.Data.Common;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkExecuteUpdateSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static (SqlContext ctx, CaptureInterceptor capture) NewContext()
    {
        CaptureInterceptor capture = new();
        SqlContext ctx = new(new DbContextOptionsBuilder<SqlContext>()
            .UseCamusDB(ConnString)
            .AddInterceptors(capture, new SuppressOpenInterceptor())
            .Options);
        return (ctx, capture);
    }

    [Fact]
    public async Task UpdateWithFilterIsWrittenDirectly()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;
        string tag = "t1";

        await ctx.Orders.Where(o => o.Tag == tag && o.Total > 5)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "paid").SetProperty(o => o.Total, o => o.Total + 10));

        Assert.Equal(
            "UPDATE `xu_orders_v1`\nSET `status` = @p,\n    `total` = `total` + 10\nWHERE `tag` = @tag AND `total` > 5",
            capture.Single());
    }

    [Fact]
    public async Task UpdateWithoutFilterGetsWhereTrue()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Orders.ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "x"));

        Assert.Equal("UPDATE `xu_orders_v1`\nSET `status` = @p\nWHERE TRUE", capture.Single());
    }

    [Fact]
    public async Task DeleteWithFilterIsWrittenDirectly()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;
        string tag = "t1";

        await ctx.Orders.Where(o => o.Tag == tag).ExecuteDeleteAsync();

        Assert.Equal("DELETE FROM `xu_orders_v1`\nWHERE `tag` = @tag", capture.Single());
    }

    [Fact]
    public async Task DeleteWithoutFilterGetsWhereTrue()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Orders.ExecuteDeleteAsync();

        Assert.Equal("DELETE FROM `xu_orders_v1`\nWHERE TRUE", capture.Single());
    }

    [Fact]
    public async Task CorrelatedFilterSelectsKeysInSubquery()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Orders.Where(o => ctx.Customers.Any(c => c.Id == o.CustomerId && c.Name == "alice"))
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Total, o => o.Total * 2));

        string sql = capture.Single();
        Assert.StartsWith("UPDATE `xu_orders_v1`\nSET `total` = `total` * 2\nWHERE `Id` IN (", sql);
        Assert.Contains("SELECT `x`.`Id`\n    FROM `xu_orders_v1` AS `x`", sql);
        Assert.Contains("EXISTS (", sql);
        Assert.DoesNotContain("UPDATE `xu_orders_v1` AS", sql);
    }

    [Fact]
    public async Task TakeSelectsKeysInSubquery()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Orders.OrderBy(o => o.Total).Take(2).ExecuteDeleteAsync();

        string sql = capture.Single();
        Assert.StartsWith("DELETE FROM `xu_orders_v1`\nWHERE `Id` IN (", sql);
        Assert.Contains("LIMIT", sql);
        Assert.DoesNotContain("DELETE FROM `xu_orders_v1` AS", sql);
    }

    [Fact]
    public async Task ValueFromCorrelatedSubqueryUsesTheAliasForm()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Customers.ExecuteUpdateAsync(s => s.SetProperty(c => c.OrderCount, c => ctx.Orders.Count(o => o.CustomerId == c.Id)));

        Assert.Equal(
            "UPDATE `xu_customers_v1` AS `x`\nSET `ordercount` = (\n    SELECT COUNT(*)\n    FROM `xu_orders_v1` AS `x0`\n    WHERE `x0`.`customerid` = `x`.`Id`)\nWHERE TRUE",
            capture.Single());
    }

    [Fact]
    public async Task ValueFromJoinedTableUsesUpdateFrom()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await (from o in ctx.Orders
               join c in ctx.Customers on o.CustomerId equals c.Id
               where c.Name == "alice"
               select new { o, c })
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.o.Status, x => x.c.Name));

        Assert.Equal(
            "UPDATE `xu_orders_v1` AS `x`\nSET `status` = `x0`.`name`\nFROM `xu_customers_v1` AS `x0`\nWHERE `x`.`customerid` = `x0`.`Id` AND `x0`.`name` = 'alice'",
            capture.Single());
    }

    [Fact]
    public async Task ValueFromTakeUsesUpdateFrom()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Orders.OrderBy(o => o.Total).Take(2)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, o => ctx.Customers.Where(c => c.Id == o.CustomerId).Select(c => c.Name).First()));

        // EF Core joins the bounded query as a derived table, and the value reads it.
        string sql = capture.Single();
        Assert.StartsWith("UPDATE `xu_orders_v1` AS `x0`\nSET `status` = (", sql);
        Assert.Contains("WHERE `x2`.`Id` = `x1`.`customerid`", sql);
        Assert.Contains("FROM (\n    SELECT `x`.`Id`, `x`.`customerid`\n    FROM `xu_orders_v1` AS `x`\n    ORDER BY `x`.`total`\n    LIMIT @p\n) AS `x1`", sql);
        Assert.EndsWith("WHERE `x0`.`Id` = `x1`.`Id`", sql);
    }

    [Fact]
    public async Task CompositeKeyWithTakeUsesTheAliasForm()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Lines.OrderBy(l => l.Qty).Take(1).ExecuteDeleteAsync();

        string sql = capture.Single();
        Assert.StartsWith("DELETE FROM `xu_lines_v1` AS `x`\nWHERE EXISTS (", sql);
        Assert.Contains("LIMIT @p", sql);
        Assert.DoesNotContain("USING", sql);
    }

    [Fact]
    public async Task CompositeKeyUpdateWithJoinUsesUpdateFrom()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await (from l in ctx.Lines
               join o in ctx.Orders on l.OrderId equals o.Id
               where o.Tag == "t1"
               select l)
            .ExecuteUpdateAsync(s => s.SetProperty(l => l.Qty, l => l.Qty + 1));

        Assert.Equal(
            "UPDATE `xu_lines_v1` AS `x`\nSET `qty` = `x`.`qty` + 1\nFROM `xu_orders_v1` AS `x0`\nWHERE `x`.`orderid` = `x0`.`Id` AND `x0`.`tag` = 't1'",
            capture.Single());
    }

    [Fact]
    public async Task CompositeKeyUpdateWithTakeUsesUpdateFrom()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Lines.OrderBy(l => l.Qty).Take(3).ExecuteUpdateAsync(s => s.SetProperty(l => l.Qty, 0));

        Assert.Equal(
            "UPDATE `xu_lines_v1` AS `x0`\nSET `qty` = @p1\nFROM (\n    SELECT `x`.`orderid`, `x`.`lineno`\n    FROM `xu_lines_v1` AS `x`\n    ORDER BY `x`.`qty`\n    LIMIT @p\n) AS `x1`\n" +
            "WHERE `x0`.`orderid` = `x1`.`orderid` AND `x0`.`lineno` = `x1`.`lineno`",
            capture.Single());
    }

    [Fact]
    public async Task CompositeKeyDeleteWithCorrelatedFilterUsesTheAliasForm()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Lines.Where(l => ctx.Orders.Any(o => o.Id == l.OrderId && o.Tag == "t1")).ExecuteDeleteAsync();

        Assert.Equal(
            "DELETE FROM `xu_lines_v1` AS `x`\nWHERE EXISTS (\n    SELECT 1\n    FROM `xu_orders_v1` AS `x0`\n    WHERE `x0`.`Id` = `x`.`orderid` AND `x0`.`tag` = 't1')",
            capture.Single());
    }

    [Fact]
    public async Task CompositeKeyWithDirectFilterIsWritten()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        await ctx.Lines.Where(l => l.Qty == 0).ExecuteDeleteAsync();

        Assert.Equal("DELETE FROM `xu_lines_v1`\nWHERE `qty` = 0", capture.Single());
    }

    private sealed class CaptureInterceptor : DbCommandInterceptor
    {
        public List<string> Commands { get; } = new();

        public string Single() => Assert.Single(Commands).Replace("\r\n", "\n");

        public override InterceptionResult<int> NonQueryExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
        {
            Commands.Add(command.CommandText);
            return InterceptionResult<int>.SuppressWithResult(0);
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            Commands.Add(command.CommandText);
            return ValueTask.FromResult(InterceptionResult<int>.SuppressWithResult(0));
        }
    }

    private sealed class SuppressOpenInterceptor : DbConnectionInterceptor
    {
        public override InterceptionResult ConnectionOpening(
            DbConnection connection, ConnectionEventData eventData, InterceptionResult result)
            => InterceptionResult.Suppress();

        public override ValueTask<InterceptionResult> ConnectionOpeningAsync(
            DbConnection connection, ConnectionEventData eventData, InterceptionResult result,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(InterceptionResult.Suppress());
    }

    private class SqlContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<XuCustomer> Customers => Set<XuCustomer>();
        public DbSet<XuOrder> Orders => Set<XuOrder>();
        public DbSet<XuLine> Lines => Set<XuLine>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<XuCustomer>(b =>
            {
                b.ToTable("xu_customers_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.OrderCount).HasColumnName("ordercount");
            });
            mb.Entity<XuOrder>(b =>
            {
                b.ToTable("xu_orders_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.CustomerId).HasColumnName("customerid").HasColumnType("id");
                b.Property(e => e.Total).HasColumnName("total");
                b.Property(e => e.Status).HasColumnName("status").HasMaxLength(32);
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
            mb.Entity<XuLine>(b =>
            {
                b.ToTable("xu_lines_v1");
                b.HasKey(e => new { e.OrderId, e.LineNo });
                b.Property(e => e.OrderId).HasColumnName("orderid");
                b.Property(e => e.LineNo).HasColumnName("lineno");
                b.Property(e => e.Qty).HasColumnName("qty");
            });
        }
    }

    private class XuCustomer
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int OrderCount { get; set; }
    }

    private class XuOrder
    {
        public string Id { get; set; } = "";
        public string CustomerId { get; set; } = "";
        public long Total { get; set; }
        public string Status { get; set; } = "";
        public string Tag { get; set; } = "";
    }

    private class XuLine
    {
        public string OrderId { get; set; } = "";
        public int LineNo { get; set; }
        public int Qty { get; set; }
    }
}
