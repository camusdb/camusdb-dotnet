/**
 * This file is part of CamusDB
 *
 * Offline (no server) companion to TestEntityFrameworkExecuteUpdate: asserts the SQL the provider emits
 * for ExecuteUpdate/ExecuteDelete. ExecuteUpdate has no ToQueryString, so interceptors suppress the
 * connection open and the command, and keep the command text.
 *
 * CamusDB's UPDATE/DELETE take no alias on the target and need a WHERE, and the server cannot evaluate
 * a subquery correlated with the target row there. These pin the two shapes the provider writes: the
 * direct form (bare target columns) and the primary-key IN subquery for everything else.
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
    public async Task ValueFromAnotherTableIsRejected()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ctx.Customers.ExecuteUpdateAsync(s => s.SetProperty(c => c.OrderCount, c => ctx.Orders.Count(o => o.CustomerId == c.Id))));

        Assert.Contains("reads another table", ex.Message);
        Assert.Empty(capture.Commands);
    }

    [Fact]
    public async Task CompositeKeyWithTakeIsRejected()
    {
        var (ctx, capture) = NewContext();
        await using var _ = ctx;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ctx.Lines.OrderBy(l => l.Qty).Take(1).ExecuteDeleteAsync());

        Assert.Contains("single-column primary key", ex.Message);
        Assert.Empty(capture.Commands);
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
