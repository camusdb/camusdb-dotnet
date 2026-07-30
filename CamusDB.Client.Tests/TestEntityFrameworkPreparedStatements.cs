
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data.Common;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CamusDB.Client.Tests;

/// <summary>
/// The reason auto-preparation exists: Entity Framework never calls <c>Prepare</c>, so a driver that
/// only prepares on request would never prepare for the workload that repeats statements the most.
///
/// <para>These tests deliberately use a <b>new <c>DbContext</c> per query</b>, which is how EF is used in
/// practice — a web application resolves a scoped context per request — and which also means a new
/// connection, a new connection-string builder, and a new command each time. That is the case a
/// naively-scoped policy fails: if the decision to prepare lived on any of those objects it would be
/// discarded before the second execution, and nothing would ever be prepared.</para>
/// </summary>
public class TestEntityFrameworkPreparedStatements
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    /// <summary>Captures the SQL EF generated, so the assertions can name the exact statement rather than
    /// counting whatever else the process has prepared.</summary>
    private sealed class SqlCapture : DbCommandInterceptor
    {
        public List<string> Commands { get; } = [];

        public override InterceptionResult<DbDataReader> ReaderExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            lock (Commands)
                Commands.Add(command.CommandText);

            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            lock (Commands)
                Commands.Add(command.CommandText);

            return ValueTask.FromResult(result);
        }
    }

    /// <summary>
    /// Creates the table, tolerating the lock contention that concurrent test classes creating their own
    /// schema produce. The server reports those as retryable rather than fatal, and the suite's other
    /// schema helpers retry them the same way.
    /// </summary>
    private static async Task EnsureCreatedAsync(RobotContext context)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await context.Database.EnsureCreatedAsync();
                return;
            }
            catch (CamusException ex) when (attempt < 4 && (
                ex.Message.Contains("MustRetry", StringComparison.Ordinal) ||
                ex.Message.Contains("AlreadyLocked", StringComparison.Ordinal) ||
                ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase)))
            {
                await Task.Delay(100 * (attempt + 1));
            }
        }
    }

    private static DbContextOptions<RobotContext> Options(SqlCapture? capture = null)
    {
        DbContextOptionsBuilder<RobotContext> builder = new();
        builder.UseCamusDB(ConnString);

        if (capture is not null)
            builder.AddInterceptors(capture);

        return builder.Options;
    }

    [Fact]
    public async Task TestRepeatedQueryIsPreparedAcrossContexts()
    {
        string tag = Guid.NewGuid().ToString("n");

        await using (RobotContext seed = new(Options()))
        {
            await EnsureCreatedAsync(seed);

            seed.Robots.Add(new Robot { Id = CamusObjectIdGenerator.GenerateAsString(), Name = "optimus", Year = 1984, Tag = tag });
            seed.Robots.Add(new Robot { Id = CamusObjectIdGenerator.GenerateAsString(), Name = "wall-e", Year = 2008, Tag = tag });

            await seed.SaveChangesAsync();
        }

        SqlCapture capture = new();

        // A context per query, as a request-scoped DbContext would be.
        for (int i = 0; i < 4; i++)
        {
            await using RobotContext ctx = new(Options(capture));

            List<string> names = await ctx.Robots
                .Where(r => r.Tag == tag && r.Year > 1900)
                .OrderBy(r => r.Name)
                .Select(r => r.Name)
                .ToListAsync();

            Assert.Equal(["optimus", "wall-e"], names);
        }

        string sql = Assert.Single(capture.Commands.Distinct());

        CamusConnectionStringBuilder builder = new(ConnString);

        Assert.True(
            builder.IsPrepared(sql),
            $"EF's repeated query should have been prepared automatically, but was not:\n{sql}");
    }

    /// <summary>
    /// <c>SaveChanges</c> is the other half of the case: an application inserts through the same generated
    /// statement over and over, and that statement should end up prepared too.
    /// </summary>
    [Fact]
    public async Task TestRepeatedSaveChangesIsPrepared()
    {
        string tag = Guid.NewGuid().ToString("n");

        await using (RobotContext init = new(Options()))
            await EnsureCreatedAsync(init);

        for (int i = 0; i < 4; i++)
        {
            await using RobotContext ctx = new(Options());

            ctx.Robots.Add(new Robot
            {
                Id = CamusObjectIdGenerator.GenerateAsString(),
                Name = "robot" + i,
                Year = 2000 + i,
                Tag = tag,
            });

            await ctx.SaveChangesAsync();
        }

        await using RobotContext verify = new(Options());
        Assert.Equal(4, await verify.Robots.CountAsync(r => r.Tag == tag));

        // EF's INSERT for one entity type is a single statement shape, so after four saves the driver is
        // holding at least one registration for this deployment.
        CamusConnectionStringBuilder builder = new(ConnString);
        Assert.True(builder.PreparedStatementCount > 0);
    }

    private class RobotContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Robot> Robots => Set<Robot>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Robot>(b =>
            {
                b.ToTable("prepared_robots_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.Year).HasColumnName("year");
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
        }
    }

    private class Robot
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public long Year { get; set; }

        public string Tag { get; set; } = "";
    }
}
