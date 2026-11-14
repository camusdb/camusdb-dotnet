/**
 * This file is part of CamusDB
 *
 * Live coverage for intervals over REST (buffered and streamed) and gRPC, and through EF Core and
 * Dapper. The server sends an interval as ISO-8601 text in a string column, and reads a TimeSpan
 * parameter with CAST(@p AS interval). Needs a server on :5095 (REST) and :5096 (gRPC) that has the
 * interval type (server commit 3449de61).
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Dapper;
using CamusDB.EntityFrameworkCore;
using Dapper;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestIntervalLive : BaseTest
{
    private static readonly DateTime Started = new(2024, 1, 31, 10, 0, 0, DateTimeKind.Utc);

    private static readonly DateTime Finished = new(2024, 2, 1, 13, 30, 15, 250, DateTimeKind.Utc);

    private static CamusConnectionStringBuilder BuilderFor(string protocol) => protocol == "grpc"
        ? new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc")
        : new("Endpoint=http://localhost:5095;Database=test");

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(BuilderFor(protocol));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task IntervalResultsReadAsTimeSpanAndCamusInterval(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);

        await using CamusCommand select = connection.CreateCamusCommand(
            "SELECT CAST(@span AS interval) AS span, " +
            "CAST(@finished AS datetime) - CAST(@started AS datetime) AS elapsed, " +
            "age(CAST('2001-04-10' AS date), CAST('1957-06-13' AS date)) AS age, " +
            "CAST(@started AS datetime) + CAST(@span AS interval) AS moved, " +
            "CAST(@interval AS interval) AS months");

        select.Parameters.Add("@span", ColumnType.Null, new TimeSpan(1, 2, 3, 4) + TimeSpan.FromTicks(1_234_567));
        select.Parameters.Add("@started", ColumnType.Null, Started);
        select.Parameters.Add("@finished", ColumnType.Null, Finished);
        select.Parameters.Add("@interval", ColumnType.Null, new CamusInterval(14, -3, 5_000_000));

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(typeof(string), reader.GetFieldType(0));

        // The server rounds the seventh digit of the seconds to a microsecond.
        Assert.Equal(new TimeSpan(1, 2, 3, 4) + TimeSpan.FromTicks(1_234_570), reader.GetFieldValue<TimeSpan>(0));
        Assert.Equal("P1DT2H3M4.123457S", reader.GetString(0));

        Assert.Equal(Finished - Started, reader.GetTimeSpan(1));
        Assert.Equal(new CamusInterval(525, 27, 0), reader.GetFieldValue<CamusInterval>(2));
        Assert.Throws<InvalidCastException>(() => reader.GetTimeSpan(2));
        Assert.Equal(Started + new TimeSpan(1, 2, 3, 4) + TimeSpan.FromTicks(1_234_570), reader.GetDateTime(3));
        Assert.Equal(new CamusInterval(14, -3, 5_000_000), reader.GetInterval(4));
    }

    [Fact]
    public async Task StreamedReaderReadsIntervals()
    {
        await using CamusConnection connection = await OpenAsync("");

        await using CamusCommand select = connection.CreateCamusCommand(
            "SELECT CAST(@a AS interval) * 1.5 AS scaled, -CAST(@a AS interval) AS negated");
        select.Parameters.Add("@a", ColumnType.Null, new TimeSpan(1, 2, 0, 0));

        await using CamusDataReader reader = await select.ExecuteStreamReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(new TimeSpan(1, 15, 0, 0), reader.GetFieldValue<TimeSpan>(0));
        Assert.Equal(-new TimeSpan(1, 2, 0, 0), reader.GetFieldValue<TimeSpan>(1));
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task TimeSpanParameterFiltersRows(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTempTableAsync(connection, "interval_rows",
            " id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, started DATETIME NOT NULL, finished DATETIME NOT NULL");

        foreach ((string name, TimeSpan length) in new[] { ("short", TimeSpan.FromMinutes(5)), ("long", TimeSpan.FromHours(3)) })
        {
            await using CamusCommand insert = connection.CreateCamusCommand(
                $"INSERT INTO {table} (id, name, started, finished) VALUES (@id, @name, @started, @finished)");
            insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("@name", ColumnType.String, name);
            insert.Parameters.Add("@started", ColumnType.DateTime, Started);
            insert.Parameters.Add("@finished", ColumnType.DateTime, Started + length);
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT name, finished - started AS length FROM {table} WHERE finished - started > CAST(@limit AS interval)");
        select.Parameters.Add("@limit", ColumnType.Null, TimeSpan.FromHours(1));

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("long", reader.GetString(0));
        Assert.Equal(TimeSpan.FromHours(3), reader.GetTimeSpan(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task DapperBindsAndReadsIntervals()
    {
        CamusDapper.Register();

        await using CamusConnection connection = await OpenAsync("");

        IntervalRow row = await connection.QuerySingleAsync<IntervalRow>(
            "SELECT CAST(@span AS interval) AS Span, CAST(@age AS interval) AS Age",
            new { span = TimeSpan.FromMinutes(90), age = new CamusInterval(13, 2, 0) });

        Assert.Equal(TimeSpan.FromMinutes(90), row.Span);
        Assert.Equal(new CamusInterval(13, 2, 0), row.Age);
    }

    public sealed class IntervalRow
    {
        public TimeSpan Span { get; set; }

        public CamusInterval Age { get; set; }
    }

    [Fact]
    public async Task EntityFrameworkTranslatesDateAndIntervalArithmetic()
    {
        DbContextOptions<JobContext> options = new DbContextOptionsBuilder<JobContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test").Options;

        await using (JobContext ctx = new(options))
            await ctx.Database.EnsureCreatedAsync();

        string tag = Guid.NewGuid().ToString("n");
        DateTime now = DateTime.UtcNow;

        await using (JobContext ctx = new(options))
        {
            ctx.Jobs.Add(new JobRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Name = "old", Started = Started, Finished = Finished, Day = new DateOnly(2024, 1, 31), Months = 1 });
            ctx.Jobs.Add(new JobRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Name = "recent", Started = now.AddDays(-2), Finished = now.AddDays(-2).AddMinutes(5), Day = new DateOnly(2024, 3, 15), Months = 2 });
            await ctx.SaveChangesAsync();
        }

        await using (JobContext ctx = new(options))
        {
            // DateTime.UtcNow is now(), and AddDays(-7) is one interval literal.
            List<string> lastWeek = await ctx.Jobs.AsNoTracking()
                .Where(j => j.Tag == tag && j.Started > DateTime.UtcNow.AddDays(-7))
                .Select(j => j.Name)
                .ToListAsync();
            Assert.Equal(["recent"], lastWeek);

            // A datetime difference compared with a TimeSpan parameter.
            TimeSpan limit = TimeSpan.FromHours(1);
            List<string> slow = await ctx.Jobs.AsNoTracking()
                .Where(j => j.Tag == tag && j.Finished - j.Started > limit)
                .Select(j => j.Name)
                .ToListAsync();
            Assert.Equal(["old"], slow);

            // The projection of a difference reads back as a TimeSpan.
            TimeSpan elapsed = await ctx.Jobs.AsNoTracking()
                .Where(j => j.Tag == tag && j.Name == "old")
                .Select(j => j.Finished - j.Started)
                .SingleAsync();
            Assert.Equal(Finished - Started, elapsed);

            // A month added to January 31 moves back to the last day of February, as in .NET.
            var moved = await ctx.Jobs.AsNoTracking()
                .Where(j => j.Tag == tag && j.Name == "old")
                .Select(j => new
                {
                    Month = j.Started.AddMonths(j.Months),
                    Hours = j.Started.AddHours(1.5),
                    Plus = j.Started + TimeSpan.FromMinutes(j.Months),
                    DayMonth = j.Day.AddMonths(1),
                    DayDays = j.Day.AddDays(3),
                    Scaled = (j.Finished - j.Started) * 2.0,
                })
                .SingleAsync();

            Assert.Equal(Started.AddMonths(1), moved.Month);
            Assert.Equal(Started.AddHours(1.5), moved.Hours);
            Assert.Equal(Started.AddMinutes(1), moved.Plus);
            Assert.Equal(new DateOnly(2024, 2, 29), moved.DayMonth);
            Assert.Equal(new DateOnly(2024, 2, 3), moved.DayDays);
            Assert.Equal((Finished - Started) * 2.0, moved.Scaled);

            // Skip with no Take writes OFFSET alone; the server refuses LIMIT -1.
            List<string> skipped = await ctx.Jobs.AsNoTracking()
                .Where(j => j.Tag == tag)
                .OrderBy(j => j.Started)
                .Skip(1)
                .Select(j => j.Name)
                .ToListAsync();
            Assert.Equal(["recent"], skipped);
        }
    }

    public class JobRow
    {
        public string Id { get; set; } = "";

        public string Tag { get; set; } = "";

        public string Name { get; set; } = "";

        public DateTime Started { get; set; }

        public DateTime Finished { get; set; }

        public DateOnly Day { get; set; }

        public int Months { get; set; }
    }

    private class JobContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<JobRow> Jobs => Set<JobRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<JobRow>(b =>
            {
                b.ToTable("ef_interval_jobs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Tag).HasMaxLength(32);
            });
        }
    }
}
