/**
 * This file is part of CamusDB
 *
 * Offline (no server) coverage for date and interval arithmetic in EF Core: the SQL the provider emits
 * for DateTime.AddDays and the other Add methods, a datetime difference, TimeSpan operators and
 * parameters, DateTime.UtcNow, DateOnly arithmetic, the model check that refuses an interval column,
 * and an OFFSET with no LIMIT. ToQueryString compiles the query without opening a connection.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkIntervalSql
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static SqlContext NewContext() =>
        new(new DbContextOptionsBuilder<SqlContext>().UseCamusDB(ConnString).Options);

    [Fact]
    public void AddDaysWithAConstantIsOneIntervalLiteral()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.Events.Where(e => e.Created > DateTime.UtcNow.AddDays(-7)).ToQueryString();

        Assert.Contains("`e`.`created` > (now() + INTERVAL 'P-7D')", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("years", "INTERVAL 'P2Y'")]
    [InlineData("months", "INTERVAL 'P2M'")]
    [InlineData("hours", "INTERVAL 'PT2H'")]
    [InlineData("minutes", "INTERVAL 'PT2M'")]
    [InlineData("seconds", "INTERVAL 'PT2S'")]
    [InlineData("milliseconds", "INTERVAL 'PT0.002S'")]
    [InlineData("microseconds", "INTERVAL 'PT0.000002S'")]
    public void EachAddMethodAddsItsUnit(string unit, string literal)
    {
        using SqlContext ctx = NewContext();

        IQueryable<DateTime> query = unit switch
        {
            "years" => ctx.Events.Select(e => e.Created.AddYears(2)),
            "months" => ctx.Events.Select(e => e.Created.AddMonths(2)),
            "hours" => ctx.Events.Select(e => e.Created.AddHours(2)),
            "minutes" => ctx.Events.Select(e => e.Created.AddMinutes(2)),
            "seconds" => ctx.Events.Select(e => e.Created.AddSeconds(2)),
            "milliseconds" => ctx.Events.Select(e => e.Created.AddMilliseconds(2)),
            _ => ctx.Events.Select(e => e.Created.AddMicroseconds(2)),
        };

        Assert.Contains($"`e`.`created` + {literal}", query.ToQueryString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AFractionOfADayGoesToTheServer()
    {
        using SqlContext ctx = NewContext();

        // The server moves the fraction of a day into the time, as PostgreSQL does.
        string sql = ctx.Events.Select(e => e.Created.AddDays(1.5)).ToQueryString();

        Assert.Contains("`e`.`created` + (1.5 * INTERVAL 'P1D')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AColumnAmountMultipliesTheUnit()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.Events.Select(e => e.Created.AddMonths(e.Months)).ToQueryString();

        Assert.Contains("`e`.`created` + (`e`.`months` * INTERVAL 'P1M')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AParameterAmountMultipliesTheUnit()
    {
        using SqlContext ctx = NewContext();
        double days = 3;

        string sql = ctx.Events.Select(e => e.Created.AddDays(days)).ToQueryString();

        Assert.Contains("`e`.`created` + (@days * INTERVAL 'P1D')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DateTimeDifferenceIsAnInterval()
    {
        using SqlContext ctx = NewContext();
        TimeSpan limit = TimeSpan.FromHours(2);

        string sql = ctx.Events.Where(e => e.Finished - e.Created > limit).ToQueryString();

        Assert.Contains("(`e`.`finished` - `e`.`created`) > CAST(@limit AS interval)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SubtractMethodIsAnInterval()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.Events.Select(e => e.Finished.Subtract(e.Created)).ToQueryString();

        Assert.Contains("SELECT `e`.`finished` - `e`.`created`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DateTimeMinusATimeSpanParameterIsADateTime()
    {
        using SqlContext ctx = NewContext();
        TimeSpan window = TimeSpan.FromDays(7);

        string sql = ctx.Events.Where(e => e.Created > DateTime.UtcNow - window).ToQueryString();

        Assert.Contains("`e`.`created` > (now() - CAST(@window AS interval))", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AddAndSubtractATimeSpan()
    {
        using SqlContext ctx = NewContext();
        TimeSpan span = TimeSpan.FromMinutes(5);

        string add = ctx.Events.Select(e => e.Created.Add(span)).ToQueryString();
        string subtract = ctx.Events.Select(e => e.Created.Subtract(span)).ToQueryString();

        Assert.Contains("`e`.`created` + CAST(@span AS interval)", add, StringComparison.Ordinal);
        Assert.Contains("`e`.`created` - CAST(@span AS interval)", subtract, StringComparison.Ordinal);
    }

    [Fact]
    public void TimeSpanOperators()
    {
        using SqlContext ctx = NewContext();

        string scaled = ctx.Events.Select(e => (e.Finished - e.Created) * 2.0).ToQueryString();
        string divided = ctx.Events.Select(e => (e.Finished - e.Created) / 4.0).ToQueryString();
        string sum = ctx.Events.Select(e => (e.Finished - e.Created) + TimeSpan.FromHours(e.Months)).ToQueryString();
        string negated = ctx.Events.Select(e => (e.Finished - e.Created).Negate()).ToQueryString();

        Assert.Contains("(`e`.`finished` - `e`.`created`) * 2.0", scaled, StringComparison.Ordinal);
        Assert.Contains("(`e`.`finished` - `e`.`created`) / 4.0", divided, StringComparison.Ordinal);
        Assert.Contains("(`e`.`finished` - `e`.`created`) + (`e`.`months` * INTERVAL 'PT1H')", sum, StringComparison.Ordinal);
        Assert.Contains("-(`e`.`finished` - `e`.`created`)", negated, StringComparison.Ordinal);
    }

    [Fact]
    public void TimeSpanDividedByATimeSpanDoesNotTranslate()
    {
        using SqlContext ctx = NewContext();

        Assert.Throws<InvalidOperationException>(() =>
            ctx.Events.Where(e => (e.Finished - e.Created) / TimeSpan.FromHours(1) > 2).ToQueryString());
    }

    [Fact]
    public void UtcNowIsNow()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.Events.Where(e => e.Created < DateTime.UtcNow).ToQueryString();
        string offset = ctx.Events.Select(e => DateTimeOffset.UtcNow).ToQueryString();

        Assert.Contains("`e`.`created` < now()", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT now()", offset, StringComparison.Ordinal);
    }

    [Fact]
    public void DateOnlyArithmetic()
    {
        using SqlContext ctx = NewContext();

        string days = ctx.Events.Select(e => e.Day.AddDays(3)).ToQueryString();
        string months = ctx.Events.Select(e => e.Day.AddMonths(1)).ToQueryString();
        string years = ctx.Events.Select(e => e.Day.AddYears(e.Months)).ToQueryString();

        Assert.Contains("`e`.`day` + 3", days, StringComparison.Ordinal);
        Assert.Contains("CAST(`e`.`day` + INTERVAL 'P1M' AS date)", months, StringComparison.Ordinal);
        Assert.Contains("CAST(`e`.`day` + (`e`.`months` * INTERVAL 'P1Y') AS date)", years, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimeSpanConstantIsALiteral()
    {
        using SqlContext ctx = NewContext();

        string sql = ctx.Events.Where(e => e.Finished - e.Created > EF.Constant(TimeSpan.FromMinutes(90))).ToQueryString();

        Assert.Contains("> INTERVAL 'PT1H30M'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimeSpanPropertyOnATableIsRefused()
    {
        using DurationContext ctx = new(new DbContextOptionsBuilder<DurationContext>().UseCamusDB(ConnString).Options);

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => ctx.Model);

        Assert.Contains("'DurationRow.Length'", ex.Message);
        Assert.Contains("HasConversion<long>()", ex.Message);
    }

    [Fact]
    public void ATimeSpanPropertyWithAConverterIsAccepted()
    {
        using DurationContext ctx = new(
            new DbContextOptionsBuilder<DurationContext>().UseCamusDB(ConnString).Options,
            b => b.Property(e => e.Length).HasConversion<long>());

        string sql = ctx.Rows.Where(r => r.Length > TimeSpan.FromHours(1)).ToQueryString();

        // The converter stores ticks, so the constant is a number of ticks.
        Assert.Contains("`d`.`length` > 36000000000", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("interval", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimeSpanPropertyOnAViewIsAccepted()
    {
        using DurationContext ctx = new(
            new DbContextOptionsBuilder<DurationContext>().UseCamusDB(ConnString).Options,
            b => b.ToTable((string?)null).ToView("durations"));

        string sql = ctx.Rows.Select(r => r.Length).ToQueryString();

        Assert.Contains("SELECT `d`.`length`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SkipWithNoTakeWritesOffsetAlone()
    {
        using SqlContext ctx = NewContext();
        int skip = 5;

        string sql = ctx.Events.OrderBy(e => e.Id).Skip(skip).ToQueryString();

        Assert.Contains("OFFSET @p", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);

        string both = ctx.Events.OrderBy(e => e.Id).Skip(skip).Take(2).ToQueryString();
        Assert.Contains("LIMIT @p1 OFFSET @p", both, StringComparison.Ordinal);
    }

    public class EventRow
    {
        public long Id { get; set; }

        public DateTime Created { get; set; }

        public DateTime Finished { get; set; }

        public DateOnly Day { get; set; }

        public int Months { get; set; }
    }

    private sealed class SqlContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<EventRow> Events => Set<EventRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EventRow>(b =>
            {
                b.ToTable("events");
                b.HasKey(e => e.Id);
                b.Property(e => e.Created).HasColumnName("created");
                b.Property(e => e.Finished).HasColumnName("finished");
                b.Property(e => e.Day).HasColumnName("day");
                b.Property(e => e.Months).HasColumnName("months");
            });
        }
    }

    public class DurationRow
    {
        public long Id { get; set; }

        public TimeSpan Length { get; set; }
    }

    private sealed class DurationContext(
        DbContextOptions options,
        Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<DurationRow>>? configure = null) : DbContext(options)
    {
        public DbSet<DurationRow> Rows => Set<DurationRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<DurationRow>(b =>
            {
                b.ToTable("durations");
                b.HasKey(e => e.Id);
                b.Property(e => e.Length).HasColumnName("length");
                configure?.Invoke(b);
            });
        }

        // Each configuration is a different model, so EF must not share one cached model between them.
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.ReplaceService<Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory, PerInstanceModelCacheKeyFactory>();
    }

    private sealed class PerInstanceModelCacheKeyFactory : Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) => (context, designTime);
    }
}
