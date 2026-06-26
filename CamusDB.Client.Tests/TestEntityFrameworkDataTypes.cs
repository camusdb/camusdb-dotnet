
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkDataTypes
{
    private const string ConnectionString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<EventContext> BuildOptions()
        => new DbContextOptionsBuilder<EventContext>().UseCamusDB(ConnectionString).Options;

    [Fact]
    public async Task TestSaveAndQueryNewTypes()
    {
        await using var ctx = new EventContext(BuildOptions());
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString();
        var happened = new DateTime(2026, 5, 1, 8, 0, 0, DateTimeKind.Utc);
        var day = new DateOnly(2026, 5, 1);
        byte[] payload = [1, 2, 3, 4, 5];

        ctx.Events.Add(new Event
        {
            Id = id,
            Name = "launch",
            Payload = payload,
            Score = 9.5f,
            Happened = happened,
            Day = day,
        });
        await ctx.SaveChangesAsync();

        // Fresh context to force a real round-trip read (no identity-map cache hit).
        await using var readCtx = new EventContext(BuildOptions());
        var loaded = await readCtx.Events.AsNoTracking().FirstOrDefaultAsync(e => e.Id == id);

        Assert.NotNull(loaded);
        Assert.Equal("launch", loaded!.Name);
        Assert.Equal(payload, loaded.Payload);
        Assert.Equal(9.5f, loaded.Score);
        Assert.Equal(happened, loaded.Happened);
        Assert.Equal(day, loaded.Day);
    }

    private class EventContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Event> Events => Set<Event>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>(b =>
            {
                b.ToTable("ef_events");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasMaxLength(64);
                b.Property(e => e.Payload);
                b.Property(e => e.Score);
                b.Property(e => e.Happened);
                b.Property(e => e.Day);
            });
        }
    }

    private class Event
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public byte[] Payload { get; set; } = [];
        public float Score { get; set; }
        public DateTime Happened { get; set; }
        public DateOnly Day { get; set; }
    }
}
