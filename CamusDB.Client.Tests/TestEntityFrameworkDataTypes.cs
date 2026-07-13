
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

    [Fact]
    public async Task TestSaveAndQueryUuid()
    {
        var options = new DbContextOptionsBuilder<UuidContext>().UseCamusDB(ConnectionString).Options;

        await using var ctx = new UuidContext(options);
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString();
        Guid reference = Guid.NewGuid();

        ctx.Docs.Add(new UuidDoc { Id = id, ExternalRef = reference, Name = "doc" });
        await ctx.SaveChangesAsync();

        // Fresh context + a WHERE on the UUID column to exercise both write and parameterized read.
        await using var readCtx = new UuidContext(options);
        var loaded = await readCtx.Docs.AsNoTracking()
            .FirstOrDefaultAsync(d => d.ExternalRef == reference);

        Assert.NotNull(loaded);
        Assert.Equal(id, loaded!.Id);
        Assert.Equal(reference, loaded.ExternalRef);
        Assert.Equal("doc", loaded.Name);
    }

    [Fact]
    public async Task TestCheckConstraintEnforcedOnSave()
    {
        var options = new DbContextOptionsBuilder<CheckContext>().UseCamusDB(ConnectionString).Options;

        await using var ctx = new CheckContext(options);
        await ctx.Database.EnsureCreatedAsync();

        // A valid row (price > 0) is accepted.
        string okId = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString();
        ctx.Products.Add(new CheckProduct { Id = okId, Name = "ok", Price = 10 });
        await ctx.SaveChangesAsync();

        // A row that violates the check (price <= 0) is rejected with CADB0303.
        await using var badCtx = new CheckContext(options);
        badCtx.Products.Add(new CheckProduct { Id = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString(), Name = "bad", Price = -5 });
        // EF wraps the provider exception in a DbUpdateException; the CADB0303 CamusException is the inner cause.
        var ex = await Assert.ThrowsAsync<DbUpdateException>(() => badCtx.SaveChangesAsync());
        var camusEx = Assert.IsType<CamusException>(ex.InnerException);
        Assert.Equal("CADB0303", camusEx.Code);

        // The valid row is readable.
        await using var readCtx = new CheckContext(options);
        var loaded = await readCtx.Products.AsNoTracking().FirstOrDefaultAsync(p => p.Id == okId);
        Assert.NotNull(loaded);
        Assert.Equal(10, loaded!.Price);
    }

    private class CheckContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<CheckProduct> Products => Set<CheckProduct>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CheckProduct>(b =>
            {
                b.ToTable("ef_check_products", t => t.HasCheckConstraint("ck_ef_check_products_price", "price > 0"));
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnName("name").HasMaxLength(64);
                b.Property(e => e.Price).HasColumnName("price");
            });
        }
    }

    private class CheckProduct
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public long Price { get; set; }
    }

    private class UuidContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<UuidDoc> Docs => Set<UuidDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UuidDoc>(b =>
            {
                b.ToTable("ef_uuid_docs_v2");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.ExternalRef).HasColumnType("uuid");
                b.Property(e => e.Name).HasMaxLength(64);
            });
        }
    }

    private class UuidDoc
    {
        public string Id { get; set; } = "";
        public Guid ExternalRef { get; set; }
        public string Name { get; set; } = "";
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
