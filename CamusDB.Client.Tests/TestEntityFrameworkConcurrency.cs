/**
 * This file is part of CamusDB
 *
 * EF Core optimistic-concurrency tests: [Timestamp]/IsRowVersion() (a provider-managed byte[] token)
 * and [ConcurrencyCheck]. A stale write matches zero rows on the server and must surface as
 * DbUpdateConcurrencyException; a non-conflicting update must succeed and bump the token.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkConcurrency
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    // ── [Timestamp] / IsRowVersion() ─────────────────────────────────────────

    private static DbContextOptions<RvCtx> RvOptions() =>
        new DbContextOptionsBuilder<RvCtx>().UseCamusDB(ConnString).Options;

    [Fact]
    public void TestRowVersionMapsToBytesConcurrencyToken()
    {
        using var ctx = new RvCtx(RvOptions());
        var p = ctx.Model.FindEntityType(typeof(RvDoc))!.FindProperty(nameof(RvDoc.Version))!;
        Assert.Equal("bytes", p.GetColumnType());
        Assert.True(p.IsConcurrencyToken);
        Assert.Equal(ValueGenerated.OnAddOrUpdate, p.ValueGenerated);
    }

    [Fact]
    public async Task TestRowVersionIsGeneratedAndBumpedOnUpdate()
    {
        await using var ctx = new RvCtx(RvOptions());
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        ctx.Docs.Add(new RvDoc { Id = id, Body = "v1" });
        await ctx.SaveChangesAsync();

        byte[] afterInsert = (await new RvCtx(RvOptions()).Docs.AsNoTracking().FirstAsync(x => x.Id == id)).Version;
        Assert.NotEmpty(afterInsert);

        await using (var edit = new RvCtx(RvOptions()))
        {
            var d = await edit.Docs.FirstAsync(x => x.Id == id);
            d.Body = "v2";
            await edit.SaveChangesAsync();
        }

        var reread = await new RvCtx(RvOptions()).Docs.AsNoTracking().FirstAsync(x => x.Id == id);
        Assert.Equal("v2", reread.Body);
        Assert.False(afterInsert.SequenceEqual(reread.Version)); // token bumped
    }

    [Fact]
    public async Task TestRowVersionStaleUpdateThrows()
    {
        await using var seed = new RvCtx(RvOptions());
        await seed.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        seed.Docs.Add(new RvDoc { Id = id, Body = "v1" });
        await seed.SaveChangesAsync();

        await using var a = new RvCtx(RvOptions());
        await using var b = new RvCtx(RvOptions());
        var da = await a.Docs.FirstAsync(x => x.Id == id);
        var db = await b.Docs.FirstAsync(x => x.Id == id);

        da.Body = "fromA";
        await a.SaveChangesAsync();

        db.Body = "fromB";
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => b.SaveChangesAsync());

        // The first writer's value stands; the stale write was rejected by the server.
        var final = await new RvCtx(RvOptions()).Docs.AsNoTracking().FirstAsync(x => x.Id == id);
        Assert.Equal("fromA", final.Body);
    }

    // ── [ConcurrencyCheck] on a numeric version ──────────────────────────────

    private static DbContextOptions<CcCtx> CcOptions() =>
        new DbContextOptionsBuilder<CcCtx>().UseCamusDB(ConnString).Options;

    [Fact]
    public async Task TestConcurrencyCheckStaleUpdateThrows()
    {
        await using var seed = new CcCtx(CcOptions());
        await seed.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        seed.Docs.Add(new CcDoc { Id = id, Body = "v1", Version = 1 });
        await seed.SaveChangesAsync();

        await using var a = new CcCtx(CcOptions());
        await using var b = new CcCtx(CcOptions());
        var da = await a.Docs.FirstAsync(x => x.Id == id);
        var db = await b.Docs.FirstAsync(x => x.Id == id);

        da.Body = "A"; da.Version++;
        await a.SaveChangesAsync();

        db.Body = "B"; db.Version++;
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => b.SaveChangesAsync());

        var final = await new CcCtx(CcOptions()).Docs.AsNoTracking().FirstAsync(x => x.Id == id);
        Assert.Equal("A", final.Body);
    }

    [Fact]
    public async Task TestUpdateWithoutConcurrencyTokenStaysLenient()
    {
        // A plain entity (no concurrency token) must keep the lenient behavior: a normal update works.
        await using var ctx = new CcCtx(CcOptions());
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        ctx.Plain.Add(new PlainDoc { Id = id, Body = "v1" });
        await ctx.SaveChangesAsync();

        await using (var edit = new CcCtx(CcOptions()))
        {
            var d = await edit.Plain.FirstAsync(x => x.Id == id);
            d.Body = "v2";
            await edit.SaveChangesAsync();
        }

        var reread = await new CcCtx(CcOptions()).Plain.AsNoTracking().FirstAsync(x => x.Id == id);
        Assert.Equal("v2", reread.Body);
    }

    private class RvCtx(DbContextOptions options) : DbContext(options)
    {
        public DbSet<RvDoc> Docs => Set<RvDoc>();
        protected override void OnModelCreating(ModelBuilder mb) => mb.Entity<RvDoc>(b =>
        {
            b.ToTable("ef_rowversion_docs");
            b.HasKey(e => e.Id);
            b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
            b.Property(e => e.Body).HasColumnName("body").HasMaxLength(64);
            b.Property(e => e.Version).HasColumnName("version").IsRowVersion();
        });
    }

    private class RvDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public byte[] Version { get; set; } = [];
    }

    private class CcCtx(DbContextOptions options) : DbContext(options)
    {
        public DbSet<CcDoc> Docs => Set<CcDoc>();
        public DbSet<PlainDoc> Plain => Set<PlainDoc>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<CcDoc>(b =>
            {
                b.ToTable("ef_cc_docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Body).HasColumnName("body").HasMaxLength(64);
                b.Property(e => e.Version).HasColumnName("version").IsConcurrencyToken();
            });
            mb.Entity<PlainDoc>(b =>
            {
                b.ToTable("ef_plain_docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Body).HasColumnName("body").HasMaxLength(64);
            });
        }
    }

    private class CcDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public long Version { get; set; }
    }

    private class PlainDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
    }
}
