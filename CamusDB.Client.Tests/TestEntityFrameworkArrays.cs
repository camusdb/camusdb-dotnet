/**
 * This file is part of CamusDB
 *
 * End-to-end tests for EF Core mapping of CLR arrays to CamusDB native ARRAY(T) columns.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkArrays
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<ArrCtx> Options() =>
        new DbContextOptionsBuilder<ArrCtx>().UseCamusDB(ConnString).Options;

    [Fact]
    public void TestArrayPropertiesMapToNativeArrayColumns()
    {
        using var ctx = new ArrCtx(Options());
        var et = ctx.Model.FindEntityType(typeof(ArrDoc))!;

        // Direct type mappings must win over EF's primitive-collection (JSON string) convention:
        // arrays land in first-class ARRAY(T) columns, never a JSON/text store type.
        Assert.Equal("array(int64)", et.FindProperty(nameof(ArrDoc.Longs))!.GetColumnType());
        Assert.Equal("array(string)", et.FindProperty(nameof(ArrDoc.Strings))!.GetColumnType());
        Assert.Equal("array(float64)", et.FindProperty(nameof(ArrDoc.Doubles))!.GetColumnType());
        Assert.Equal("array(bool)", et.FindProperty(nameof(ArrDoc.Bools))!.GetColumnType());

        // The array mappings carry a scalar element mapping (so `values.Contains(col)` predicates can
        // translate), which makes EF classify them as primitive collections — but the native array(T)
        // store type above still wins, so nothing is stored as a JSON string.
        Assert.DoesNotContain("json", et.FindProperty(nameof(ArrDoc.Strings))!.GetColumnType()!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TestRoundTripAllElementTypes()
    {
        await using var ctx = new ArrCtx(Options());
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        ctx.Docs.Add(new ArrDoc
        {
            Id = id,
            Longs = [1, 2, 3],
            Strings = ["a", "b"],
            Doubles = [1.5, 2.5],
            Bools = [true, false, true],
        });
        await ctx.SaveChangesAsync();

        await using var read = new ArrCtx(Options());
        var doc = await read.Docs.AsNoTracking().FirstAsync(d => d.Id == id);

        Assert.Equal([1L, 2L, 3L], doc.Longs);
        Assert.Equal(["a", "b"], doc.Strings);
        Assert.Equal([1.5, 2.5], doc.Doubles);
        Assert.Equal([true, false, true], doc.Bools);
    }

    [Fact]
    public async Task TestEmptyArrayRoundTrips()
    {
        await using var ctx = new ArrCtx(Options());
        await ctx.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        ctx.Docs.Add(new ArrDoc { Id = id, Longs = [] });
        await ctx.SaveChangesAsync();

        await using var read = new ArrCtx(Options());
        var doc = await read.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
        Assert.Empty(doc.Longs);
    }

    [Fact]
    public async Task TestArrayMutationIsDetectedAndPersisted()
    {
        await using var seed = new ArrCtx(Options());
        await seed.Database.EnsureCreatedAsync();

        string id = CamusObjectIdGenerator.GenerateAsString();
        seed.Docs.Add(new ArrDoc { Id = id, Longs = [1, 2, 3] });
        await seed.SaveChangesAsync();

        // Load tracked, replace the array, save — the value comparer must flag the change.
        await using (var edit = new ArrCtx(Options()))
        {
            var doc = await edit.Docs.FirstAsync(d => d.Id == id);
            doc.Longs = [9, 8];
            await edit.SaveChangesAsync();
        }

        await using var read = new ArrCtx(Options());
        var reread = await read.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
        Assert.Equal([9L, 8L], reread.Longs);
    }

    private class ArrCtx(DbContextOptions options) : DbContext(options)
    {
        public DbSet<ArrDoc> Docs => Set<ArrDoc>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<ArrDoc>(b =>
            {
                b.ToTable("ef_array_docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Longs).HasColumnName("longs");
                b.Property(e => e.Strings).HasColumnName("strings");
                b.Property(e => e.Doubles).HasColumnName("doubles");
                b.Property(e => e.Bools).HasColumnName("bools");
            });
        }
    }

    private class ArrDoc
    {
        public string Id { get; set; } = "";
        public long[] Longs { get; set; } = [];
        public string[] Strings { get; set; } = [];
        public double[] Doubles { get; set; } = [];
        public bool[] Bools { get; set; } = [];
    }
}
