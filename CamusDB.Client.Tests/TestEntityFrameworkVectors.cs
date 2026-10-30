/**
 * This file is part of CamusDB
 *
 * Offline (no server) coverage for IsVector: the model it configures, the DDL, the float[] converter
 * and comparer, the model validation, and the SQL of entity queries when a vector is not loaded with
 * its entity. ToQueryString compiles the query without opening a connection.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkVectors
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static VectorContext NewContext() =>
        new(new DbContextOptionsBuilder<VectorContext>().UseCamusDB(ConnString).Options);

    // ── Model ─────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void IsVectorConfiguresSizeStorageAndDimensions()
    {
        using VectorContext ctx = NewContext();
        IProperty embedding = ctx.Model.FindEntityType(typeof(RawChunk))!.FindProperty(nameof(RawChunk.Embedding))!;

        Assert.Equal(3072, embedding.GetMaxLength());
        Assert.Equal(CamusColumnStorage.Plain, embedding.GetStorage());
        Assert.Equal(768, embedding.GetVectorDimensions());
        Assert.True(embedding.IsLoadedWithEntity());
        Assert.Null(embedding.GetValueConverter());
    }

    [Fact]
    public void IsVectorOnAFloatArrayInstallsTheConverterAndComparer()
    {
        using VectorContext ctx = NewContext();
        IProperty embedding = ctx.Model.FindEntityType(typeof(Chunk))!.FindProperty(nameof(Chunk.Embedding))!;

        Assert.IsType<CamusVectorValueConverter>(embedding.GetValueConverter());
        Assert.IsType<CamusVectorValueComparer>(embedding.GetValueComparer());
        // One scalar value, not an ARRAY(FLOAT32) primitive collection.
        Assert.False(embedding.IsPrimitiveCollection);
        Assert.Equal(16, embedding.GetMaxLength());
        Assert.False(embedding.IsLoadedWithEntity());
    }

    [Fact]
    public void IsVectorAddsTheDimensionCheck()
    {
        using VectorContext ctx = NewContext();
        IEntityType raw = ctx.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(RawChunk))!;

        ICheckConstraint check = Assert.Single(raw.GetCheckConstraints());
        Assert.Equal("CK_vec_raw_embedding_dims", check.Name);
        Assert.Equal("vector_dims(`embedding`) = 768", check.Sql);
    }

    [Fact]
    public void TheCheckUsesTheFinalColumnName()
    {
        using RenamedContext ctx = new(new DbContextOptionsBuilder<RenamedContext>().UseCamusDB(ConnString).Options);
        IEntityType raw = ctx.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(RawChunk))!;

        ICheckConstraint check = Assert.Single(raw.GetCheckConstraints());
        Assert.Equal("CK_renamed_vec_emb_dims", check.Name);
        Assert.Equal("vector_dims(`emb`) = 768", check.Sql);
    }

    [Fact]
    public void CreateTableDeclaresTheVectorColumn()
    {
        using VectorContext ctx = NewContext();

        string sql = Assert.Single(CreateTablesSql(ctx), s => s.Contains("`vec_raw`", StringComparison.Ordinal));

        Assert.Contains("`embedding` BYTES(3072) NOT NULL STORAGE PLAIN", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT `CK_vec_raw_embedding_dims` CHECK (vector_dims(`embedding`) = 768)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ALaterHasStorageReplacesPlain()
    {
        using VectorContext ctx = NewContext();
        IProperty summary = ctx.Model.FindEntityType(typeof(Chunk))!.FindProperty(nameof(Chunk.Summary))!;

        Assert.Equal(CamusColumnStorage.External, summary.GetStorage());
    }

    [Fact]
    public void IsVectorRefusesAPropertyThatIsNotBytesOrFloats()
    {
        ModelBuilder builder = new();

        Assert.Throws<InvalidOperationException>(() => builder.Entity<Chunk>().Property(c => c.Content).IsVector(4));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MaxValue / 4 + 1)]
    public void IsVectorRefusesADimensionOutOfRange(int dimensions)
    {
        ModelBuilder builder = new();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Entity<RawChunk>().Property(c => c.Embedding).IsVector(dimensions));
    }

    [Fact]
    public void AnOptionalVectorThatIsNotLoadedIsRefused()
    {
        using OptionalUnloadedContext ctx = new(new DbContextOptionsBuilder<OptionalUnloadedContext>().UseCamusDB(ConnString).Options);

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => ctx.Model);
        Assert.Contains("'Chunk.Summary'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("must be required", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnnotationsReachTheModelSnapshotAsAnnotations()
    {
        // IsVector is not rendered in a snapshot: it would set the storage again, over a later HasStorage.
        // The size, the storage and the CHECK are in the snapshot on their own.
        using VectorContext ctx = NewContext();
        IProperty embedding = ctx.GetService<IDesignTimeModel>().Model
            .FindEntityType(typeof(RawChunk))!.FindProperty(nameof(RawChunk.Embedding))!;

        Assert.Equal(768, embedding.FindAnnotation(CamusAnnotationNames.VectorDimensions)?.Value);
    }

    [Fact]
    public void ADroppedVectorDropsItsCheck()
    {
        using VectorContext ctx = NewContext();
        IModel model = ctx.GetService<IDesignTimeModel>().Model;

        using PlainContext plain = new(new DbContextOptionsBuilder<PlainContext>().UseCamusDB(ConnString).Options);
        IReadOnlyList<MigrationOperation> operations = ctx.GetService<IMigrationsModelDiffer>()
            .GetDifferences(model.GetRelationalModel(), plain.GetService<IDesignTimeModel>().Model.GetRelationalModel());

        DropCheckConstraintOperation drop = Assert.Single(operations.OfType<DropCheckConstraintOperation>());
        Assert.Equal("CK_vec_raw_embedding_dims", drop.Name);
    }

    // ── float[] conversion ────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheConverterUsesTheCamusVectorLayout()
    {
        float[] vector = [1.0f, -2.5f, 0.0f];

        byte[] bytes = CamusVectorValueConverter.ToProvider(vector);

        Assert.Equal(CamusVector.ToBytes(vector), bytes);
        Assert.Equal(vector, CamusVectorValueConverter.FromProvider(bytes));
    }

    [Fact]
    public void TheConverterRefusesAByteCountThatIsNotAVector()
        => Assert.Throws<CamusException>(() => CamusVectorValueConverter.FromProvider([1, 2, 3]));

    [Fact]
    public void TheComparerSeesAChangeMadeInPlace()
    {
        float[] vector = [1, 2, 3];
        float[] snapshot = CamusVectorValueComparer.Instance.Snapshot(vector);

        vector[1] = 5;

        Assert.False(CamusVectorValueComparer.Instance.Equals(vector, snapshot));
        Assert.True(CamusVectorValueComparer.Instance.Equals(new float[] { 1, 2 }, new float[] { 1, 2 }));
        Assert.True(CamusVectorValueComparer.Instance.Equals(null, null));
        Assert.False(CamusVectorValueComparer.Instance.Equals(new float[] { 1 }, null));
        Assert.False(CamusVectorValueComparer.Instance.Equals(new float[] { 1 }, new float[] { 1, 0 }));
    }

    // ── Entity queries and a vector that is not loaded ────────────────────────────────────────────

    [Fact]
    public void AnEntityQueryReadsNullInPlaceOfTheVector()
    {
        using VectorContext ctx = NewContext();

        string sql = ctx.Chunks.ToQueryString();

        Assert.Contains("NULL AS `embedding`", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("`v`.`embedding`", sql, StringComparison.Ordinal);
        // A vector that is loaded with its entity stays.
        Assert.Contains("`v`.`summary`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AKnnQueryStillRanksOnTheColumn()
    {
        using VectorContext ctx = NewContext();
        float[] q = [1, 0, 0, 0];

        string sql = ctx.Chunks
            .Where(c => c.Tag == "t")
            .OrderBy(c => EF.Functions.CosineDistance(c.Embedding, q))
            .Take(2)
            .ToQueryString();

        Assert.Contains("NULL AS `embedding`", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY cosine_distance(`v`.`embedding`, @q)", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT @p", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AFilterOnTheVectorStillReadsTheColumn()
    {
        using VectorContext ctx = NewContext();

        string sql = ctx.Chunks.Where(c => EF.Functions.VectorDims(c.Embedding) == 4).ToQueryString();

        Assert.Contains("NULL AS `embedding`", sql, StringComparison.Ordinal);
        Assert.Contains("vector_dims(`v`.`embedding`) = 4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AProjectionThatNamesTheVectorReadsIt()
    {
        using VectorContext ctx = NewContext();

        string sql = ctx.Chunks.Select(c => new { c.Id, c.Embedding }).ToQueryString();

        Assert.Contains("`v`.`embedding`", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEntityAndItsVectorInOneProjectionReadTheVector()
    {
        using VectorContext ctx = NewContext();

        // The entity and the explicit member share one position, so the vector stays.
        string sql = ctx.Chunks.Select(c => new { Chunk = c, c.Embedding }).ToQueryString();

        Assert.Contains("`v`.`embedding`", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEntityInsideAProjectionReadsNullInPlaceOfTheVector()
    {
        using VectorContext ctx = NewContext();

        string sql = ctx.Chunks.Select(c => new { Chunk = c, c.Content.Length }).ToQueryString();

        Assert.Contains("NULL AS `embedding`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ADistinctQueryIsLeftAsItIs()
    {
        using VectorContext ctx = NewContext();

        string sql = ctx.Chunks.Distinct().ToQueryString();

        Assert.Contains("`v`.`embedding`", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AByteArrayDistanceIsUnchanged()
    {
        using VectorContext ctx = NewContext();
        byte[] q = CamusVector.ToBytes([1f, 0f]);

        string sql = ctx.RawChunks.OrderBy(c => EF.Functions.L2Distance(c.Embedding, q)).Take(1).ToQueryString();

        Assert.Contains("ORDER BY l2_distance(`v`.`embedding`, @q)", sql, StringComparison.Ordinal);
        // A vector that is loaded with its entity is not replaced.
        Assert.DoesNotContain("NULL", sql, StringComparison.Ordinal);
    }

    private static IReadOnlyList<string> CreateTablesSql(DbContext ctx)
    {
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IReadOnlyList<MigrationOperation> operations = ctx.GetService<IMigrationsModelDiffer>()
            .GetDifferences(null, model.GetRelationalModel());

        return ctx.GetService<IMigrationsSqlGenerator>()
            .Generate(operations, model)
            .Select(c => c.CommandText.Trim())
            .ToList();
    }

    internal sealed class Chunk
    {
        public string Id { get; set; } = "";
        public string Tag { get; set; } = "";
        public string Content { get; set; } = "";
        public float[] Embedding { get; set; } = [];
        public float[]? Summary { get; set; }
    }

    internal sealed class RawChunk
    {
        public string Id { get; set; } = "";
        public byte[] Embedding { get; set; } = [];
    }

    private sealed class VectorContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Chunk> Chunks => Set<Chunk>();

        public DbSet<RawChunk> RawChunks => Set<RawChunk>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Chunk>(b =>
            {
                b.ToTable("vec_chunks");
                b.HasKey(c => c.Id);
                b.Property(c => c.Id).HasColumnName("id");
                b.Property(c => c.Tag).HasColumnName("tag");
                b.Property(c => c.Content).HasColumnName("content");
                b.Property(c => c.Embedding).HasColumnName("embedding").IsRequired().IsVector(4, loadWithEntity: false);
                b.Property(c => c.Summary).HasColumnName("summary").IsVector(4).HasStorage(CamusColumnStorage.External);
            });

            modelBuilder.Entity<RawChunk>(b =>
            {
                b.ToTable("vec_raw");
                b.HasKey(c => c.Id);
                b.Property(c => c.Id).HasColumnName("id");
                b.Property(c => c.Embedding).HasColumnName("embedding").IsVector(768);
            });
        }
    }

    private sealed class RenamedContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<RawChunk>(b =>
            {
                b.HasKey(c => c.Id);
                b.Property(c => c.Embedding).IsVector(768).HasColumnName("emb");
                b.ToTable("renamed_vec");
            });
    }

    private sealed class PlainContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Chunk>(b =>
            {
                b.ToTable("vec_chunks");
                b.HasKey(c => c.Id);
                b.Property(c => c.Id).HasColumnName("id");
                b.Property(c => c.Tag).HasColumnName("tag");
                b.Property(c => c.Content).HasColumnName("content");
                b.Property(c => c.Embedding).HasColumnName("embedding").IsRequired().IsVector(4, loadWithEntity: false);
                b.Property(c => c.Summary).HasColumnName("summary").IsVector(4).HasStorage(CamusColumnStorage.External);
            });

            modelBuilder.Entity<RawChunk>(b =>
            {
                b.ToTable("vec_raw");
                b.HasKey(c => c.Id);
                b.Property(c => c.Id).HasColumnName("id");
                b.Property(c => c.Embedding).HasColumnName("embedding").HasMaxLength(3072);
            });
        }
    }

    private sealed class OptionalUnloadedContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Chunk>(b =>
            {
                b.HasKey(c => c.Id);
                b.Property(c => c.Embedding).IsRequired().IsVector(4);
                b.Property(c => c.Summary).IsVector(4, loadWithEntity: false);
            });
    }
}
