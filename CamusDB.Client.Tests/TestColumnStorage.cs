/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.Client.Tests;

/// <summary>
/// Large-value column storage strategies (<c>STORAGE PLAIN | MAIN | EXTERNAL | EXTENDED</c>) and
/// <c>REWRITE STORAGE</c> through the EF Core provider. No server is needed: these assert the model
/// metadata and the DDL text that the server receives.
/// </summary>
public class TestColumnStorage
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    [Theory]
    [InlineData(CamusColumnStorage.Extended, "EXTENDED")]
    [InlineData(CamusColumnStorage.Plain, "PLAIN")]
    [InlineData(CamusColumnStorage.Main, "MAIN")]
    [InlineData(CamusColumnStorage.External, "EXTERNAL")]
    public void TestToSql(CamusColumnStorage storage, string expected)
        => Assert.Equal(expected, storage.ToSql());

    [Fact]
    public void TestToSqlRejectsUndefinedValue()
        => Assert.Throws<ArgumentOutOfRangeException>(() => ((CamusColumnStorage)42).ToSql());

    [Fact]
    public void TestNumericValuesMatchServer()
    {
        // The server persists these numbers in schema JSON; they must never drift.
        Assert.Equal(0, (int)CamusColumnStorage.Extended);
        Assert.Equal(1, (int)CamusColumnStorage.Plain);
        Assert.Equal(2, (int)CamusColumnStorage.Main);
        Assert.Equal(3, (int)CamusColumnStorage.External);
    }

    [Fact]
    public void TestHasStorageSetsAnnotation()
    {
        using DocsContext ctx = new(Options<DocsContext>());
        IEntityType entity = ctx.Model.FindEntityType(typeof(Doc))!;

        Assert.Null(entity.FindProperty(nameof(Doc.Title))!.GetStorage());
        Assert.Equal(CamusColumnStorage.Extended, entity.FindProperty(nameof(Doc.Body))!.GetStorage());
        Assert.Equal(CamusColumnStorage.External, entity.FindProperty(nameof(Doc.Thumbnail))!.GetStorage());
        Assert.Equal(CamusColumnStorage.Plain, entity.FindProperty(nameof(Doc.Embedding))!.GetStorage());
        Assert.Equal(CamusColumnStorage.Main, entity.FindProperty(nameof(Doc.Tags))!.GetStorage());
    }

    [Fact]
    public void TestCreateTableFromModelEmitsStorageClauses()
    {
        using DocsContext ctx = new(Options<DocsContext>());

        string sql = Assert.Single(CreateTablesSql(ctx));

        Assert.Contains("`Title` STRING NOT NULL,", sql);
        Assert.Contains("`Body` STRING NOT NULL STORAGE EXTENDED COMMENT 'Article body'", sql);
        Assert.Contains("`Thumbnail` BYTES STORAGE EXTERNAL", sql);
        Assert.Contains("`Embedding` BYTES(3072) STORAGE PLAIN", sql);
        Assert.Contains("`Tags` ARRAY(STRING) STORAGE MAIN", sql);
        Assert.DoesNotContain("`Id` OID NOT NULL STORAGE", sql);
    }

    [Fact]
    public void TestModelWithoutStorageEmitsNoClause()
    {
        using PlainDocsContext ctx = new(Options<PlainDocsContext>());

        Assert.DoesNotContain("STORAGE", Assert.Single(CreateTablesSql(ctx)));
    }

    [Fact]
    public void TestAddColumnEmitsStorageBeforeComment()
    {
        AddColumnOperation operation = new()
        {
            Table = "docs",
            Name = "body",
            ClrType = typeof(string),
            ColumnType = "string",
            IsNullable = true,
            Comment = "Article body",
        };
        operation.AddAnnotation(CamusAnnotationNames.ColumnStorage, CamusColumnStorage.External);

        Assert.Equal(
            "ALTER TABLE `docs` ADD COLUMN `body` STRING STORAGE EXTERNAL COMMENT 'Article body'",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestAlterColumnStorageEmitsSetStorage()
    {
        AlterColumnOperation operation = AlterBytes(oldStorage: null, newStorage: CamusColumnStorage.Plain);

        Assert.Equal(
            "ALTER TABLE `docs` ALTER COLUMN `embedding` SET STORAGE PLAIN",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestAlterColumnRemovedStorageResetsToExtended()
    {
        // CamusDB has no RESET STORAGE, so a strategy removed from the model is set back to the default.
        AlterColumnOperation operation = AlterBytes(oldStorage: CamusColumnStorage.Plain, newStorage: null);

        Assert.Equal(
            "ALTER TABLE `docs` ALTER COLUMN `embedding` SET STORAGE EXTENDED",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestAlterColumnStorageAndNullability()
    {
        AlterColumnOperation operation = AlterBytes(oldStorage: CamusColumnStorage.Extended, newStorage: CamusColumnStorage.External);
        operation.IsNullable = false;

        Assert.Equal(
            [
                "ALTER TABLE `docs` ALTER COLUMN `embedding` SET NOT NULL",
                "ALTER TABLE `docs` ALTER COLUMN `embedding` SET STORAGE EXTERNAL",
            ],
            Generate(operation));
    }

    [Fact]
    public void TestAlterColumnAcceptsStorageAsSqlKeyword()
    {
        // A hand-written migration can spell the annotation value as the SQL keyword.
        AlterColumnOperation operation = AlterBytes(oldStorage: null, newStorage: null);
        operation.AddAnnotation(CamusAnnotationNames.ColumnStorage, "external");

        Assert.Equal(
            "ALTER TABLE `docs` ALTER COLUMN `embedding` SET STORAGE EXTERNAL",
            Assert.Single(Generate(operation)));
    }

    [Theory]
    [InlineData("toast")]
    [InlineData("1")]
    [InlineData("Plain, Main")]
    public void TestAlterColumnRejectsUnknownStorageKeyword(string keyword)
    {
        AlterColumnOperation operation = AlterBytes(oldStorage: null, newStorage: null);
        operation.AddAnnotation(CamusAnnotationNames.ColumnStorage, keyword);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => Generate(operation));
        Assert.Contains(keyword, ex.Message);
    }

    [Fact]
    public void TestUnchangedAlterColumnStillRejected()
    {
        AlterColumnOperation operation = AlterBytes(oldStorage: CamusColumnStorage.Plain, newStorage: CamusColumnStorage.Plain);

        Assert.Throws<NotSupportedException>(() => Generate(operation));
    }

    [Fact]
    public void TestValidatorRejectsStorageOnFixedWidthColumn()
    {
        using BadStorageContext ctx = new(Options<BadStorageContext>());

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => ctx.Model);
        Assert.Contains("BadDoc.Year", ex.Message);
        Assert.Contains("int64", ex.Message);
    }

    [Fact]
    public void TestRewriteStorageMigration()
    {
        MigrationBuilder builder = new("CamusDB");
        builder.RewriteStorage("docs");

        SqlOperation operation = Assert.IsType<SqlOperation>(Assert.Single(builder.Operations));
        Assert.Equal("ALTER TABLE `docs` REWRITE STORAGE", operation.Sql);
        // The rewrite commits its own batches, so it cannot run inside the migration transaction.
        Assert.True(operation.SuppressTransaction);
    }

    [Fact]
    public void TestRewriteStorageInlineMigration()
    {
        MigrationBuilder builder = new("CamusDB");
        builder.RewriteStorage("docs", inline: true);

        Assert.Equal(
            "ALTER TABLE `docs` REWRITE STORAGE INLINE",
            Assert.IsType<SqlOperation>(Assert.Single(builder.Operations)).Sql);
    }

    [Fact]
    public void TestRewriteStorageRejectsBacktickInName()
    {
        MigrationBuilder builder = new("CamusDB");

        Assert.Throws<ArgumentException>(() => builder.RewriteStorage("docs` x"));
    }

    [Fact]
    public void TestAnnotationCodeGeneratorRendersHasStorage()
    {
        using DocsContext ctx = new(Options<DocsContext>());
        IProperty embedding = ctx.GetService<IDesignTimeModel>().Model
            .FindEntityType(typeof(Doc))!.FindProperty(nameof(Doc.Embedding))!;

#pragma warning disable EF1001 // The dependencies type is marked internal; the design-time host builds it the same way.
        CamusAnnotationCodeGenerator generator = new(
            new AnnotationCodeGeneratorDependencies(ctx.GetService<IRelationalTypeMappingSource>()));
#pragma warning restore EF1001

        Dictionary<string, IAnnotation> annotations = embedding.GetAnnotations()
            .ToDictionary(a => a.Name, a => a);

        MethodCallCodeFragment fragment = Assert.Single(
            generator.GenerateFluentApiCalls(embedding, annotations),
            f => f.Method == nameof(CamusPropertyBuilderExtensions.HasStorage));

        Assert.Equal(CamusColumnStorage.Plain, Assert.Single(fragment.Arguments));
        Assert.False(annotations.ContainsKey(CamusAnnotationNames.ColumnStorage));
    }

    private static DbContextOptions<T> Options<T>() where T : DbContext
        => new DbContextOptionsBuilder<T>().UseCamusDB(ConnString).Options;

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

    private static IReadOnlyList<string> Generate(MigrationOperation operation)
    {
        using DocsContext ctx = new(Options<DocsContext>());

        return ctx.GetService<IMigrationsSqlGenerator>()
            .Generate([operation], null)
            .Select(c => c.CommandText.Trim())
            .ToList();
    }

    private static AlterColumnOperation AlterBytes(CamusColumnStorage? oldStorage, CamusColumnStorage? newStorage)
    {
        AlterColumnOperation operation = new()
        {
            Table = "docs",
            Name = "embedding",
            ClrType = typeof(byte[]),
            ColumnType = "bytes(3072)",
            IsNullable = true,
            OldColumn = new AddColumnOperation
            {
                Table = "docs",
                Name = "embedding",
                ClrType = typeof(byte[]),
                ColumnType = "bytes(3072)",
                IsNullable = true,
            },
        };

        if (newStorage is not null)
            operation.AddAnnotation(CamusAnnotationNames.ColumnStorage, newStorage);

        if (oldStorage is not null)
            operation.OldColumn.AddAnnotation(CamusAnnotationNames.ColumnStorage, oldStorage);

        return operation;
    }

    private class DocsContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Doc> Docs => Set<Doc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Doc>(b =>
            {
                b.ToTable("docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Title);
                b.Property(e => e.Body).HasStorage(CamusColumnStorage.Extended).HasComment("Article body");
                b.Property(e => e.Thumbnail).HasStorage(CamusColumnStorage.External);
                b.Property(e => e.Embedding).HasMaxLength(3072).HasStorage(CamusColumnStorage.Plain);
                b.Property(e => e.Tags).HasStorage(CamusColumnStorage.Main);
            });
        }
    }

    private class PlainDocsContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Doc> Docs => Set<Doc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Doc>(b =>
            {
                b.ToTable("docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
            });
        }
    }

    private class BadStorageContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<BadDoc> Docs => Set<BadDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BadDoc>(b =>
            {
                b.ToTable("bad_docs");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Year).HasStorage(CamusColumnStorage.Plain);
            });
        }
    }

    private class Doc
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public byte[]? Thumbnail { get; set; }
        public byte[]? Embedding { get; set; }
        public string[]? Tags { get; set; }
    }

    private class BadDoc
    {
        public string Id { get; set; } = "";
        public long Year { get; set; }
    }
}
