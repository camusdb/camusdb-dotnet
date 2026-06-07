using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkProvider
{
    // ── Milestone 1 ──────────────────────────────────────────────────────────

    [Fact]
    public void TestUseCamusDBRegistersOptionsExtension()
    {
        DbContextOptionsBuilder builder = new();

        builder.UseCamusDB("Endpoint=http://localhost:5095;Database=test");

        CamusDBOptionsExtension? extension = builder.Options.FindExtension<CamusDBOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Equal("Endpoint=http://localhost:5095;Database=test", extension!.ConnectionString);
    }

    [Fact]
    public void TestAddEntityFrameworkCamusDBRegistersDatabaseProvider()
    {
        ServiceCollection services = new();

        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IDatabaseProvider? databaseProvider = provider.GetService<IDatabaseProvider>();

        Assert.NotNull(databaseProvider);
    }

    // ── Milestone 2 ──────────────────────────────────────────────────────────

    [Fact]
    public void TestAddEntityFrameworkCamusDBRegistersRelationalTypeMappingSource()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IRelationalTypeMappingSource? source = provider.GetService<IRelationalTypeMappingSource>();

        Assert.NotNull(source);
        Assert.IsType<CamusTypeMappingSource>(source);
    }

    [Fact]
    public void TestTypeMappingSourceMapsClrTypesToStoreTypes()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IRelationalTypeMappingSource source = provider.GetRequiredService<IRelationalTypeMappingSource>();

        Assert.Equal("string", source.FindMapping(typeof(string))!.StoreType);
        Assert.Equal("bool", source.FindMapping(typeof(bool))!.StoreType);
        Assert.Equal("int64", source.FindMapping(typeof(int))!.StoreType);
        Assert.Equal("int64", source.FindMapping(typeof(long))!.StoreType);
        Assert.Equal("float64", source.FindMapping(typeof(double))!.StoreType);
        Assert.Equal("id", source.FindMapping(typeof(Guid))!.StoreType);
    }

    [Fact]
    public void TestTypeMappingSourceMapsStoreTypeNamesToMappings()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IRelationalTypeMappingSource source = provider.GetRequiredService<IRelationalTypeMappingSource>();

        Assert.NotNull(source.FindMapping("string"));
        Assert.NotNull(source.FindMapping("bool"));
        Assert.NotNull(source.FindMapping("int64"));
        Assert.NotNull(source.FindMapping("float64"));
        Assert.NotNull(source.FindMapping("id"));
    }

    [Fact]
    public void TestModelValidatorRejectsConcurrencyTokens()
    {
        var options = new DbContextOptionsBuilder<ConcurrencyTokenContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        Assert.Throws<NotSupportedException>(() =>
        {
            using var ctx = new ConcurrencyTokenContext(options);
            ctx.Model.GetEntityTypes(); // triggers model building + validation
        });
    }

    [Fact]
    public void TestModelValidatorRejectsUnsupportedKeyTypes()
    {
        var options = new DbContextOptionsBuilder<BadKeyTypeContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        Assert.Throws<NotSupportedException>(() =>
        {
            using var ctx = new BadKeyTypeContext(options);
            ctx.Model.GetEntityTypes();
        });
    }

    // ── Milestone 3 ──────────────────────────────────────────────────────────

    [Fact]
    public void TestSqlGenerationHelperProducesUnquotedIdentifiers()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        ISqlGenerationHelper helper = provider.GetRequiredService<ISqlGenerationHelper>();

        Assert.Equal("MyTable", helper.DelimitIdentifier("MyTable"));
        Assert.Equal("my_column", helper.DelimitIdentifier("my_column"));
        Assert.Equal("", helper.StatementTerminator);
    }

    [Fact]
    public void TestSqlGenerationHelperGeneratesAtPrefixedParameters()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        ISqlGenerationHelper helper = provider.GetRequiredService<ISqlGenerationHelper>();

        Assert.Equal("@p0", helper.GenerateParameterName("p0"));
    }

    [Fact]
    public void TestObjectIdValueGeneratorProducesNonTemporaryValues()
    {
        var gen = new CamusObjectIdValueGenerator();

        Assert.False(gen.GeneratesTemporaryValues);
    }

    [Fact]
    public void TestObjectIdValueGeneratorProducesUniqueNonEmptyStrings()
    {
        var gen = new CamusObjectIdValueGenerator();

        var v1 = gen.Next(null!);
        var v2 = gen.Next(null!);

        Assert.NotEmpty(v1);
        Assert.NotEmpty(v2);
        Assert.NotEqual(v1, v2);
        Assert.Equal(24, v1.Length); // 12-byte ObjectId as hex
    }

    [Fact]
    public void TestValueGeneratorSelectorRegistered()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IValueGeneratorSelector selector = provider.GetRequiredService<IValueGeneratorSelector>();

        Assert.IsType<CamusValueGeneratorSelector>(selector);
    }

    [Fact]
    public void TestRelationalDatabaseCreatorRegistered()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        Assert.NotNull(creator);
        Assert.IsType<CamusDatabaseCreator>(creator);
    }

    [Fact]
    public void TestDatabaseCreatorExistsAlwaysReturnsTrue()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var creator = ctx.GetService<IRelationalDatabaseCreator>();

        Assert.True(creator.Exists());
    }

    // ── Milestone 4 ──────────────────────────────────────────────────────────

    [Fact]
    public void TestMigrationsSqlGeneratorRegistered()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        Assert.NotNull(generator);
        Assert.IsType<CamusMigrationsSqlGenerator>(generator);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorCreateTable()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "products",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(string), ColumnType = "id", IsNullable = false },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "string", IsNullable = false },
                new AddColumnOperation { Name = "Price", ClrType = typeof(double), ColumnType = "float64", IsNullable = true },
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        var sql = commands[0].CommandText;
        Assert.Contains("CREATE TABLE products", sql);
        Assert.Contains("Id OID PRIMARY KEY NOT NULL", sql);
        Assert.Contains("Name STRING NOT NULL", sql);
        Assert.Contains("Price FLOAT64", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorDropTable()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var commands = generator.Generate([new DropTableOperation { Name = "products" }], null);

        Assert.Single(commands);
        Assert.Contains("DROP TABLE products", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorCreateIndex()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateIndexOperation
        {
            Name = "idx_products_name",
            Table = "products",
            Columns = ["Name"],
            IsUnique = false
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        var sql = commands[0].CommandText;
        Assert.Contains("CREATE INDEX idx_products_name ON products", sql);
        Assert.Contains("Name", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorThrowsForUnsupportedOperations()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var op = new AddColumnOperation { Name = "NewCol", Table = "t", ClrType = typeof(string) };

        Assert.Throws<NotSupportedException>(() => generator.Generate([op], null));
    }

    [Fact]
    public void TestHistoryRepositoryRegistered()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var repo = ctx.GetService<IHistoryRepository>();

        Assert.NotNull(repo);
        Assert.IsType<CamusHistoryRepository>(repo);
    }

    [Fact]
    public void TestHistoryRepositoryExistsReturnsFalseWithoutServer()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var repo = ctx.GetService<IHistoryRepository>();

        // Without a running server, Exists() should return false (not throw)
        Assert.False(repo.Exists());
    }

    [Fact]
    public void TestExecutionStrategyRegistered()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var factory = ctx.GetService<IExecutionStrategyFactory>();

        Assert.NotNull(factory);
        Assert.IsType<CamusExecutionStrategyFactory>(factory);

        var strategy = factory.Create();
        Assert.IsType<CamusExecutionStrategy>(strategy);
    }

    [Fact]
    public void TestExecutionStrategyDoesNotRetry()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var factory = ctx.GetService<IExecutionStrategyFactory>();
        var strategy = factory.Create();

        Assert.False(strategy.RetriesOnFailure);
    }

    // ── Helper DbContexts for validation tests ────────────────────────────────

    private class ConcurrencyTokenContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<RowVersionEntity> Entities => Set<RowVersionEntity>();
    }

    private class RowVersionEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        [System.ComponentModel.DataAnnotations.ConcurrencyCheck]
        public string RowVersion { get; set; } = "";
    }

    private class BadKeyTypeContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<DateKeyEntity> Entities => Set<DateKeyEntity>();
    }

    private class DateKeyEntity
    {
        public DateTime Id { get; set; }
        public string Name { get; set; } = "";
    }

    private class SimpleProductContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Product> Products => Set<Product>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Product>(b =>
            {
                b.ToTable("products");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnType("string");
                b.Property(e => e.Price).HasColumnType("float64");
            });
        }
    }

    private class Product
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public double Price { get; set; }
    }
}
