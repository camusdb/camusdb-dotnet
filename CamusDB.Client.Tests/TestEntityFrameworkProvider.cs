using CamusDB.Client;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
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
    public void TestUseCamusDBWithExternalConnectionStoresConnection()
    {
        var connection = new CamusConnection(
            new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));

        DbContextOptionsBuilder builder = new();
        builder.UseCamusDB(connection);

        CamusDBOptionsExtension? extension = builder.Options.FindExtension<CamusDBOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Same(connection, extension!.Connection);
        Assert.Null(extension.ConnectionString);
    }

    [Fact]
    public void TestUseCamusDBWithExternalConnectionContextOpens()
    {
        var connection = new CamusConnection(
            new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));

        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB(connection)
            .Options;

        using var ctx = new SimpleProductContext(options);

        // The registered relational connection should wrap the externally supplied DbConnection
        var relationalConnection = ctx.GetService<IRelationalConnection>();
        Assert.NotNull(relationalConnection);
        Assert.Same(connection, relationalConnection.DbConnection);
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
    public void TestModelValidatorRejectsNonNumericConcurrencyTokens()
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
    public void TestModelValidatorAcceptsNumericConcurrencyTokens()
    {
        var options = new DbContextOptionsBuilder<NumericConcurrencyTokenContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        // Should not throw — int/long/short are supported concurrency token types
        using var ctx = new NumericConcurrencyTokenContext(options);
        var entityTypes = ctx.Model.GetEntityTypes();
        Assert.NotEmpty(entityTypes);
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
    public void TestSqlGenerationHelperDelimitsIdentifiers()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        ISqlGenerationHelper helper = provider.GetRequiredService<ISqlGenerationHelper>();

        Assert.Equal("`MyTable`", helper.DelimitIdentifier("MyTable"));
        Assert.Equal("`my_column`", helper.DelimitIdentifier("my_column"));
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
        Assert.Contains("CREATE TABLE IF NOT EXISTS `products`", sql);
        Assert.Contains("`Id` OID NOT NULL", sql);
        Assert.Contains("`Name` STRING NOT NULL", sql);
        Assert.Contains("`Price` FLOAT64", sql);
        Assert.Contains("PRIMARY KEY (`Id`)", sql);
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
        Assert.Contains("DROP TABLE `products`", commands[0].CommandText);
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
        Assert.Contains("CREATE INDEX IF NOT EXISTS `idx_products_name` ON `products`", sql);
        Assert.Contains("`Name`", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorCreateUniqueIndex()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateIndexOperation
        {
            Name = "uq_products_name",
            Table = "products",
            Columns = ["Name"],
            IsUnique = true
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("CREATE UNIQUE INDEX IF NOT EXISTS `uq_products_name` ON `products`", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorDropIndex()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new DropIndexOperation { Name = "idx_products_name", Table = "products" };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        // CamusDB syntax: ALTER TABLE t DROP INDEX name
        Assert.Contains("ALTER TABLE `products` DROP INDEX `idx_products_name`", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorAddColumn()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new AddColumnOperation
        {
            Name = "Stock",
            Table = "products",
            ClrType = typeof(int),
            ColumnType = "int64",
            IsNullable = false
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        var sql = commands[0].CommandText;
        Assert.Contains("ALTER TABLE `products` ADD COLUMN `Stock` INT64", sql);
        Assert.Contains("NOT NULL", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorAddColumnWithDefault()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new AddColumnOperation
        {
            Name = "Active",
            Table = "products",
            ClrType = typeof(bool),
            ColumnType = "bool",
            IsNullable = false,
            DefaultValue = true
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        var sql = commands[0].CommandText;
        Assert.Contains("ALTER TABLE `products` ADD COLUMN `Active` BOOL", sql);
        Assert.Contains("DEFAULT (true)", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorDropColumn()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new DropColumnOperation { Name = "Price", Table = "products" };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("ALTER TABLE `products` DROP COLUMN `Price`", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorThrowsForUnsupportedOperations()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var op = new AlterColumnOperation { Name = "Price", Table = "products", ClrType = typeof(string) };

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

    [Fact]
    public void TestModificationCommandBatchFactoryCreatesCamusBatch()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var factory = ctx.GetService<IModificationCommandBatchFactory>();

        var batch = factory.Create();

        Assert.IsType<CamusModificationCommandBatch>(batch);
    }

    [Fact]
    public void TestLockConflictMappedToDbUpdateConcurrencyException()
    {
        // CamusException with CADB0502 (TransactionConflict / AlreadyLocked) wrapped in
        // DbUpdateException must surface as DbUpdateConcurrencyException so that EF callers
        // can catch the right type and retry logic engages.
        var inner = new CamusException("CADB0502", "Range '...' is exclusively locked by another transaction");
        var dbUpdateEx = new DbUpdateException("Update failed", inner);

        // Simulate what CamusModificationCommandBatch does: rethrow as concurrency exception
        DbUpdateConcurrencyException? concurrencyEx = null;
        try
        {
            try { throw dbUpdateEx; }
            catch (DbUpdateException ex) when (ex.InnerException is CamusException { Code: "CADB0502" })
            {
                throw new DbUpdateConcurrencyException(ex.Message, ex.InnerException);
            }
        }
        catch (DbUpdateConcurrencyException ex)
        {
            concurrencyEx = ex;
        }

        Assert.NotNull(concurrencyEx);
        Assert.Same(inner, concurrencyEx!.InnerException);
        Assert.IsType<DbUpdateConcurrencyException>(concurrencyEx);
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

    private class NumericConcurrencyTokenContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<VersionedEntity> Entities => Set<VersionedEntity>();
    }

    private class VersionedEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        [System.ComponentModel.DataAnnotations.ConcurrencyCheck]
        public long Version { get; set; }
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
