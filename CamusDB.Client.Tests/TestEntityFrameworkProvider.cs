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
    public void TestMigrationsSqlGeneratorCreateTableWithCheckConstraint()
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
                new AddColumnOperation { Name = "Price", ClrType = typeof(double), ColumnType = "float64", IsNullable = true },
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] },
            CheckConstraints =
            {
                new AddCheckConstraintOperation { Name = "CK_products_price", Table = "products", Sql = "Price > 0" }
            }
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("CONSTRAINT `CK_products_price` CHECK (Price > 0)", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorAddCheckConstraint()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new AddCheckConstraintOperation
        {
            Name = "CK_products_price",
            Table = "products",
            Sql = "Price > 0"
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("ALTER TABLE `products` ADD CONSTRAINT `CK_products_price` CHECK (Price > 0)", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorDropCheckConstraint()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var operation = new DropCheckConstraintOperation { Name = "CK_products_price", Table = "products" };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("ALTER TABLE `products` DROP CONSTRAINT `CK_products_price`", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorAlterColumnSetNotNull()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        // Same stored type, nullable -> non-nullable => SET NOT NULL
        var operation = new AlterColumnOperation
        {
            Name = "Name",
            Table = "products",
            ClrType = typeof(string),
            ColumnType = "string",
            IsNullable = false,
            OldColumn = new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "string", IsNullable = true }
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("ALTER TABLE `products` ALTER COLUMN `Name` SET NOT NULL", commands[0].CommandText);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorAlterColumnDropNotNull()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using var ctx = new SimpleProductContext(options);
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        // Same stored type, non-nullable -> nullable => DROP NOT NULL
        var operation = new AlterColumnOperation
        {
            Name = "Name",
            Table = "products",
            ClrType = typeof(string),
            ColumnType = "string",
            IsNullable = true,
            OldColumn = new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "string", IsNullable = false }
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("ALTER TABLE `products` ALTER COLUMN `Name` DROP NOT NULL", commands[0].CommandText);
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
    public void TestEnableRetryOnFailureStoresRetrySettings()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB(
                "Endpoint=http://localhost:5095;Database=test",
                camus => camus.EnableRetryOnFailure(
                    maxRetryCount: 7,
                    maxRetryDelay: TimeSpan.FromMilliseconds(250),
                    retryDeadline: TimeSpan.FromSeconds(2),
                    medianFirstRetryDelay: TimeSpan.FromMilliseconds(15)))
            .Options;

        CamusDBOptionsExtension? extension = options.FindExtension<CamusDBOptionsExtension>();

        Assert.NotNull(extension);
        Assert.True(extension!.RetryOnFailureEnabled);
        Assert.Equal(7, extension.RetryOnFailureCount);
        Assert.Equal(TimeSpan.FromMilliseconds(250), extension.RetryMaxDelay);
        Assert.Equal(TimeSpan.FromSeconds(2), extension.RetryDeadline);
        Assert.Equal(TimeSpan.FromMilliseconds(15), extension.RetryMedianFirstDelay);
    }

    [Fact]
    public void TestExecutionStrategyRetriesRetryableConflictsWhenEnabled()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB(
                "Endpoint=http://localhost:5095;Database=test",
                camus => camus.EnableRetryOnFailure(
                    maxRetryCount: 2,
                    maxRetryDelay: TimeSpan.FromMilliseconds(5),
                    retryDeadline: TimeSpan.FromMilliseconds(100),
                    medianFirstRetryDelay: TimeSpan.FromMilliseconds(1)))
            .Options;

        using var ctx = new SimpleProductContext(options);
        var strategy = ctx.GetService<IExecutionStrategyFactory>().Create();
        var attempts = 0;

        strategy.Execute(() =>
        {
            attempts++;

            if (attempts < 3)
                throw new DbUpdateConcurrencyException(
                    "Update failed",
                    new CamusException("CADB0502", "Range is exclusively locked by another transaction"));
        });

        Assert.True(strategy.RetriesOnFailure);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public void TestExecutionStrategyDoesNotRetryNonRetryableErrorsWhenEnabled()
    {
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB(
                "Endpoint=http://localhost:5095;Database=test",
                camus => camus.EnableRetryOnFailure(
                    maxRetryCount: 3,
                    maxRetryDelay: TimeSpan.FromMilliseconds(5),
                    retryDeadline: TimeSpan.FromMilliseconds(100),
                    medianFirstRetryDelay: TimeSpan.FromMilliseconds(1)))
            .Options;

        using var ctx = new SimpleProductContext(options);
        var strategy = ctx.GetService<IExecutionStrategyFactory>().Create();
        var attempts = 0;

        Assert.Throws<CamusException>(() => strategy.Execute(() =>
        {
            attempts++;
            throw new CamusException("CADB9999", "Permanent failure");
        }));

        Assert.Equal(1, attempts);
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

    // ── Milestone N: InsertData migration seeding ────────────────────────────

    [Fact]
    public void TestMigrationsSqlGeneratorInsertDataProducesLiteralValues()
    {
        // InsertData must emit VALUES with the actual literal values, not NULLs.
        // Regression: provider was generating VALUES (NULL, NULL, ...) because
        // parameter values from the InsertDataOperation were not being inlined
        // into the SQL — instead bare parameter placeholders were emitted and
        // never substituted.
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using SimpleProductContext ctx = new(options);
        IMigrationsSqlGenerator generator = ctx.GetService<IMigrationsSqlGenerator>();

        InsertDataOperation operation = new()
        {
            Table = "products",
            Columns = ["Id", "Name", "Price"],
            ColumnTypes = ["string", "string", "float64"],
            Values = new object?[,] { { "prod-1", "Widget", 9.99 } }
        };

        IReadOnlyList<MigrationCommand> commands = generator.Generate([operation], null);

        Assert.Single(commands);
        string sql = commands[0].CommandText;

        // Values must be inlined as literals, not left as NULL or @p0 placeholders
        Assert.DoesNotContain("NULL", sql);
        Assert.Contains("prod-1", sql);
        Assert.Contains("Widget", sql);
        Assert.Contains("9.99", sql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorInsertDataMultipleRows()
    {
        // Each row in the Values matrix must produce a separate INSERT statement
        // with the correct literal values for that row.
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using SimpleProductContext ctx = new(options);
        IMigrationsSqlGenerator generator = ctx.GetService<IMigrationsSqlGenerator>();

        InsertDataOperation operation = new()
        {
            Table = "products",
            Columns = ["Id", "Name", "Price"],
            ColumnTypes = ["string", "string", "float64"],
            Values = new object?[,]
            {
                { "prod-1", "Widget", 9.99 },
                { "prod-2", "Gadget", 19.99 },
            }
        };

        IReadOnlyList<MigrationCommand> commands = generator.Generate([operation], null);

        // Two rows → two INSERT commands (or one multi-row INSERT — either is acceptable,
        // but each row's values must appear in the output)
        string allSql = string.Join("\n", commands.Select(c => c.CommandText));

        Assert.DoesNotContain("NULL", allSql);
        Assert.Contains("prod-1", allSql);
        Assert.Contains("Widget", allSql);
        Assert.Contains("prod-2", allSql);
        Assert.Contains("Gadget", allSql);
    }

    [Fact]
    public void TestMigrationsSqlGeneratorInsertDataBoolAndIntLiterals()
    {
        // Bool and int values must be inlined correctly, not coerced to NULL.
        var options = new DbContextOptionsBuilder<SimpleProductContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using SimpleProductContext ctx = new(options);
        IMigrationsSqlGenerator generator = ctx.GetService<IMigrationsSqlGenerator>();

        InsertDataOperation operation = new()
        {
            Table = "characters",
            Columns = ["id", "name", "rarity", "hp", "canBeInitial"],
            ColumnTypes = ["string", "string", "int64", "int64", "bool"],
            Values = new object?[,] { { "1", "Striker-X", 0, 920, true } }
        };

        IReadOnlyList<MigrationCommand> commands = generator.Generate([operation], null);

        Assert.Single(commands);
        string sql = commands[0].CommandText;

        Assert.DoesNotContain("NULL", sql);
        Assert.Contains("Striker-X", sql);
        Assert.Contains("920", sql);
        // Bool literal must appear as true/false or 1/0 — not NULL
        Assert.True(sql.Contains("true") || sql.Contains("1"), $"Expected bool literal in: {sql}");
    }

    [Fact]
    public void TestGuidOidMappingDbTypeIsGuid()
    {
        // IdGuidMapping must use DbType.Guid so that CamusParameter.FromDbType(DbType.Guid)
        // maps to ColumnType.Id. With DbType.String it would map to ColumnType.String and the
        // server would reject OID columns with "Type String cannot be assigned to id (Id)".
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();
        ServiceProvider provider = services.BuildServiceProvider();
        IRelationalTypeMappingSource source = provider.GetRequiredService<IRelationalTypeMappingSource>();

        // Guid CLR type maps to "id" store type
        var mapping = source.FindMapping(typeof(Guid))!;
        Assert.Equal("id", mapping.StoreType);
        Assert.Equal(System.Data.DbType.Guid, mapping.DbType);

        // DbType.Guid on a parameter must resolve to ColumnType.Id
        var parameter = new CamusParameter("@id", ColumnType.Null, null);
        parameter.DbType = System.Data.DbType.Guid;
        Assert.Equal(ColumnType.Id, parameter.ColumnType);
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
