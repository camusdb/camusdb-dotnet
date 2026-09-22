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
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace CamusDB.Client.Tests;

/// <summary>
/// Sequences through the EF Core provider: the migration DDL, the model configuration of
/// <c>UseSequence</c> and <c>UseHiLo</c>, and the validator. No server is needed.
/// </summary>
public class TestSequences
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    [Fact]
    public void TestCreateSequence()
    {
        CreateSequenceOperation operation = new() { Name = "order_no", ClrType = typeof(long), StartValue = 1, IncrementBy = 1 };

        Assert.Equal("CREATE SEQUENCE `order_no` START WITH 1 INCREMENT BY 1", Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestCreateSequenceWithBounds()
    {
        CreateSequenceOperation operation = new()
        {
            Name = "invoice_no",
            ClrType = typeof(int),
            StartValue = 1000,
            IncrementBy = 5,
            MinValue = 1000,
            MaxValue = 999999,
        };

        Assert.Equal(
            "CREATE SEQUENCE `invoice_no` START WITH 1000 INCREMENT BY 5 MINVALUE 1000 MAXVALUE 999999",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestCreateCyclicSequenceIsRefused()
    {
        CreateSequenceOperation operation = new() { Name = "s", ClrType = typeof(long), StartValue = 1, IncrementBy = 1, IsCyclic = true };

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => Generate(operation));
        Assert.Contains("'s'", ex.Message);
    }

    [Fact]
    public void TestCreateDecimalSequenceIsRefused()
    {
        CreateSequenceOperation operation = new() { Name = "s", ClrType = typeof(decimal), StartValue = 1, IncrementBy = 1 };

        Assert.Throws<NotSupportedException>(() => Generate(operation));
    }

    [Fact]
    public void TestDropSequence()
        => Assert.Equal("DROP SEQUENCE `order_no`", Assert.Single(Generate(new DropSequenceOperation { Name = "order_no" })));

    [Fact]
    public void TestRenameSequence()
        => Assert.Equal(
            "ALTER SEQUENCE `order_no` RENAME TO `order_number`",
            Assert.Single(Generate(new RenameSequenceOperation { Name = "order_no", NewName = "order_number" })));

    [Fact]
    public void TestRenameSequenceSchemaOnlyEmitsNothing()
        => Assert.Empty(Generate(new RenameSequenceOperation { Name = "order_no", NewSchema = "sales" }));

    [Fact]
    public void TestAlterSequenceEmitsChangedOptionsOnly()
    {
        AlterSequenceOperation operation = new()
        {
            Name = "order_no",
            IncrementBy = 10,
            MinValue = 5,
            MaxValue = 100000,
            OldSequence = new AlterSequenceOperation { IncrementBy = 1, MinValue = 5 },
        };

        Assert.Equal(
            "ALTER SEQUENCE `order_no` INCREMENT BY 10 MAXVALUE 100000",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestAlterSequenceRemovedBoundsGoBackToDefaults()
    {
        // A null minimum is the server default 1, never NO MINVALUE: the server reads NO MINVALUE as the
        // smallest 64-bit value, which a model without a minimum does not mean.
        AlterSequenceOperation operation = new()
        {
            Name = "order_no",
            IncrementBy = 1,
            OldSequence = new AlterSequenceOperation { IncrementBy = 1, MinValue = 100, MaxValue = 500 },
        };

        Assert.Equal(
            "ALTER SEQUENCE `order_no` MINVALUE 1 NO MAXVALUE",
            Assert.Single(Generate(operation)));
    }

    [Fact]
    public void TestAlterSequenceWithoutChangeEmitsNothing()
    {
        // The server grammar needs at least one option, and cyclic true → false has nothing to change.
        AlterSequenceOperation operation = new()
        {
            Name = "order_no",
            IncrementBy = 1,
            OldSequence = new AlterSequenceOperation { IncrementBy = 1, IsCyclic = true },
        };

        Assert.Empty(Generate(operation));
    }

    [Fact]
    public void TestAlterSequenceToCyclicIsRefused()
    {
        AlterSequenceOperation operation = new()
        {
            Name = "order_no",
            IncrementBy = 1,
            IsCyclic = true,
            OldSequence = new AlterSequenceOperation { IncrementBy = 1 },
        };

        Assert.Throws<NotSupportedException>(() => Generate(operation));
    }

    [Theory]
    [InlineData(null, "ALTER SEQUENCE `order_no` RESTART")]
    [InlineData(500L, "ALTER SEQUENCE `order_no` RESTART WITH 500")]
    public void TestRestartSequenceRunsOutsideTheMigrationTransaction(long? startValue, string expected)
    {
        using OrdersContext ctx = new(Options<OrdersContext>());

        MigrationCommand command = Assert.Single(ctx.GetService<IMigrationsSqlGenerator>()
            .Generate([new RestartSequenceOperation { Name = "order_no", StartValue = startValue }], null));

        Assert.Equal(expected, command.CommandText.Trim());
        // The server refuses RESTART inside an explicit transaction: a rollback cannot move the counter back.
        Assert.True(command.TransactionSuppressed);
    }

    [Fact]
    public void TestMigrationBuilderSequenceRoundTrip()
    {
        MigrationBuilder builder = new("CamusDB");
        builder.CreateSequence("order_no", startValue: 10, incrementBy: 2);
        builder.RenameSequence("order_no", newName: "order_number");
        builder.DropSequence("order_number");

        using OrdersContext ctx = new(Options<OrdersContext>());
        List<string> sql = ctx.GetService<IMigrationsSqlGenerator>()
            .Generate(builder.Operations, null)
            .Select(c => c.CommandText.Trim())
            .ToList();

        Assert.Equal(
            [
                "CREATE SEQUENCE `order_no` START WITH 10 INCREMENT BY 2",
                "ALTER SEQUENCE `order_no` RENAME TO `order_number`",
                "DROP SEQUENCE `order_number`",
            ],
            sql);
    }

    [Fact]
    public void TestUseSequenceConfiguresProperty()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IProperty number = model.FindEntityType(typeof(Order))!.FindProperty(nameof(Order.Number))!;

        Assert.Equal("order_no", number.GetSequenceName());
        Assert.Null(number.GetHiLoSequenceName());
        Assert.Equal(ValueGenerated.OnAdd, number.ValueGenerated);
        Assert.Equal("nextval('order_no')", number.GetDefaultValueSql());
        Assert.NotNull(number.GetValueGeneratorFactory());

        IReadOnlySequence sequence = model.FindSequence("order_no")!;
        Assert.Equal(1000, sequence.StartValue);
        Assert.Equal(1, sequence.IncrementBy);
    }

    [Fact]
    public void TestUseSequenceInstallsGenerator()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());
        IProperty number = ctx.Model.FindEntityType(typeof(Order))!.FindProperty(nameof(Order.Number))!;

        ValueGenerator generator = number.GetValueGeneratorFactory()!(number, number.DeclaringType);

        Assert.IsType<CamusSequenceValueGenerator<long>>(generator);
        Assert.False(generator.GeneratesTemporaryValues);
    }

    [Fact]
    public void TestUseHiLoAddsSequenceWithDefaultBlockSize()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IProperty ticket = model.FindEntityType(typeof(Order))!.FindProperty(nameof(Order.Ticket))!;

        Assert.Equal("ticket_no", ticket.GetHiLoSequenceName());
        Assert.Null(ticket.GetSequenceName());
        Assert.Equal(CamusPropertyBuilderExtensions.DefaultHiLoBlockSize, model.FindSequence("ticket_no")!.IncrementBy);
    }

    [Fact]
    public void TestUseHiLoKeepsIncrementOfConfiguredSequence()
    {
        using HiLoContext ctx = new(Options<HiLoContext>());
        IModel model = ctx.GetService<IDesignTimeModel>().Model;

        Assert.Equal(50, model.FindSequence("configured")!.IncrementBy);
        Assert.Equal(25, model.FindSequence("sized")!.IncrementBy);
    }

    [Fact]
    public void TestCreateTablesFromModelCreatesSequencesFirst()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());

        IReadOnlyList<string> sql = CreateTablesSql(ctx);

        Assert.Equal("CREATE SEQUENCE `order_no` START WITH 1000 INCREMENT BY 1", sql[0]);
        Assert.Equal("CREATE SEQUENCE `ticket_no` START WITH 1 INCREMENT BY 10", sql[1]);

        string createTable = sql[2];
        Assert.StartsWith("CREATE TABLE IF NOT EXISTS `orders`", createTable);
        Assert.Contains("`Number` INT64 NOT NULL DEFAULT (nextval('order_no'))", createTable);
        Assert.Contains("`Ticket` INT64 NOT NULL DEFAULT (nextval('ticket_no'))", createTable);
    }

    [Fact]
    public void TestCreateTableEmitsDefaultValue()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());

        string createTable = CreateTablesSql(ctx)[2];

        Assert.Contains("`Status` STRING NOT NULL DEFAULT ('new')", createTable);
    }

    [Fact]
    public void TestAlterColumnDefaultOnlyNamesTheColumn()
    {
        AlterColumnOperation operation = new()
        {
            Table = "orders",
            Name = "Number",
            ClrType = typeof(long),
            ColumnType = "int64",
            DefaultValueSql = "nextval('order_no')",
            OldColumn = new AddColumnOperation { Table = "orders", Name = "Number", ClrType = typeof(long), ColumnType = "int64" },
        };

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => Generate(operation));
        Assert.Contains("orders.Number", ex.Message);
        Assert.Contains("SET DEFAULT", ex.Message);
    }

    [Fact]
    public void TestValidatorRefusesNonIntegerSequenceProperty()
    {
        using BadTypeContext ctx = new(Options<BadTypeContext>());

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => ctx.Model);
        Assert.Contains("BadOrder.Code", ex.Message);
    }

    [Fact]
    public void TestValidatorRefusesCyclicSequence()
    {
        using CyclicContext ctx = new(Options<CyclicContext>());

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => ctx.Model);
        Assert.Contains("'wraps'", ex.Message);
    }

    [Fact]
    public void TestUseSequenceRejectsBacktickInName()
    {
        ModelBuilder builder = new();

        Assert.Throws<ArgumentException>(() => builder.Entity<Order>().Property(e => e.Number).UseSequence("order` x"));
    }

    [Fact]
    public void TestUseHiLoRejectsBlockSizeBelowOne()
    {
        ModelBuilder builder = new();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Entity<Order>().Property(e => e.Ticket).UseHiLo("s", 0));
    }

    [Fact]
    public void TestAnnotationCodeGeneratorRendersUseSequenceAndUseHiLo()
    {
        using OrdersContext ctx = new(Options<OrdersContext>());
        IEntityType order = ctx.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(Order))!;

#pragma warning disable EF1001 // The dependencies type is marked internal; the design-time host builds it the same way.
        CamusAnnotationCodeGenerator generator = new(
            new AnnotationCodeGeneratorDependencies(ctx.GetService<IRelationalTypeMappingSource>()));
#pragma warning restore EF1001

        IProperty number = order.FindProperty(nameof(Order.Number))!;
        Dictionary<string, IAnnotation> numberAnnotations = number.GetAnnotations().ToDictionary(a => a.Name, a => a);
        MethodCallCodeFragment useSequence = Assert.Single(
            generator.GenerateFluentApiCalls(number, numberAnnotations),
            f => f.Method == nameof(CamusPropertyBuilderExtensions.UseSequence));
        Assert.Equal("order_no", Assert.Single(useSequence.Arguments));

        IProperty ticket = order.FindProperty(nameof(Order.Ticket))!;
        Dictionary<string, IAnnotation> ticketAnnotations = ticket.GetAnnotations().ToDictionary(a => a.Name, a => a);
        MethodCallCodeFragment useHiLo = Assert.Single(
            generator.GenerateFluentApiCalls(ticket, ticketAnnotations),
            f => f.Method == nameof(CamusPropertyBuilderExtensions.UseHiLo));
        Assert.Equal("ticket_no", useHiLo.Arguments[0]);
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
        using OrdersContext ctx = new(Options<OrdersContext>());

        return ctx.GetService<IMigrationsSqlGenerator>()
            .Generate([operation], null)
            .Select(c => c.CommandText.Trim())
            .ToList();
    }

    public class Order
    {
        public string Id { get; set; } = "";

        public long Number { get; set; }

        public int Ticket { get; set; }

        public string Status { get; set; } = "";
    }

    public class BadOrder
    {
        public string Id { get; set; } = "";

        public string Code { get; set; } = "";
    }

    private class OrdersContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasSequence("order_no").StartsAt(1000);

            modelBuilder.Entity<Order>(b =>
            {
                b.ToTable("orders");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Number).UseSequence("order_no");
                b.Property(e => e.Ticket).UseHiLo("ticket_no");
                b.Property(e => e.Status).HasDefaultValue("new");
            });
        }
    }

    private class HiLoContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasSequence("configured").IncrementsBy(50);

            modelBuilder.Entity<Order>(b =>
            {
                b.ToTable("orders");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Number).UseHiLo("configured");
                b.Property(e => e.Ticket).UseHiLo("sized", blockSize: 25);
            });
        }
    }

    private class BadTypeContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<BadOrder> Orders => Set<BadOrder>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BadOrder>(b =>
            {
                b.ToTable("bad_orders");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Code).UseSequence("codes");
            });
        }
    }

    private class CyclicContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasSequence("wraps").IsCyclic();

            modelBuilder.Entity<Order>(b =>
            {
                b.ToTable("orders");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Number).UseSequence("wraps");
            });
        }
    }
}
