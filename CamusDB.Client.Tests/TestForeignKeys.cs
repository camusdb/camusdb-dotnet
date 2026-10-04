/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CamusDB.Client.Tests;

/// <summary>
/// The foreign-key DDL that migrations and <c>EnsureCreated</c> compose. These tests read the composed
/// text only, so no server is needed. <see cref="TestForeignKeysLive"/> runs the same DDL on a server.
/// </summary>
public class TestForeignKeys
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static TContext NewContext<TContext>(bool foreignKeys = true) where TContext : DbContext
    {
        DbContextOptions<TContext> options = new DbContextOptionsBuilder<TContext>()
            .UseCamusDB(ConnString, o => o.UseForeignKeyConstraints(foreignKeys)).Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    private static IReadOnlyList<string> Sql(DbContext ctx, params MigrationOperation[] operations)
        => ctx.GetService<IMigrationsSqlGenerator>().Generate(operations, null).Select(c => c.CommandText.Trim()).ToList();

    /// <summary>The commands a first migration of the model of <paramref name="ctx"/> produces.</summary>
    private static IReadOnlyList<string> InitialMigrationSql(DbContext ctx)
    {
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IReadOnlyList<MigrationOperation> operations =
            ctx.GetService<IMigrationsModelDiffer>().GetDifferences(null, model.GetRelationalModel());

        return ctx.GetService<IMigrationsSqlGenerator>().Generate(operations, model).Select(c => c.CommandText.Trim()).ToList();
    }

    private static AddForeignKeyOperation OrderCustomerForeignKey(ReferentialAction onDelete = ReferentialAction.NoAction) => new()
    {
        Name = "FK_orders_customers_CustomerId",
        Table = "orders",
        Columns = ["CustomerId"],
        PrincipalTable = "customers",
        PrincipalColumns = ["Id"],
        OnDelete = onDelete,
    };

    private static CreateTableOperation OrdersTable(params AddForeignKeyOperation[] foreignKeys)
    {
        CreateTableOperation table = new()
        {
            Name = "orders",
            Columns =
            {
                new AddColumnOperation { Name = "Id", Table = "orders", ClrType = typeof(long), ColumnType = "int64" },
                new AddColumnOperation { Name = "CustomerId", Table = "orders", ClrType = typeof(long), ColumnType = "int64" },
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] },
        };

        foreach (AddForeignKeyOperation foreignKey in foreignKeys)
            table.ForeignKeys.Add(foreignKey);

        return table;
    }

    [Fact]
    public void TestCreateTableEmitsTheForeignKey()
    {
        using var ctx = NewContext<ShopContext>();

        string sql = Assert.Single(Sql(ctx, OrdersTable(OrderCustomerForeignKey())));

        Assert.Contains(
            "CONSTRAINT `FK_orders_customers_CustomerId` FOREIGN KEY (`CustomerId`) REFERENCES `customers` (`Id`)",
            sql);
        Assert.DoesNotContain("ON DELETE", sql);
        Assert.DoesNotContain("ON UPDATE", sql);
    }

    [Fact]
    public void TestRestrictIsRendered()
    {
        using var ctx = NewContext<ShopContext>();
        AddForeignKeyOperation foreignKey = OrderCustomerForeignKey(ReferentialAction.Restrict);
        foreignKey.OnUpdate = ReferentialAction.Restrict;

        string sql = Assert.Single(Sql(ctx, OrdersTable(foreignKey)));

        Assert.Contains("REFERENCES `customers` (`Id`) ON DELETE RESTRICT ON UPDATE RESTRICT", sql);
    }

    /// <summary>
    /// The server refuses CASCADE, SET NULL and SET DEFAULT. EF Core gives a required relationship
    /// Cascade by default, so the provider renders those actions as NO ACTION.
    /// </summary>
    [Theory]
    [InlineData(ReferentialAction.Cascade)]
    [InlineData(ReferentialAction.SetNull)]
    [InlineData(ReferentialAction.SetDefault)]
    public void TestUnsupportedActionsRenderAsNoAction(ReferentialAction action)
    {
        using var ctx = NewContext<ShopContext>();
        AddForeignKeyOperation foreignKey = OrderCustomerForeignKey(action);
        foreignKey.OnUpdate = action;

        string sql = Assert.Single(Sql(ctx, OrdersTable(foreignKey)));

        Assert.Contains("REFERENCES `customers` (`Id`)", sql);
        Assert.DoesNotContain("ON DELETE", sql);
        Assert.DoesNotContain("ON UPDATE", sql);
        Assert.DoesNotContain("CASCADE", sql);
    }

    [Fact]
    public void TestNoPrincipalColumnsReferencesThePrimaryKey()
    {
        using var ctx = NewContext<ShopContext>();
        AddForeignKeyOperation foreignKey = OrderCustomerForeignKey();
        foreignKey.PrincipalColumns = null;

        string sql = Assert.Single(Sql(ctx, foreignKey));

        Assert.Equal(
            "ALTER TABLE `orders` ADD CONSTRAINT `FK_orders_customers_CustomerId` FOREIGN KEY (`CustomerId`) REFERENCES `customers`",
            sql);
    }

    [Fact]
    public void TestAddAndDropForeignKey()
    {
        using var ctx = NewContext<ShopContext>();

        IReadOnlyList<string> sql = Sql(
            ctx,
            OrderCustomerForeignKey(ReferentialAction.Restrict),
            new DropForeignKeyOperation { Name = "FK_orders_customers_CustomerId", Table = "orders" });

        Assert.Equal(
            [
                "ALTER TABLE `orders` ADD CONSTRAINT `FK_orders_customers_CustomerId` FOREIGN KEY (`CustomerId`) " +
                "REFERENCES `customers` (`Id`) ON DELETE RESTRICT",
                "ALTER TABLE `orders` DROP CONSTRAINT `FK_orders_customers_CustomerId`",
            ],
            sql);
    }

    [Fact]
    public void TestCompositeForeignKey()
    {
        using var ctx = NewContext<ShopContext>();
        AddForeignKeyOperation foreignKey = new()
        {
            Name = "stores_region_fk",
            Table = "stores",
            Columns = ["country", "region"],
            PrincipalTable = "regions",
            PrincipalColumns = ["country", "code"],
        };

        string sql = Assert.Single(Sql(ctx, foreignKey));

        Assert.Equal(
            "ALTER TABLE `stores` ADD CONSTRAINT `stores_region_fk` FOREIGN KEY (`country`, `region`) " +
            "REFERENCES `regions` (`country`, `code`)",
            sql);
    }

    /// <summary>
    /// The index EF Core declares on the referencing column goes into the CREATE TABLE, so the server
    /// reuses it for the foreign key instead of a second index of its own.
    /// </summary>
    [Fact]
    public void TestIndexOfANewChildTableIsFoldedIntoCreateTable()
    {
        using var ctx = NewContext<ShopContext>();
        CreateIndexOperation index = new() { Name = "IX_orders_CustomerId", Table = "orders", Columns = ["CustomerId"] };
        index.AddAnnotation(CamusAnnotationNames.IndexComment, "by customer");

        string sql = Assert.Single(Sql(ctx, OrdersTable(OrderCustomerForeignKey()), index));

        Assert.Contains("KEY `IX_orders_CustomerId` (`CustomerId`) COMMENT 'by customer'", sql);
        Assert.True(
            sql.IndexOf("KEY `IX_orders_CustomerId`", StringComparison.Ordinal)
            < sql.IndexOf("CONSTRAINT `FK_orders_customers_CustomerId`", StringComparison.Ordinal));
    }

    [Fact]
    public void TestIndexOfATableWithoutForeignKeysStaysSeparate()
    {
        using var ctx = NewContext<ShopContext>();
        CreateIndexOperation index = new() { Name = "IX_orders_CustomerId", Table = "orders", Columns = ["CustomerId"] };

        IReadOnlyList<string> sql = Sql(ctx, OrdersTable(), index);

        Assert.Equal(2, sql.Count);
        Assert.DoesNotContain("KEY `IX_orders_CustomerId`", sql[0]);
        Assert.Equal("CREATE INDEX IF NOT EXISTS `IX_orders_CustomerId` ON `orders` (`CustomerId`)", sql[1]);
    }

    [Fact]
    public void TestDisabledForeignKeysEmitNothing()
    {
        using var ctx = NewContext<ShopContext>(foreignKeys: false);
        CreateIndexOperation index = new() { Name = "IX_orders_CustomerId", Table = "orders", Columns = ["CustomerId"] };

        IReadOnlyList<string> sql = Sql(
            ctx,
            OrdersTable(OrderCustomerForeignKey()),
            index,
            OrderCustomerForeignKey(),
            new DropForeignKeyOperation { Name = "FK_orders_customers_CustomerId", Table = "orders" });

        Assert.Equal(2, sql.Count);
        Assert.DoesNotContain("FOREIGN KEY", sql[0]);
        Assert.StartsWith("CREATE INDEX", sql[1]);
    }

    [Fact]
    public void TestUniqueConstraints()
    {
        using var ctx = NewContext<ShopContext>();
        CreateTableOperation table = OrdersTable();
        table.UniqueConstraints.Add(new AddUniqueConstraintOperation { Name = "AK_orders_CustomerId", Columns = ["CustomerId"] });

        IReadOnlyList<string> sql = Sql(
            ctx,
            table,
            new AddUniqueConstraintOperation { Name = "AK_orders_Code", Table = "orders", Columns = ["Code"] },
            new DropUniqueConstraintOperation { Name = "AK_orders_Code", Table = "orders" });

        Assert.Contains("UNIQUE KEY `AK_orders_CustomerId` (`CustomerId`)", sql[0]);
        Assert.Equal("CREATE UNIQUE INDEX `AK_orders_Code` ON `orders` (`Code`)", sql[1]);
        Assert.Equal("ALTER TABLE `orders` DROP INDEX `AK_orders_Code`", sql[2]);
    }

    /// <summary>
    /// A first migration of a model with relationships: principal tables first, the foreign keys inline,
    /// the IX_ index of each foreign key folded into its table, and the alternate key as a unique key.
    /// </summary>
    [Fact]
    public void TestInitialMigrationOfAModel()
    {
        using var ctx = NewContext<ShopContext>();

        IReadOnlyList<string> sql = InitialMigrationSql(ctx);

        Assert.Equal(3, sql.Count);
        Assert.DoesNotContain(sql, s => s.StartsWith("CREATE INDEX", StringComparison.Ordinal));

        int customers = FindIndex(sql, "CREATE TABLE IF NOT EXISTS `fk_customers`");
        int orders = FindIndex(sql, "CREATE TABLE IF NOT EXISTS `fk_orders`");
        int employees = FindIndex(sql, "CREATE TABLE IF NOT EXISTS `fk_employees`");
        Assert.True(customers < orders);

        Assert.Contains("UNIQUE KEY `AK_fk_customers_Email` (`Email`)", sql[customers]);
        Assert.Contains("KEY `IX_fk_orders_CustomerId` (`CustomerId`)", sql[orders]);
        Assert.Contains("KEY `IX_fk_orders_CustomerEmail` (`CustomerEmail`)", sql[orders]);
        Assert.Contains(
            "CONSTRAINT `FK_fk_orders_fk_customers_CustomerId` FOREIGN KEY (`CustomerId`) REFERENCES `fk_customers` (`Id`)",
            sql[orders]);
        Assert.Contains(
            "CONSTRAINT `FK_fk_orders_fk_customers_CustomerEmail` FOREIGN KEY (`CustomerEmail`) REFERENCES `fk_customers` (`Email`) ON DELETE RESTRICT",
            sql[orders]);
        Assert.Contains("KEY `IX_fk_employees_ManagerId` (`ManagerId`)", sql[employees]);
        Assert.Contains("REFERENCES `fk_employees` (`Id`)", sql[employees]);
    }

    private static int FindIndex(IReadOnlyList<string> sql, string prefix)
    {
        for (int i = 0; i < sql.Count; i++)
        {
            if (sql[i].StartsWith(prefix, StringComparison.Ordinal))
                return i;
        }

        Assert.Fail($"No command starts with '{prefix}'.");
        return -1;
    }

    [Fact]
    public void TestEnsureCreatedDdl()
    {
        using var ctx = NewContext<ShopContext>();
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IEntityType orders = model.FindEntityType(typeof(FkOrder))!;
        IEntityType customers = model.FindEntityType(typeof(FkCustomer))!;

        string orderSql = CamusDatabaseCreator.BuildCreateTableSql(orders, "fk_orders");
        string customerSql = CamusDatabaseCreator.BuildCreateTableSql(customers, "fk_customers");

        Assert.Contains(
            ", CONSTRAINT `FK_fk_orders_fk_customers_CustomerId` FOREIGN KEY (`CustomerId`) REFERENCES `fk_customers` (`Id`)",
            orderSql);
        Assert.Contains("REFERENCES `fk_customers` (`Email`) ON DELETE RESTRICT", orderSql);
        Assert.Contains(", UNIQUE KEY `AK_fk_customers_Email` (`Email`)", customerSql);
        Assert.DoesNotContain("FOREIGN KEY", customerSql);

        string withoutForeignKeys = CamusDatabaseCreator.BuildCreateTableSql(orders, "fk_orders", foreignKeys: false);
        Assert.DoesNotContain("FOREIGN KEY", withoutForeignKeys);
    }

    [Fact]
    public void TestEnsureCreatedCreatesPrincipalsFirst()
    {
        using var ctx = NewContext<ShopContext>();
        IModel model = ctx.GetService<IDesignTimeModel>().Model;

        // The model order puts the child first; the creation order must not.
        IEntityType[] entityTypes = model.GetEntityTypes().OrderByDescending(e => e.GetTableName()).ToArray();
        Assert.Equal("fk_orders", entityTypes[0].GetTableName());

        List<string?> order = CamusDatabaseCreator.OrderForCreation(entityTypes, foreignKeys: true)
            .Select(e => e.GetTableName()).ToList();

        Assert.True(order.IndexOf("fk_customers") < order.IndexOf("fk_orders"));
        Assert.Equal(3, order.Count);
    }

    [Fact]
    public void TestEnsureCreatedRefusesACycle()
    {
        using var ctx = NewContext<CycleContext>();
        IModel model = ctx.GetService<IDesignTimeModel>().Model;

        NotSupportedException ex = Assert.Throws<NotSupportedException>(
            () => CamusDatabaseCreator.OrderForCreation(model.GetEntityTypes(), foreignKeys: true));

        Assert.Contains("'fk_cycle_a'", ex.Message);
        Assert.Contains("'fk_cycle_b'", ex.Message);
        Assert.Contains("UseForeignKeyConstraints(false)", ex.Message);

        Assert.Equal(2, CamusDatabaseCreator.OrderForCreation(model.GetEntityTypes(), foreignKeys: false).Count);
    }

    [Fact]
    public void TestOptionsDefaultToEnabled()
    {
        DbContextOptions options = new DbContextOptionsBuilder().UseCamusDB(ConnString).Options;
        Assert.True(options.FindExtension<CamusDBOptionsExtension>()!.ForeignKeyConstraintsEnabled);

        options = new DbContextOptionsBuilder().UseCamusDB(ConnString, o => o.UseForeignKeyConstraints(false)).Options;
        Assert.False(options.FindExtension<CamusDBOptionsExtension>()!.ForeignKeyConstraintsEnabled);
    }

    public class FkCustomer
    {
        public long Id { get; set; }

        public string Email { get; set; } = "";

        public List<FkOrder> Orders { get; set; } = [];
    }

    public class FkOrder
    {
        public long Id { get; set; }

        public long CustomerId { get; set; }

        public string? CustomerEmail { get; set; }
    }

    public class FkEmployee
    {
        public long Id { get; set; }

        public long? ManagerId { get; set; }

        public FkEmployee? Manager { get; set; }
    }

    public class ShopContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FkCustomer>(b =>
            {
                b.ToTable("fk_customers");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.Email).HasMaxLength(64);
                b.HasMany(e => e.Orders).WithOne().HasForeignKey(o => o.CustomerId);
            });

            modelBuilder.Entity<FkOrder>(b =>
            {
                b.ToTable("fk_orders");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.CustomerEmail).HasMaxLength(64);
                b.HasOne<FkCustomer>().WithMany().HasForeignKey(o => o.CustomerEmail)
                 .HasPrincipalKey(c => c.Email).OnDelete(DeleteBehavior.Restrict);
            });

            modelBuilder.Entity<FkEmployee>(b =>
            {
                b.ToTable("fk_employees");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.HasOne(e => e.Manager).WithMany().HasForeignKey(e => e.ManagerId);
            });
        }
    }

    public class CycleA
    {
        public long Id { get; set; }

        public long? BId { get; set; }
    }

    public class CycleB
    {
        public long Id { get; set; }

        public long? AId { get; set; }
    }

    public class CycleContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CycleA>(b =>
            {
                b.ToTable("fk_cycle_a");
                b.HasOne<CycleB>().WithMany().HasForeignKey(e => e.BId);
            });

            modelBuilder.Entity<CycleB>(b =>
            {
                b.ToTable("fk_cycle_b");
                b.HasOne<CycleA>().WithMany().HasForeignKey(e => e.AId);
            });
        }
    }
}
