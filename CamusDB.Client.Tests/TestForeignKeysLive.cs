/**
 * This file is part of CamusDB
 *
 * Live coverage for foreign keys: the DDL of EnsureCreated and of a first migration runs on a server,
 * and the server enforces it under SaveChanges. Each test works in a database of its own, which it
 * drops at the end. TestForeignKeys pins the DDL text.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.Client.Tests;

public class TestForeignKeysLive
{
    private static LiveContext NewContext(string database, bool foreignKeys = true)
    {
        DbContextOptions<LiveContext> options = new DbContextOptionsBuilder<LiveContext>()
            .UseCamusDB($"Endpoint=http://localhost:5095;Database={database}", o => o.UseForeignKeyConstraints(foreignKeys))
            .Options;

        return new LiveContext(options);
    }

    private static string NewDatabaseName() => "fk" + Guid.NewGuid().ToString("n")[..12];

    private static async Task<string> CreatedDatabaseAsync(bool foreignKeys = true)
    {
        string database = NewDatabaseName();
        await using LiveContext ctx = NewContext(database, foreignKeys);
        await ctx.Database.EnsureCreatedAsync();
        return database;
    }

    private static async Task DropAsync(string database)
    {
        await using LiveContext ctx = NewContext(database);
        await ctx.Database.EnsureDeletedAsync();
    }

    private static string CodeOf(DbUpdateException ex)
        => Assert.IsType<CamusException>(ex.InnerException).Code;

    [Fact]
    public async Task TestEnsureCreatedEnforcesTheChildSide()
    {
        string database = await CreatedDatabaseAsync();
        try
        {
            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Customers.Add(new LiveCustomer { Id = 1, Email = "a@example.com" });
                ctx.Orders.Add(new LiveOrder { Id = 10, CustomerId = 1, CustomerEmail = "a@example.com" });
                ctx.Orders.Add(new LiveOrder { Id = 11, CustomerId = 1, CustomerEmail = null });
                await ctx.SaveChangesAsync();
            }

            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Orders.Add(new LiveOrder { Id = 12, CustomerId = 999 });
                DbUpdateException ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
                Assert.Equal("CADB0304", CodeOf(ex));
            }

            // The alternate key is a unique index, so the server accepts a reference to it.
            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Orders.Add(new LiveOrder { Id = 13, CustomerId = 1, CustomerEmail = "nobody@example.com" });
                DbUpdateException ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
                Assert.Equal("CADB0304", CodeOf(ex));
            }
        }
        finally
        {
            await DropAsync(database);
        }
    }

    /// <summary>
    /// The relationship is required, so EF Core gives it Cascade. The server runs NO ACTION: a delete of
    /// a customer whose orders EF does not track fails, and a delete of one whose orders EF tracks
    /// succeeds, because EF deletes the orders first.
    /// </summary>
    [Fact]
    public async Task TestParentDeleteWithCascadeBehavior()
    {
        string database = await CreatedDatabaseAsync();
        try
        {
            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Customers.Add(new LiveCustomer { Id = 1, Email = "a@example.com" });
                ctx.Orders.Add(new LiveOrder { Id = 10, CustomerId = 1 });
                await ctx.SaveChangesAsync();
            }

            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Customers.Remove(new LiveCustomer { Id = 1, Email = "a@example.com" });
                DbUpdateException ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
                Assert.Equal("CADB0305", CodeOf(ex));
            }

            await using (LiveContext ctx = NewContext(database))
            {
                LiveCustomer customer = await ctx.Customers.SingleAsync(c => c.Id == 1);
                await ctx.Orders.Where(o => o.CustomerId == 1).LoadAsync();
                Assert.Single(customer.Orders);

                ctx.Customers.Remove(customer);
                await ctx.SaveChangesAsync();
            }

            await using (LiveContext ctx = NewContext(database))
            {
                Assert.Equal(0, await ctx.Customers.CountAsync());
                Assert.Equal(0, await ctx.Orders.CountAsync());
            }
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [Fact]
    public async Task TestSelfReference()
    {
        string database = await CreatedDatabaseAsync();
        try
        {
            await using (LiveContext ctx = NewContext(database))
            {
                LiveEmployee boss = new() { Id = 1 };
                ctx.Employees.Add(boss);
                ctx.Employees.Add(new LiveEmployee { Id = 2, Manager = boss });
                await ctx.SaveChangesAsync();
            }

            await using (LiveContext ctx = NewContext(database))
            {
                ctx.Employees.Add(new LiveEmployee { Id = 3, ManagerId = 42 });
                DbUpdateException ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
                Assert.Equal("CADB0304", CodeOf(ex));
            }
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [Fact]
    public async Task TestDisabledForeignKeysKeepTheOldBehavior()
    {
        string database = await CreatedDatabaseAsync(foreignKeys: false);
        try
        {
            await using LiveContext ctx = NewContext(database, foreignKeys: false);
            ctx.Orders.Add(new LiveOrder { Id = 12, CustomerId = 999 });
            await ctx.SaveChangesAsync();
        }
        finally
        {
            await DropAsync(database);
        }
    }

    /// <summary>
    /// A first migration runs on the server. The server reuses the IX_ index of each foreign key, which
    /// the provider folds into the CREATE TABLE, so it creates no <c>~fk_</c> index of its own.
    /// </summary>
    [Fact]
    public async Task TestInitialMigrationRunsAndReusesTheIndexes()
    {
        string database = NewDatabaseName();
        await using LiveContext ctx = NewContext(database);
        await ctx.GetService<IRelationalDatabaseCreator>().CreateAsync();
        try
        {
            await RunInitialMigrationAsync(ctx);

            ctx.Orders.Add(new LiveOrder { Id = 12, CustomerId = 999 });
            DbUpdateException ex = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
            Assert.Equal("CADB0304", CodeOf(ex));

            List<string> indexes = await ShowIndexNamesAsync(database, "live_orders");
            Assert.Contains("IX_live_orders_CustomerId", indexes);
            Assert.DoesNotContain(indexes, n => n.StartsWith("~fk_", StringComparison.Ordinal));
        }
        finally
        {
            await ctx.Database.EnsureDeletedAsync();
        }
    }

    /// <summary>ADD CONSTRAINT reads the existing rows and refuses an orphan.</summary>
    [Fact]
    public async Task TestAddForeignKeyValidatesExistingRows()
    {
        string database = await CreatedDatabaseAsync(foreignKeys: false);
        try
        {
            await using LiveContext ctx = NewContext(database);
            ctx.Orders.Add(new LiveOrder { Id = 12, CustomerId = 999 });
            await ctx.SaveChangesAsync();

            IReadOnlyList<MigrationCommand> commands = ctx.GetService<IMigrationsSqlGenerator>().Generate(
            [
                new Microsoft.EntityFrameworkCore.Migrations.Operations.AddForeignKeyOperation
                {
                    Name = "FK_live_orders_live_customers_CustomerId",
                    Table = "live_orders",
                    Columns = ["CustomerId"],
                    PrincipalTable = "live_customers",
                    PrincipalColumns = ["Id"],
                },
            ]);

            CamusException ex = await Assert.ThrowsAsync<CamusException>(() => ExecuteAsync(ctx, commands));
            Assert.Equal("CADB0304", ex.Code);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    private static async Task RunInitialMigrationAsync(DbContext ctx)
    {
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        var operations = ctx.GetService<IMigrationsModelDiffer>().GetDifferences(null, model.GetRelationalModel());
        IReadOnlyList<MigrationCommand> commands = ctx.GetService<IMigrationsSqlGenerator>().Generate(operations, model);
        await ExecuteAsync(ctx, commands);
    }

    private static Task ExecuteAsync(DbContext ctx, IReadOnlyList<MigrationCommand> commands)
        => ctx.GetService<IMigrationCommandExecutor>().ExecuteNonQueryAsync(commands, ctx.GetService<IRelationalConnection>());

    private static async Task<List<string>> ShowIndexNamesAsync(string database, string table)
    {
        await using CamusConnection connection = new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:5095;Database={database}"));
        await connection.OpenAsync();

        await using CamusCommand command = connection.CreateCamusCommand($"SHOW INDEXES FROM {table}");
        await using var reader = await command.ExecuteReaderAsync();

        int nameOrdinal = reader.GetOrdinal("Key_name");

        List<string> names = [];
        while (await reader.ReadAsync())
            names.Add(reader.GetString(nameOrdinal));

        return names;
    }

    public class LiveCustomer
    {
        public long Id { get; set; }

        public string Email { get; set; } = "";

        public List<LiveOrder> Orders { get; set; } = [];
    }

    public class LiveOrder
    {
        public long Id { get; set; }

        public long CustomerId { get; set; }

        public string? CustomerEmail { get; set; }
    }

    public class LiveEmployee
    {
        public long Id { get; set; }

        public long? ManagerId { get; set; }

        public LiveEmployee? Manager { get; set; }
    }

    public class LiveContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<LiveCustomer> Customers => Set<LiveCustomer>();

        public DbSet<LiveOrder> Orders => Set<LiveOrder>();

        public DbSet<LiveEmployee> Employees => Set<LiveEmployee>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LiveCustomer>(b =>
            {
                b.ToTable("live_customers");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.Email).HasMaxLength(64);
                b.HasMany(e => e.Orders).WithOne().HasForeignKey(o => o.CustomerId);
            });

            modelBuilder.Entity<LiveOrder>(b =>
            {
                b.ToTable("live_orders");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.Property(e => e.CustomerEmail).HasMaxLength(64);
                b.HasOne<LiveCustomer>().WithMany().HasForeignKey(o => o.CustomerEmail)
                 .HasPrincipalKey(c => c.Email).OnDelete(DeleteBehavior.Restrict);
            });

            modelBuilder.Entity<LiveEmployee>(b =>
            {
                b.ToTable("live_employees");
                b.Property(e => e.Id).ValueGeneratedNever();
                b.HasOne(e => e.Manager).WithMany().HasForeignKey(e => e.ManagerId);
            });
        }
    }
}
