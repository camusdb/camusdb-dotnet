/**
 * This file is part of CamusDB
 *
 * End-to-end coverage of the database existence check behind MigrateAsync and EnsureCreated.
 *
 * The tests that need a grant-scoped account are opt-in, like TestAuthenticationLive: they no-op
 * unless CAMUSDB_TEST_USER / CAMUSDB_TEST_PASSWORD name the bootstrap superuser of a server that runs
 * with CAMUSDB_AUTH_ENABLED=true. The others run against the shared unauthenticated instance, or as
 * that superuser when the variables are set.
 *
 * To run the grant-scoped tests:
 *   export CAMUSDB_TEST_USER=admin CAMUSDB_TEST_PASSWORD='…'
 *   export CAMUSDB_TEST_ENDPOINT=http://localhost:5195   # optional; default http://localhost:5095
 *   dotnet test --filter FullyQualifiedName~TestEntityFrameworkMigrateLive
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.Client.Tests;

/// <summary>
/// <c>MigrateAsync</c> asks the provider whether the database exists, and sends <c>CREATE DATABASE</c>
/// when the answer is no. That statement is for a superuser only, so the answer must be true for an
/// existing database, or an account that holds only DDL grants on it cannot migrate.
/// </summary>
public class TestEntityFrameworkMigrateLive
{
    // CAMUSDB_TEST_ENDPOINT points the tests at a second server, so an authenticated one can run next
    // to the shared unauthenticated instance.
    private static readonly string Endpoint =
        Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") is { Length: > 0 } endpoint
            ? endpoint
            : AuthenticatedDatabaseFixture.Endpoint;

    private const string MigratorPassword = "Migrator-pw-123456";

    private static bool Configured => AuthenticatedDatabaseFixture.Configured;

    /// <summary>The superuser when the credentials are set, otherwise an unauthenticated caller.</summary>
    private static string AdminConnectionString(string database) => Configured
        ? $"Endpoint={Endpoint};Database={database};User={AuthenticatedDatabaseFixture.User};Password={AuthenticatedDatabaseFixture.Password}"
        : $"Endpoint={Endpoint};Database={database}";

    private static string MigratorConnectionString(string database, string user)
        => $"Endpoint={Endpoint};Database={database};User={user};Password={MigratorPassword}";

    private static string UniqueName(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..12];

    [Fact]
    public async Task TestExistsAnswersFromTheServer()
    {
        string database = UniqueName("efex");

        await using (MigrateContext missing = new(AdminConnectionString(database)))
        {
            IRelationalDatabaseCreator creator = missing.GetService<IRelationalDatabaseCreator>();
            Assert.False(await creator.ExistsAsync());
            Assert.False(creator.Exists());
        }

        await using CamusConnection admin = await OpenAsync(AdminConnectionString(database));
        await admin.CreateDatabaseAsync(ifNotExists: true);

        try
        {
            await using MigrateContext existing = new(AdminConnectionString(database));
            IRelationalDatabaseCreator creator = existing.GetService<IRelationalDatabaseCreator>();
            Assert.True(await creator.ExistsAsync());
            Assert.True(creator.Exists());
        }
        finally
        {
            await admin.DropDatabaseAsync();
        }
    }

    /// <summary>A superuser (or a server without authentication) still gets a missing database created.</summary>
    [Fact]
    public async Task TestMigrateCreatesAMissingDatabase()
    {
        string database = UniqueName("efmig");

        try
        {
            await using (MigrateContext ctx = new(AdminConnectionString(database)))
                await ctx.Database.MigrateAsync();

            await AssertMigratedAsync(AdminConnectionString(database));
        }
        finally
        {
            await DropDatabaseAsync(database);
        }
    }

    /// <summary>
    /// The case behind this check: an account with <c>SELECT, INSERT, CREATE TABLE, ALTER, INDEX</c> on
    /// an existing database applies a migration, and a second run is a no-op.
    /// </summary>
    [Fact]
    public async Task TestGrantScopedAccountMigratesAnExistingDatabase()
    {
        if (!Configured)
            return;

        string database = UniqueName("efgrant");
        string user = UniqueName("efm");

        await using CamusConnection admin = await OpenAsync(AdminConnectionString(database));
        await admin.CreateDatabaseAsync(ifNotExists: true);

        try
        {
            await CreateMigratorAsync(admin, user, database);

            await using (MigrateContext ctx = new(MigratorConnectionString(database, user)))
                await ctx.Database.MigrateAsync();

            await using (MigrateContext ctx = new(MigratorConnectionString(database, user)))
            {
                Assert.Empty(await ctx.Database.GetPendingMigrationsAsync());
                await ctx.Database.MigrateAsync();
            }

            await AssertMigratedAsync(MigratorConnectionString(database, user));
        }
        finally
        {
            await DropUserAsync(admin, user);
            await admin.DropDatabaseAsync();
        }
    }

    /// <summary>
    /// The account cannot create a database, and the refusal must surface as one: not as a silent
    /// success, and not as a different error.
    /// </summary>
    [Fact]
    public async Task TestGrantScopedAccountCannotCreateAMissingDatabase()
    {
        if (!Configured)
            return;

        string granted = UniqueName("efgrant");
        string missing = UniqueName("efmissing");
        string user = UniqueName("efm");

        await using CamusConnection admin = await OpenAsync(AdminConnectionString(granted));
        await admin.CreateDatabaseAsync(ifNotExists: true);

        try
        {
            await CreateMigratorAsync(admin, user, granted);

            await using MigrateContext ctx = new(MigratorConnectionString(missing, user));
            CamusException ex = await Assert.ThrowsAsync<CamusException>(() => ctx.Database.MigrateAsync());

            Assert.Equal("CADB0517", ex.Code);
        }
        finally
        {
            await DropUserAsync(admin, user);
            await admin.DropDatabaseAsync();
        }
    }

    [Fact]
    public async Task TestGrantScopedAccountEnsureCreatesTables()
    {
        if (!Configured)
            return;

        string database = UniqueName("efgrant");
        string user = UniqueName("efm");

        await using CamusConnection admin = await OpenAsync(AdminConnectionString(database));
        await admin.CreateDatabaseAsync(ifNotExists: true);

        try
        {
            await CreateMigratorAsync(admin, user, database);

            await using (MigrateContext ctx = new(MigratorConnectionString(database, user)))
            {
                await ctx.Database.EnsureCreatedAsync();

                // The tables are there now; a second call must not fail on them.
                await ctx.Database.EnsureCreatedAsync();
            }

            await using CamusConnection migrator = await OpenAsync(MigratorConnectionString(database, user));
            await using CamusCommand select = migrator.CreateCamusCommand("SELECT * FROM `ef_mig_things`");
            await using CamusDataReader reader = await select.ExecuteReaderAsync();
            Assert.False(await reader.ReadAsync());
        }
        finally
        {
            await DropUserAsync(admin, user);
            await admin.DropDatabaseAsync();
        }
    }

    private static async Task<CamusConnection> OpenAsync(string connectionString)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(connectionString));
        await connection.OpenAsync();
        return connection;
    }

    private static async Task CreateMigratorAsync(CamusConnection admin, string user, string database)
    {
        await using (CamusCommand create = admin.CreateCamusCommand($"CREATE USER {user} IDENTIFIED BY @password"))
        {
            create.Parameters.Add("@password", ColumnType.String, MigratorPassword);
            await create.ExecuteNonQueryAsync();
        }

        await using CamusCommand grant = admin.CreateCamusCommand(
            $"GRANT SELECT, INSERT, CREATE TABLE, ALTER, INDEX ON {database}.* TO {user}");
        await grant.ExecuteNonQueryAsync();
    }

    private static async Task DropUserAsync(CamusConnection admin, string user)
    {
        await using CamusCommand drop = admin.CreateCamusCommand($"DROP USER IF EXISTS {user}");
        await drop.ExecuteNonQueryAsync();
    }

    private static async Task DropDatabaseAsync(string database)
    {
        await using CamusConnection admin = await OpenAsync(AdminConnectionString(database));
        try
        {
            await admin.DropDatabaseAsync();
        }
        catch (CamusException ex) when (ex.Code == "CADB0010")
        {
            // The test failed before the database was created.
        }
    }

    /// <summary>The migration's table exists, and the history holds exactly its one row.</summary>
    private static async Task AssertMigratedAsync(string connectionString)
    {
        await using CamusConnection connection = await OpenAsync(connectionString);

        await using (CamusCommand things = connection.CreateCamusCommand("SELECT * FROM `ef_mig_things`"))
        await using (CamusDataReader reader = await things.ExecuteReaderAsync())
            Assert.False(await reader.ReadAsync());

        List<string> applied = [];
        await using (CamusCommand history = connection.CreateCamusCommand(
            $"SELECT `MigrationId` FROM `{HistoryRepository.DefaultTableName}`"))
        await using (CamusDataReader reader = await history.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                applied.Add(reader.GetString(0));
        }

        Assert.Equal([CreateThingsMigration.Id], applied);
    }

    public sealed class MigratedThing
    {
        public long Id { get; set; }

        public string Name { get; set; } = "";
    }

    public sealed class MigrateContext(string connectionString) : DbContext
    {
        public DbSet<MigratedThing> Things => Set<MigratedThing>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseCamusDB(connectionString);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<MigratedThing>(e =>
            {
                e.ToTable("ef_mig_things");
                e.HasKey(t => t.Id);
                e.Property(t => t.Id).ValueGeneratedNever();
            });
    }

    [DbContext(typeof(MigrateContext))]
    [Migration(Id)]
    public sealed class CreateThingsMigration : Migration
    {
        public const string Id = "20261009000000_CreateThings";

        protected override void Up(MigrationBuilder migrationBuilder)
            => migrationBuilder.CreateTable(
                name: "ef_mig_things",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false),
                    Name = table.Column<string>(nullable: false),
                },
                constraints: table => table.PrimaryKey("PK_ef_mig_things", x => x.Id));

        protected override void Down(MigrationBuilder migrationBuilder)
            => migrationBuilder.DropTable(name: "ef_mig_things");
    }

    [DbContext(typeof(MigrateContext))]
    public sealed class MigrateContextModelSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
            => modelBuilder.Entity("CamusDB.Client.Tests.TestEntityFrameworkMigrateLive+MigratedThing", b =>
            {
                b.Property<long>("Id").ValueGeneratedNever();
                b.Property<string>("Name").IsRequired();
                b.HasKey("Id");
                b.ToTable("ef_mig_things");
            });
    }
}
