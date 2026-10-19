/**
 * This file is part of CamusDB
 *
 * Live coverage for C# extensions on both transports: a procedure through CALL text, through
 * CommandType.StoredProcedure and through Dapper, a reader on CALL, trigger DDL on the DDL endpoint,
 * and an extension function mapped with HasDbFunction in EF Core. Requires a server that loads the
 * server repository's test fixture CamusDB.Tests.Extensions/Fixture.Good (extension_assemblies), so
 * the tests are opt-in: set CAMUSDB_TEST_EXTENSIONS=true. The REST endpoint comes from
 * CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint from
 * CAMUSDB_TEST_GRPC_ENDPOINT (default localhost:5096).
 */

using System.Data;
using CamusDB.Dapper;
using CamusDB.EntityFrameworkCore;
using Dapper;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestExtensionsLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_EXTENSIONS"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();
        return connection;
    }

    private static async Task ExecuteAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    /// <summary>
    /// The fixture procedure fx_transfer writes to a table named <c>accounts</c>, so each test that
    /// calls it works in a database of its own.
    /// </summary>
    private static async Task<CamusConnection> OpenWithAccountsAsync(string protocol)
    {
        string database = "ext_" + Guid.NewGuid().ToString("n")[..12];

        await using (CamusConnection admin = await OpenAsync(protocol))
            await admin.CreateDatabaseAsync(database);

        CamusConnection connection = new(new CamusConnectionStringBuilder(
            ConnString(protocol).Replace("Database=test", "Database=" + database, StringComparison.Ordinal)));
        await connection.OpenAsync();

        await using (CamusCommand create = connection.CreateCamusCommand(
            "CREATE TABLE accounts (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, balance INT64 NOT NULL)"))
            await create.ExecuteDDLAsync();

        await ExecuteAsync(connection, "INSERT INTO accounts (id, name, balance) VALUES (GEN_ID(), 'alice', 100), (GEN_ID(), 'bob', 0)");
        return connection;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task CallTextAndReaderRunTheProcedure(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenWithAccountsAsync(protocol);

        await using (CamusCommand call = connection.CreateCamusCommand("CALL fx_transfer('alice', 'bob', 30)"))
            Assert.Equal(2, await call.ExecuteNonQueryAsync());

        await using (CamusCommand call = connection.CreateCamusCommand("CALL fx_transfer('alice', 'bob', 5)"))
        await using (CamusDataReader reader = await call.ExecuteReaderAsync())
        {
            Assert.False(await reader.ReadAsync());
            Assert.Equal(2, reader.RecordsAffected);
        }

        Assert.Equal(65, await ScalarAsync(connection, "SELECT balance FROM accounts WHERE name = 'alice'"));
        Assert.Equal(35, await ScalarAsync(connection, "SELECT balance FROM accounts WHERE name = 'bob'"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StoredProcedureCommandAndDapperRunTheProcedure(string protocol)
    {
        if (!Enabled)
            return;

        CamusDapper.Register();
        await using CamusConnection connection = await OpenWithAccountsAsync(protocol);

        await using (CamusCommand call = connection.CreateCamusCommand("fx_transfer"))
        {
            call.CommandType = CommandType.StoredProcedure;
            call.Parameters.Add("from", ColumnType.String, "alice");
            call.Parameters.Add("to", ColumnType.String, "bob");
            call.Parameters.Add("amount", ColumnType.Integer64, 10L);
            Assert.Equal(2, await call.ExecuteNonQueryAsync());
        }

        Assert.Equal(2, await connection.ExecuteAsync(
            "fx_transfer", new { from = "alice", to = "bob", amount = 20L }, commandType: CommandType.StoredProcedure));

        Assert.Equal(70, await ScalarAsync(connection, "SELECT balance FROM accounts WHERE name = 'alice'"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ProcedureErrorRollsBackAndKeepsItsCode(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenWithAccountsAsync(protocol);

        await using (CamusCommand call = connection.CreateCamusCommand("CALL fx_transfer('alice', 'bob', 500)"))
        {
            CamusException ex = await Assert.ThrowsAsync<CamusException>(() => call.ExecuteNonQueryAsync());
            Assert.Equal("CADB0822", ex.Code);
        }

        Assert.Equal(100, await ScalarAsync(connection, "SELECT balance FROM accounts WHERE name = 'alice'"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task TriggerDdlRunsAndTheTriggerFires(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenWithAccountsAsync(protocol);
        string table = "ext_people_" + Guid.NewGuid().ToString("n")[..8];

        await using (CamusCommand create = connection.CreateCamusCommand(
            "CREATE TABLE audit_log (id OID PRIMARY KEY NOT NULL DEFAULT (GEN_ID()), op STRING, timing STRING, tbl STRING, detail STRING)"))
            await create.ExecuteDDLAsync();
        await using (CamusCommand create = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL)"))
            await create.ExecuteDDLAsync();

        await ExecuteAsync(connection, $"CREATE TRIGGER audit ON {table} AFTER INSERT, DELETE EXTERNAL NAME 'CamusDB.Fixtures.Good.AuditTrigger'");
        await ExecuteAsync(connection, $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 'ann')");
        Assert.Equal(1, await ScalarAsync(connection, $"SELECT count(*) FROM audit_log WHERE tbl = '{table}' AND detail = 'ann'"));

        await ExecuteAsync(connection, $"DROP TRIGGER audit ON {table}");
        await ExecuteAsync(connection, $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 'bo')");
        Assert.Equal(1, await ScalarAsync(connection, $"SELECT count(*) FROM audit_log WHERE tbl = '{table}'"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task EfCoreCallsAMappedExtensionFunction(string protocol)
    {
        if (!Enabled)
            return;

        string table = "ext_posts_" + Guid.NewGuid().ToString("n")[..8];
        await using (CamusConnection connection = await OpenAsync(protocol))
        {
            await using (CamusCommand create = connection.CreateCamusCommand(
                $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, title STRING NOT NULL)"))
                await create.ExecuteDDLAsync();
            await ExecuteAsync(connection, $"INSERT INTO {table} (id, title) VALUES (GEN_ID(), 'Hello World'), (GEN_ID(), 'Other')");
        }

        await using FunctionsContext ctx = new(
            new DbContextOptionsBuilder<FunctionsContext>().UseCamusDB(ConnString(protocol)).Options, table);

        List<string> slugs = await ctx.Posts
            .Where(p => FunctionsContext.Slugify(p.Title) == "hello-world")
            .Select(p => FunctionsContext.Tag(p.Title))
            .ToListAsync();

        Assert.Equal(["[Hello World]"], slugs);
    }

    private class FunctionsContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<ExtPost> Posts => Set<ExtPost>();

        public static string Slugify(string value) => throw new NotSupportedException();

        public static string Tag(string value) => throw new NotSupportedException();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.ReplaceService<Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory, PerTableModelCacheKeyFactory>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<ExtPost>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Title).HasColumnName("title");
            });

            mb.HasDbFunction(typeof(FunctionsContext).GetMethod(nameof(Slugify))!).HasName("fx_slugify");
            mb.HasDbFunction(typeof(FunctionsContext).GetMethod(nameof(Tag))!).HasName("fx_tag");
        }

        public string Table => table;
    }

    private sealed class PerTableModelCacheKeyFactory : Microsoft.EntityFrameworkCore.Infrastructure.IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), ((FunctionsContext)context).Table, designTime);
    }

    private class ExtPost
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
    }
}
