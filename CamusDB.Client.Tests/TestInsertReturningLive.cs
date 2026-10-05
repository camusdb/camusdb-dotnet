/**
 * This file is part of CamusDB
 *
 * Live coverage for INSERT … RETURNING on both transports: the reader, the scalar, the count-only
 * non-query, the stream reader, a transaction, and the EF Core read-back of store-generated columns.
 * Requires a server with RETURNING support, so the tests are opt-in: set CAMUSDB_TEST_RETURNING=true.
 * The REST endpoint comes from CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint
 * from CAMUSDB_TEST_GRPC_ENDPOINT (default localhost:5096).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.Client.Tests;

public class TestInsertReturningLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_RETURNING"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<string> CreateTableAsync(CamusConnection connection)
    {
        string table = "ret_" + Guid.NewGuid().ToString("n")[..12];
        await using CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, total FLOAT64, status STRING DEFAULT ('new'))");
        await cmd.ExecuteDDLAsync();
        return table;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ReaderReturnsStoredValues(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, total) VALUES (GEN_ID(), @a, 10), (GEN_ID(), @b, 20) RETURNING id, name, total * 2 AS doubled, status");
        insert.Parameters.Add("@a", ColumnType.String, "a");
        insert.Parameters.Add("@b", ColumnType.String, "b");

        await using CamusDataReader reader = await insert.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);
        Assert.Equal(["id", "name", "doubled", "status"], Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray());

        List<(string Id, string Name, double Doubled, string Status)> rows = [];
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetDouble(2), reader.GetString(3)));

        Assert.Equal(2, rows.Count);
        Assert.Equal(["a", "b"], rows.Select(r => r.Name).ToArray());
        Assert.Equal([20.0, 40.0], rows.Select(r => r.Doubled).ToArray());
        Assert.All(rows, r => Assert.Equal("new", r.Status));
        Assert.All(rows, r => Assert.False(string.IsNullOrEmpty(r.Id)));

        // The ids that came back are the stored ids.
        await using CamusCommand count = connection.CreateCamusCommand(
            $"SELECT COUNT(*) FROM {table} WHERE id = @id");
        count.Parameters.Add("@id", ColumnType.Id, rows[0].Id);
        Assert.Equal(1L, await count.ExecuteScalarAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ScalarReturnsTheGeneratedKey(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 'k') RETURNING id");

        string? id = await insert.ExecuteScalarAsync() as string;
        Assert.False(string.IsNullOrEmpty(id));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task NonQueryReturnsTheCountOnly(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 'x'), (GEN_ID(), 'y'), (GEN_ID(), 'z') RETURNING *");

        Assert.Equal(3, await insert.ExecuteNonQueryAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamReaderReturnsTheRows(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 's1'), (GEN_ID(), 's2') RETURNING name");
        await using CamusDataReader reader = await insert.ExecuteStreamReaderAsync();

        List<string> names = [];
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        Assert.Equal(["s1", "s2"], names);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task RollbackRemovesReturnedRows(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using (CamusTransaction txn = await connection.BeginTransactionAsync())
        {
            await using CamusCommand insert = connection.CreateCamusCommand(
                $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), 'tx') RETURNING name");
            insert.Transaction = txn;

            Assert.Equal("tx", await insert.ExecuteScalarAsync());
            await txn.RollbackAsync();
        }

        await using CamusCommand count = connection.CreateCamusCommand($"SELECT COUNT(*) FROM {table}");
        Assert.Equal(0L, await count.ExecuteScalarAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task EntityFrameworkReadsDefaultsBack(string protocol)
    {
        if (!Enabled)
            return;

        string suffix = Guid.NewGuid().ToString("n")[..12];
        DbContextOptions<WidgetContext> options = new DbContextOptionsBuilder<WidgetContext>()
            .UseCamusDB(ConnString(protocol))
            .ReplaceService<IModelCacheKeyFactory, SuffixModelCacheKeyFactory>()
            .Options;

        await using (WidgetContext setup = new(options, suffix))
            await setup.Database.EnsureCreatedAsync();

        Widget first = new() { Id = CamusObjectId(), Name = "w1" };
        Widget second = new() { Id = CamusObjectId(), Name = "w2", Status = "custom" };
        await using (WidgetContext ctx = new(options, suffix))
        {
            ctx.Widgets.AddRange(first, second);
            await ctx.SaveChangesAsync();
        }

        // The server drew Serial from the sequence and filled Status from the default; EF read both back.
        Assert.Equal([1L, 2L], new[] { first.Serial, second.Serial }.Order().ToArray());
        Assert.Equal("new", first.Status);
        Assert.Equal("custom", second.Status);

        await using (WidgetContext ctx = new(options, suffix))
        {
            Widget stored = await ctx.Widgets.SingleAsync(w => w.Id == first.Id);
            Assert.Equal(first.Serial, stored.Serial);
            Assert.Equal("new", stored.Status);
        }
    }

    private static string CamusObjectId() => new CamusObjectIdValueGenerator().Next(null!);

    public class Widget
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public long Serial { get; set; }

        public string? Status { get; set; }
    }

    /// <summary>EF caches one model for each context type. The model names its table and sequence
    /// with the suffix, so each suffix needs its own model.</summary>
    private sealed class SuffixModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), ((WidgetContext)context).Suffix, designTime);
    }

    private sealed class WidgetContext(DbContextOptions options, string suffix) : DbContext(options)
    {
        public string Suffix => suffix;

        public DbSet<Widget> Widgets => Set<Widget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasSequence<long>("widget_serial_" + suffix);
            modelBuilder.Entity<Widget>(b =>
            {
                b.ToTable("widgets_" + suffix);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Serial).HasDefaultValueSql($"nextval('widget_serial_{suffix}')");
                b.Property(e => e.Status).HasDefaultValue("new");
            });
        }
    }
}
