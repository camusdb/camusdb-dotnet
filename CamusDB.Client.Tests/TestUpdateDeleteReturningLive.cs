/**
 * This file is part of CamusDB
 *
 * Live coverage for UPDATE … RETURNING and DELETE … RETURNING on both transports: the reader, the
 * count-only non-query, the stream reader, a statement that writes no rows, a transaction, and the
 * EF Core read-back on an update. Requires a server with UPDATE/DELETE RETURNING support, so the tests
 * are opt-in: set CAMUSDB_TEST_RETURNING=true. The REST endpoint comes from CAMUSDB_TEST_ENDPOINT
 * (default localhost:5095), and the gRPC endpoint from CAMUSDB_TEST_GRPC_ENDPOINT (default
 * localhost:5096).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.Client.Tests;

public class TestUpdateDeleteReturningLive
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

    /// <summary>Creates a table with the rows a (10), b (20) and c (30).</summary>
    private static async Task<string> CreateTableAsync(CamusConnection connection)
    {
        string table = "udret_" + Guid.NewGuid().ToString("n")[..12];
        await using (CamusCommand create = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, total FLOAT64, status STRING DEFAULT ('new'))"))
            await create.ExecuteDDLAsync();

        await using (CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, total) VALUES (GEN_ID(), 'a', 10), (GEN_ID(), 'b', 20), (GEN_ID(), 'c', 30)"))
            Assert.Equal(3, await insert.ExecuteNonQueryAsync());

        return table;
    }

    private static async Task<List<(string Name, double Total)>> ReadNameTotalAsync(CamusDataReader reader)
    {
        List<(string Name, double Total)> rows = [];
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetDouble(1)));

        // An UPDATE or a DELETE returns its rows in no defined order.
        rows.Sort((x, y) => string.CompareOrdinal(x.Name, y.Name));
        return rows;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task UpdateReaderReturnsTheNewRow(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand update = connection.CreateCamusCommand(
            $"UPDATE {table} SET total = total * 2 WHERE total < @limit RETURNING name, total, status");
        update.Parameters.Add("@limit", ColumnType.Float64, 25.0);

        await using CamusDataReader reader = await update.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);
        Assert.Equal(["name", "total", "status"], Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray());
        Assert.Equal([("a", 20.0), ("b", 40.0)], await ReadNameTotalAsync(reader));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task DeleteReaderReturnsTheDeletedRow(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using (CamusCommand delete = connection.CreateCamusCommand(
            $"DELETE FROM {table} WHERE total > 15 RETURNING name, total"))
        {
            await using CamusDataReader reader = await delete.ExecuteReaderAsync();

            Assert.Equal(2, reader.RecordsAffected);
            Assert.Equal([("b", 20.0), ("c", 30.0)], await ReadNameTotalAsync(reader));
        }

        await using CamusCommand count = connection.CreateCamusCommand($"SELECT COUNT(*) FROM {table}");
        Assert.Equal(1L, await count.ExecuteScalarAsync());
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

        await using (CamusCommand update = connection.CreateCamusCommand(
            $"UPDATE {table} SET status = 'seen' WHERE TRUE RETURNING *"))
            Assert.Equal(3, await update.ExecuteNonQueryAsync());

        await using (CamusCommand delete = connection.CreateCamusCommand(
            $"DELETE FROM {table} WHERE status = 'seen' LIMIT 2 RETURNING *"))
            Assert.Equal(2, await delete.ExecuteNonQueryAsync());
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

        await using (CamusCommand update = connection.CreateCamusCommand(
            $"UPDATE {table} SET total = total + 1 WHERE name <> 'c' RETURNING name, total"))
        {
            await using CamusDataReader reader = await update.ExecuteStreamReaderAsync();
            Assert.Equal([("a", 11.0), ("b", 21.0)], await ReadNameTotalAsync(reader));
        }

        await using (CamusCommand delete = connection.CreateCamusCommand(
            $"DELETE FROM {table} WHERE name = 'c' RETURNING name, total"))
        {
            await using CamusDataReader reader = await delete.ExecuteStreamReaderAsync();
            Assert.Equal([("c", 30.0)], await ReadNameTotalAsync(reader));
        }

        // The stream reader committed both writes.
        await using CamusCommand sum = connection.CreateCamusCommand($"SELECT SUM(total) FROM {table}");
        Assert.Equal(32.0, Convert.ToDouble(await sum.ExecuteScalarAsync()));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task WriteOfNoRowsKeepsTheSchema(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand update = connection.CreateCamusCommand(
            $"UPDATE {table} SET total = 0 WHERE name = 'none' RETURNING name, total");
        await using CamusDataReader reader = await update.ExecuteReaderAsync();

        Assert.Equal(0, reader.RecordsAffected);
        Assert.Equal(2, reader.FieldCount);
        Assert.False(await reader.ReadAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task RollbackKeepsTheOldValues(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using (CamusTransaction txn = await connection.BeginTransactionAsync())
        {
            await using (CamusCommand update = connection.CreateCamusCommand(
                $"UPDATE {table} SET total = 99 WHERE name = 'a' RETURNING total"))
            {
                update.Transaction = txn;
                Assert.Equal(99.0, Convert.ToDouble(await update.ExecuteScalarAsync()));
            }

            await using (CamusCommand delete = connection.CreateCamusCommand(
                $"DELETE FROM {table} WHERE name = 'b' RETURNING name"))
            {
                delete.Transaction = txn;
                Assert.Equal("b", await delete.ExecuteScalarAsync());
            }

            await txn.RollbackAsync();
        }

        await using CamusCommand sum = connection.CreateCamusCommand($"SELECT SUM(total) FROM {table}");
        Assert.Equal(60.0, Convert.ToDouble(await sum.ExecuteScalarAsync()));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task EntityFrameworkUpdateReadsStoreGeneratedColumnsBack(string protocol)
    {
        if (!Enabled)
            return;

        string suffix = Guid.NewGuid().ToString("n")[..12];
        DbContextOptions<GizmoContext> options = new DbContextOptionsBuilder<GizmoContext>()
            .UseCamusDB(ConnString(protocol))
            .ReplaceService<IModelCacheKeyFactory, SuffixModelCacheKeyFactory>()
            .Options;

        await using (GizmoContext setup = new(options, suffix))
            await setup.Database.EnsureCreatedAsync();

        Gizmo gizmo = new() { Id = new CamusObjectIdValueGenerator().Next(null!), Name = "g1" };
        await using (GizmoContext ctx = new(options, suffix))
        {
            ctx.Gizmos.Add(gizmo);
            await ctx.SaveChangesAsync();
        }

        Assert.Equal("new", gizmo.Status);

        // Change the stored Status outside EF. The update does not write Status, so the value that EF
        // reads back is the stored one.
        await using (CamusConnection connection = await OpenAsync(protocol))
        await using (CamusCommand touch = connection.CreateCamusCommand(
            $"UPDATE gizmos_{suffix} SET Status = 'changed' WHERE Id = @id"))
        {
            touch.Parameters.Add("@id", ColumnType.Id, gizmo.Id);
            Assert.Equal(1, await touch.ExecuteNonQueryAsync());
        }

        await using (GizmoContext ctx = new(options, suffix))
        {
            ctx.Gizmos.Attach(gizmo);
            gizmo.Name = "g2";
            await ctx.SaveChangesAsync();
        }

        Assert.Equal("changed", gizmo.Status);
    }

    public class Gizmo
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public string? Status { get; set; }
    }

    /// <summary>EF caches one model for each context type. The model names its table with the suffix,
    /// so each suffix needs its own model.</summary>
    private sealed class SuffixModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), ((GizmoContext)context).Suffix, designTime);
    }

    private sealed class GizmoContext(DbContextOptions options, string suffix) : DbContext(options)
    {
        public string Suffix => suffix;

        public DbSet<Gizmo> Gizmos => Set<Gizmo>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Gizmo>(b =>
            {
                b.ToTable("gizmos_" + suffix);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Status).HasDefaultValue("new").ValueGeneratedOnAddOrUpdate();
            });
        }
    }
}
