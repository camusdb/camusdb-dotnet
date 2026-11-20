/**
 * This file is part of CamusDB
 *
 * Live coverage for CamusTransaction.CommitTimestamp over REST and gRPC: a write commit reports a
 * value, a read-only commit, an empty commit and a rollback report null, three commits that update one
 * row report increasing values, and a pipeline commit reports a value. Needs a server on :5095 (REST)
 * and :5096 (gRPC) that reports the commit timestamp (CamusDB server 0.18.4, commit 63ecdb03).
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestCommitTimestampLive : BaseTest
{
    private static CamusConnectionStringBuilder BuilderFor(string protocol) => protocol == "grpc"
        ? new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc")
        : new("Endpoint=http://localhost:5095;Database=test");

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(BuilderFor(protocol));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    private static async Task<(string Table, string Id)> SeedAsync(CamusConnection connection)
    {
        string table = await CreateTempTableAsync(connection, "commit_ts", "id OID PRIMARY KEY NOT NULL, n INT64 NOT NULL");
        string id = CamusObjectIdGenerator.GenerateAsString();

        await using CamusCommand insert = connection.CreateCamusCommand($"INSERT INTO {table} (id, n) VALUES (@id, 0)");
        insert.Parameters.Add("@id", ColumnType.Id, id);
        Assert.Equal(1, await insert.ExecuteNonQueryAsync());

        return (table, id);
    }

    private static CamusCommand Update(CamusConnection connection, CamusTransaction transaction, string table, string id, long n)
    {
        CamusCommand update = connection.CreateCamusCommand($"UPDATE {table} SET n = @n WHERE id = @id");
        update.Parameters.Add("@n", ColumnType.Integer64, n);
        update.Parameters.Add("@id", ColumnType.Id, id);
        update.Transaction = transaction;
        return update;
    }

    private static async Task<CamusHlcTimestamp> CommitUpdateAsync(CamusConnection connection, string table, string id, long n)
    {
        await using CamusTransaction transaction = await connection.BeginTransactionAsync();
        await using (CamusCommand update = Update(connection, transaction, table, id, n))
            Assert.Equal(1, await update.ExecuteNonQueryAsync());

        Assert.Null(transaction.CommitTimestamp);
        await transaction.CommitAsync();

        Assert.NotNull(transaction.CommitTimestamp);
        return transaction.CommitTimestamp.Value;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task CommitsOfOneRowReportIncreasingCommitTimestamps(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        (string table, string id) = await SeedAsync(connection);

        CamusHlcTimestamp first = await CommitUpdateAsync(connection, table, id, 1);
        CamusHlcTimestamp second = await CommitUpdateAsync(connection, table, id, 2);
        CamusHlcTimestamp third = await CommitUpdateAsync(connection, table, id, 3);

        Assert.True(first.L > 0);
        Assert.True(first < second, $"{first} < {second}");
        Assert.True(second < third, $"{second} < {third}");
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task AReadOnlyCommitAnEmptyCommitAndARollbackReportNull(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        (string table, string id) = await SeedAsync(connection);

        await using (CamusTransaction readOnly = await connection.BeginTransactionAsync())
        {
            await using CamusCommand read = connection.CreateSelectCommand($"SELECT n FROM {table} WHERE id = @id");
            read.Parameters.Add("@id", ColumnType.Id, id);
            read.Transaction = readOnly;
            await using (CamusDataReader reader = await read.ExecuteReaderAsync())
                Assert.True(await reader.ReadAsync());

            await readOnly.CommitAsync();
            Assert.Null(readOnly.CommitTimestamp);
        }

        await using (CamusTransaction empty = await connection.BeginTransactionAsync())
        {
            await empty.CommitAsync();
            Assert.Null(empty.CommitTimestamp);
        }

        await using (CamusTransaction rolledBack = await connection.BeginTransactionAsync())
        {
            await using (CamusCommand update = Update(connection, rolledBack, table, id, 9))
                Assert.Equal(1, await update.ExecuteNonQueryAsync());

            await rolledBack.RollbackAsync();
            Assert.Null(rolledBack.CommitTimestamp);
        }
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task APipelineCommitReportsTheCommitTimestamp(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        (string table, string id) = await SeedAsync(connection);

        CamusHlcTimestamp before = await CommitUpdateAsync(connection, table, id, 1);

        await using CamusTransaction transaction = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = transaction.CreatePipeline();
        await using CamusCommand update = Update(connection, transaction, table, id, 2);
        Task<int> updated = pipeline.QueueNonQuery(update);
        await pipeline.CommitAsync();

        Assert.Equal(1, await updated);
        Assert.NotNull(transaction.CommitTimestamp);
        Assert.True(before < transaction.CommitTimestamp.Value, $"{before} < {transaction.CommitTimestamp}");
    }
}
