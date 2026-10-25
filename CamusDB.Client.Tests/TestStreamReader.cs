/**
 * This file is part of CamusDB
 *
 * Live coverage for CamusCommand.ExecuteStreamReaderAsync() on both transports — the
 * /execute-sql-query-stream endpoint on REST and the server-streaming ExecuteQuery RPC on gRPC: a
 * streamed SELECT returns the same rows as the buffered reader, an empty result still exposes the
 * schema, and a parameterized filter round-trips. Requires a running CamusDB (REST :5095, gRPC :5096).
 */

namespace CamusDB.Client.Tests;

public class TestStreamReader : BaseTest
{
    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        if (protocol != "grpc")
            return await GetConnection();

        CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:5096;Database=test;Protocol=grpc"));
        await connection.OpenAsync();
        return connection;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamedSelectReturnsAllRows(string protocol)
    {
        CamusConnection connection = await OpenAsync(protocol);
        string tableName = await CreateTempRobotsTableAsync(connection);

        const int rows = 250;   // > the server's 128-row flush window, so multiple network flushes are drained.
        for (int i = 0; i < rows; i++)
            await InsertRobotAsync(connection, tableName, name: $"r{i}", type: "mechanical", year: 2000 + i, price: i, enabled: true);

        using CamusCommand cmd = connection.CreateSelectCommand($"SELECT * FROM {tableName}");
        await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();

        Assert.True(reader.HasRows);
        Assert.Equal(6, reader.FieldCount);

        HashSet<string> ids = new(StringComparer.Ordinal);
        while (await reader.ReadAsync())
        {
            string id = reader.GetString(0);
            Assert.True(ids.Add(id), $"Duplicate id {id}");
        }

        Assert.Equal(rows, ids.Count);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamedEmptyResultExposesSchemaAndNoRows(string protocol)
    {
        CamusConnection connection = await OpenAsync(protocol);
        string tableName = await CreateTempRobotsTableAsync(connection);

        using CamusCommand cmd = connection.CreateSelectCommand($"SELECT * FROM {tableName} WHERE year = 9999");
        await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();

        Assert.False(reader.HasRows);
        Assert.Equal(6, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.False(await reader.ReadAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamedSelectWithBoundParameterFilters(string protocol)
    {
        CamusConnection connection = await OpenAsync(protocol);
        string tableName = await CreateTempRobotsTableAsync(connection);
        await InsertRobotAsync(connection, tableName, name: "keep", type: "mechanical", year: 1974, price: 1.0, enabled: true);
        await InsertRobotAsync(connection, tableName, name: "drop", type: "mechanical", year: 1975, price: 2.0, enabled: true);

        using CamusCommand cmd = connection.CreateSelectCommand($"SELECT name FROM {tableName} WHERE year = @year");
        cmd.Parameters.Add("@year", ColumnType.Integer64, 1974);

        await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();

        List<string> names = [];
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        Assert.Equal(["keep"], names);
    }

    /// <summary>
    /// On gRPC the streamed read runs on a call of its own, outside the batched stream the transaction
    /// is pinned to; it must still see the transaction's own uncommitted writes, and the transaction
    /// must still commit after it.
    /// </summary>
    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamedSelectInsideATransactionSeesItsWrites(string protocol)
    {
        CamusConnection connection = await OpenAsync(protocol);
        string tableName = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await InsertRobotAsync(connection, tableName, name: "in-txn", year: 1990, transaction: transaction);

        using (CamusCommand cmd = connection.CreateSelectCommand($"SELECT name FROM {tableName}"))
        {
            cmd.Transaction = transaction;
            await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal("in-txn", reader.GetString(0));
            Assert.False(await reader.ReadAsync());
        }

        await InsertRobotAsync(connection, tableName, name: "after-read", year: 1991, transaction: transaction);
        await transaction.CommitAsync();

        using CamusCommand count = connection.CreateSelectCommand($"SELECT name FROM {tableName}");
        await using CamusDataReader committed = await count.ExecuteReaderAsync();

        int rows = 0;
        while (await committed.ReadAsync())
            rows++;

        Assert.Equal(2, rows);
    }

    /// <summary>
    /// A reader closed before its last row cancels the call. The connection must stay usable, and the
    /// server must not be left holding the read.
    /// </summary>
    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task StreamedReaderClosedEarlyLeavesTheConnectionUsable(string protocol)
    {
        CamusConnection connection = await OpenAsync(protocol);
        string tableName = await CreateTempRobotsTableAsync(connection);

        for (int i = 0; i < 300; i++)
            await InsertRobotAsync(connection, tableName, name: $"r{i}", year: 2000 + i);

        using (CamusCommand cmd = connection.CreateSelectCommand($"SELECT * FROM {tableName}"))
        {
            await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();
            Assert.True(await reader.ReadAsync());
        }

        await InsertRobotAsync(connection, tableName, name: "after", year: 3000);

        using CamusCommand check = connection.CreateSelectCommand($"SELECT name FROM {tableName} WHERE year = 3000");
        await using CamusDataReader again = await check.ExecuteStreamReaderAsync();

        Assert.True(await again.ReadAsync());
        Assert.Equal("after", again.GetString(0));
    }
}
