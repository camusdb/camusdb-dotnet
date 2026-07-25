
/**
 * This file is part of CamusDB
 *
 * Live coverage for CamusCommand.ExecuteStreamReaderAsync() over the /execute-sql-query-stream endpoint:
 * a streamed SELECT returns the same rows as the buffered reader, an empty result still exposes the
 * schema, and a parameterized filter round-trips. Requires a running CamusDB (REST :5095).
 */

namespace CamusDB.Client.Tests;

public class TestStreamReader : BaseTest
{
    [Fact]
    public async Task StreamedSelectReturnsAllRows()
    {
        CamusConnection connection = await GetConnection();
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

    [Fact]
    public async Task StreamedEmptyResultExposesSchemaAndNoRows()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        using CamusCommand cmd = connection.CreateSelectCommand($"SELECT * FROM {tableName} WHERE year = 9999");
        await using CamusDataReader reader = await cmd.ExecuteStreamReaderAsync();

        Assert.False(reader.HasRows);
        Assert.Equal(6, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task StreamedSelectWithBoundParameterFilters()
    {
        CamusConnection connection = await GetConnection();
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
}
