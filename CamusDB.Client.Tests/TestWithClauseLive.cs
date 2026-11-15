/**
 * This file is part of CamusDB
 *
 * Live coverage for WITH queries (server commit b78d8428) and for UPDATE ... FROM and
 * DELETE ... USING (server commit 3d8247a2) over REST and gRPC. A write after a WITH clause must reach
 * the non-query endpoint: the query endpoint refuses it with CADB0407. Needs a server on :5095 (REST)
 * and :5096 (gRPC) that has both commits.
 */

namespace CamusDB.Client.Tests;

public class TestWithClauseLive : BaseTest
{
    private static CamusConnectionStringBuilder BuilderFor(string protocol) => protocol == "grpc"
        ? new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc;MaxAutoPrepare=1")
        : new("Endpoint=http://localhost:5095;Database=test;MaxAutoPrepare=1");

    private static async Task<(CamusConnection Connection, string Table)> SeedAsync(string protocol)
    {
        CamusConnection connection = new(BuilderFor(protocol));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = await CreateTempTableAsync(connection, "with_live", "id INT64 PRIMARY KEY, grp INT64, total INT64");

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, grp, total) VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)");
        Assert.Equal(3, await insert.ExecuteNonQueryAsync());

        return (connection, table);
    }

    private static async Task<List<long>> TotalsAsync(CamusConnection connection, string table)
    {
        await using CamusCommand select = connection.CreateCamusCommand($"SELECT total FROM {table} ORDER BY id");
        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        List<long> totals = [];
        while (await reader.ReadAsync())
            totals.Add(reader.GetInt64(0));
        return totals;
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task SelectAfterWithReadsTheNamedQuery(string protocol)
    {
        (CamusConnection connection, string table) = await SeedAsync(protocol);
        await using CamusConnection _ = connection;

        // MaxAutoPrepare=1 prepares the statement on its first run, so the second run uses the handle.
        for (int run = 0; run < 2; run++)
        {
            await using CamusCommand select = connection.CreateCamusCommand(
                $"WITH big (k, t) AS (SELECT id, total FROM {table} WHERE total >= @min) SELECT k FROM big ORDER BY k");
            select.Parameters.Add("@min", ColumnType.Integer64, 20);

            await using CamusDataReader reader = await select.ExecuteReaderAsync();
            List<long> ids = [];
            while (await reader.ReadAsync())
                ids.Add(reader.GetInt64(0));

            Assert.Equal([2L, 3L], ids);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task WriteAfterWithRunsAsAWrite(string protocol)
    {
        (CamusConnection connection, string table) = await SeedAsync(protocol);
        await using CamusConnection _ = connection;

        string update = $"WITH s AS (SELECT id FROM {table} WHERE grp = 1) UPDATE {table} SET total = total + 1 WHERE id IN (SELECT id FROM s)";

        await using (CamusCommand command = connection.CreateCamusCommand(update))
            Assert.Equal(2, await command.ExecuteNonQueryAsync());

        // ExecuteReader of a write without RETURNING: the reader has no rows and the affected-row count.
        await using (CamusCommand command = connection.CreateCamusCommand(update))
        await using (CamusDataReader reader = await command.ExecuteReaderAsync())
        {
            Assert.False(await reader.ReadAsync());
            Assert.Equal(2, reader.RecordsAffected);
        }

        await using (CamusCommand command = connection.CreateCamusCommand(update + " RETURNING id, total"))
        await using (CamusDataReader reader = await command.ExecuteStreamReaderAsync())
        {
            List<(long, long)> rows = [];
            while (await reader.ReadAsync())
                rows.Add((reader.GetInt64(0), reader.GetInt64(1)));

            Assert.Equal([(1L, 13L), (2L, 23L)], rows.OrderBy(r => r.Item1));
        }

        Assert.Equal([13L, 23L, 30L], await TotalsAsync(connection, table));
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task UpdateFromAndDeleteUsingReadAnotherRelation(string protocol)
    {
        (CamusConnection connection, string table) = await SeedAsync(protocol);
        await using CamusConnection _ = connection;

        await using (CamusCommand update = connection.CreateCamusCommand(
            $"UPDATE {table} AS t SET total = t.total + d.amount " +
            "FROM (SELECT 1 AS k, 5 AS amount UNION ALL SELECT 3, 7) AS d WHERE d.k = t.id RETURNING t.id"))
        await using (CamusDataReader reader = await update.ExecuteReaderAsync())
        {
            List<long> ids = [];
            while (await reader.ReadAsync())
                ids.Add(reader.GetInt64(0));

            Assert.Equal([1L, 3L], ids.Order());
        }

        Assert.Equal([15L, 20L, 37L], await TotalsAsync(connection, table));

        await using (CamusCommand delete = connection.CreateCamusCommand(
            $"WITH gone AS (SELECT 2 AS k) DELETE FROM {table} AS t USING gone WHERE gone.k = t.id"))
            Assert.Equal(1, await delete.ExecuteNonQueryAsync());

        Assert.Equal([15L, 37L], await TotalsAsync(connection, table));
    }
}
