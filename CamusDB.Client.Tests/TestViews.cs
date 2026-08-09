/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

/// <summary>
/// View DDL routing. The server implements view statements only on its DDL endpoint, so a driver
/// that classifies them as ordinary non-queries sends them where nothing handles them and the caller
/// gets "Unknown non-query AST stmt" instead of a view. These run every view statement through
/// <see cref="CamusCommand.ExecuteNonQueryAsync(CancellationToken)"/> — the path an application and
/// EF's <c>migrationBuilder.Sql</c> both take — rather than calling ExecuteDDLAsync directly, which
/// would bypass the classification under test.
/// </summary>
public class TestViews : BaseTest
{
    private static async Task<CamusConnection> GetReadyConnection()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    /// <summary>
    /// Runs a view statement through the non-query path, retried like the suite's other schema helpers:
    /// test classes creating tables concurrently make the server report retryable lock contention.
    /// </summary>
    private static async Task ExecViewDdlAsync(CamusConnection connection, string sql)
    {
        for (int attempt = 0; ; attempt++)
        {
            await using CamusCommand cmd = connection.CreateCamusCommand(sql);

            try
            {
                await cmd.ExecuteNonQueryAsync();
                return;
            }
            catch (CamusException ex) when (attempt < 4 && (
                ex.Message.Contains("MustRetry", StringComparison.Ordinal) ||
                ex.Message.Contains("AlreadyLocked", StringComparison.Ordinal) ||
                ex.Message.Contains("commit returned Aborted", StringComparison.Ordinal)))
            {
                await Task.Delay(100 * (attempt + 1));
            }
        }
    }

    private static async Task<string> CreateViewOverRobotsAsync(CamusConnection connection, string tableName)
    {
        string viewName = "v_" + Guid.NewGuid().ToString("n");

        await ExecViewDdlAsync(
            connection,
            $"CREATE VIEW {viewName} AS SELECT id, name, year FROM {tableName} WHERE enabled = true");

        return viewName;
    }

    [Fact]
    public async Task TestCreateAndSelectThroughView()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, tableName, name: "visible", year: 1999, enabled: true);
        await InsertRobotAsync(connection, tableName, name: "hidden", year: 1999, enabled: false);

        string viewName = await CreateViewOverRobotsAsync(connection, tableName);

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name, year FROM {viewName}");
        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        List<string> names = new();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(reader.GetOrdinal("name")));

        // The view's own WHERE decides what is readable through it, so the disabled row is absent.
        Assert.Equal(["visible"], names);
    }

    [Fact]
    public async Task TestCreateOrReplaceViewAppendsColumn()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        string viewName = await CreateViewOverRobotsAsync(connection, tableName);

        // A replacement may only append; keeping id, name, year in place and adding price is legal.
        await ExecViewDdlAsync(
            connection,
            $"CREATE OR REPLACE VIEW {viewName} AS SELECT id, name, year, price FROM {tableName}");

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT price FROM {viewName}");
        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.Equal("price", reader.GetName(0));
    }

    [Fact]
    public async Task TestRenameAndDropView()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        string viewName = await CreateViewOverRobotsAsync(connection, tableName);
        string renamed = viewName + "_renamed";

        await ExecViewDdlAsync(connection, $"ALTER VIEW {viewName} RENAME TO {renamed}");

        await using (CamusCommand select = connection.CreateCamusCommand($"SELECT name FROM {renamed}"))
        {
            await using CamusDataReader reader = await select.ExecuteReaderAsync();
            Assert.Equal("name", reader.GetName(0));
        }

        await ExecViewDdlAsync(connection, $"DROP VIEW {renamed}");

        // Reading a dropped view fails; the relation is simply gone.
        await using CamusCommand afterDrop = connection.CreateCamusCommand($"SELECT name FROM {renamed}");
        await Assert.ThrowsAsync<CamusException>(async () =>
        {
            await using CamusDataReader reader = await afterDrop.ExecuteReaderAsync();
            await reader.ReadAsync();
        });
    }

    [Fact]
    public async Task TestDropViewIfExistsOnMissingView()
    {
        CamusConnection connection = await GetReadyConnection();

        await ExecViewDdlAsync(connection, $"DROP VIEW IF EXISTS v_{Guid.NewGuid():n}");
    }

    /// <summary>
    /// A materialized view answers from stored rows, so a write to the base table is invisible until a
    /// refresh — the one behavior that distinguishes it from a plain view, and the one a driver must
    /// not paper over. <c>REFRESH</c> is a write, not schema: it goes to the data endpoint and reports
    /// the rows it wrote, which is why it is absent from the DDL prefix list.
    /// </summary>
    [Fact]
    public async Task TestMaterializedViewIsASnapshotUntilRefreshed()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        string matViewName = "mv_" + Guid.NewGuid().ToString("n");

        await InsertRobotAsync(connection, tableName, name: "first", year: 2001);

        await ExecViewDdlAsync(
            connection,
            $"CREATE MATERIALIZED VIEW {matViewName} AS SELECT name, year FROM {tableName}");

        Assert.Equal(1, await CountRowsAsync(connection, matViewName));

        // The stored copy does not move on its own.
        await InsertRobotAsync(connection, tableName, name: "second", year: 2002);
        Assert.Equal(1, await CountRowsAsync(connection, matViewName));

        await using (CamusCommand refresh = connection.CreateCamusCommand(
            $"REFRESH MATERIALIZED VIEW {matViewName}"))
        {
            Assert.Equal(2, await refresh.ExecuteNonQueryAsync());
        }

        Assert.Equal(2, await CountRowsAsync(connection, matViewName));

        await ExecViewDdlAsync(connection, $"DROP MATERIALIZED VIEW {matViewName}");
    }

    [Fact]
    public async Task TestMaterializedViewRefusesWrites()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        string matViewName = "mv_" + Guid.NewGuid().ToString("n");

        await ExecViewDdlAsync(
            connection,
            $"CREATE MATERIALIZED VIEW {matViewName} AS SELECT name, year FROM {tableName}");

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {matViewName} (name, year) VALUES ('handwritten', 1970)");

        // A hand-written row would be discarded by the next refresh, so it is refused outright.
        await Assert.ThrowsAsync<CamusException>(() => insert.ExecuteNonQueryAsync());
    }

    /// <summary>
    /// Cache resolution differs by relation kind, and the driver surfaces the distinction. A plain view
    /// expands to a derived table, which has no row keyspace for the cache to fence, so a hinted read
    /// through one is a bypass — reported as <c>derived-source</c> rather than silently, so a caller is
    /// not left wondering why its hint bought nothing. A materialized view is a physical relation and
    /// caches like a table.
    /// </summary>
    [Fact]
    public async Task TestCacheHintThroughViewIsReportedAsDerivedSourceBypass()
    {
        CamusConnection connection = await GetReadyConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        await InsertRobotAsync(connection, tableName, name: "cached", year: 2003);

        string viewName = await CreateViewOverRobotsAsync(connection, tableName);
        string cacheName = "c" + Guid.NewGuid().ToString("n")[..8];

        await using (CamusCommand throughView = connection.CreateCamusCommand(
            $"SELECT name FROM {viewName} {{cache={cacheName}}}"))
        {
            await using CamusDataReader reader = await throughView.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }

            CamusCacheMetadata metadata = Assert.IsType<CamusCacheMetadata>(throughView.LastCacheMetadata);

            Assert.Equal(CamusCacheStatus.Bypass, metadata.Status);
            Assert.Equal("derived-source", metadata.BypassReason);
            Assert.Equal(cacheName, metadata.Name);
        }

        string matViewName = "mv_" + Guid.NewGuid().ToString("n");
        await ExecViewDdlAsync(
            connection,
            $"CREATE MATERIALIZED VIEW {matViewName} AS SELECT name, year FROM {tableName}");

        string matViewSql = $"SELECT name FROM {matViewName} {{cache={cacheName}}}";

        await using (CamusCommand populate = connection.CreateCamusCommand(matViewSql))
        {
            await using CamusDataReader reader = await populate.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }

            Assert.Equal(CamusCacheStatus.Miss, populate.LastCacheMetadata?.Status);
        }

        await using (CamusCommand served = connection.CreateCamusCommand(matViewSql))
        {
            await using CamusDataReader reader = await served.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }

            // A materialized view is cached like the physical relation it is.
            Assert.Equal(CamusCacheStatus.Hit, served.LastCacheMetadata?.Status);
            Assert.Null(served.LastCacheMetadata?.BypassReason);
        }

        await ExecViewDdlAsync(connection, $"DROP MATERIALIZED VIEW {matViewName}");
    }

    private static async Task<int> CountRowsAsync(CamusConnection connection, string relation)
    {
        await using CamusCommand cmd = connection.CreateCamusCommand($"SELECT name FROM {relation}");
        await using CamusDataReader reader = await cmd.ExecuteReaderAsync();

        int rows = 0;
        while (await reader.ReadAsync())
            rows++;

        return rows;
    }
}
