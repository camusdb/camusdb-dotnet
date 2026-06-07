
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class BaseTest
{
    protected static CamusConnectionStringBuilder? builder;

    private static readonly SemaphoreSlim schemaLock = new(1, 1);

    protected static async Task<CamusConnection> GetConnection()
    {
        CamusConnection cmConnection;

        if (builder is not null)
        {
            cmConnection = new(builder);

            await cmConnection.OpenAsync();

            return cmConnection;
        }

        SessionPoolOptions options = new()
        {
            MinimumPooledSessions = 100,
            MaximumActiveSessions = 200,
        };

        string connectionString = "Endpoint=http://localhost:5095;Database=test";

        SessionPoolManager manager = SessionPoolManager.Create(options);

        builder = new(connectionString)
        {
            SessionPoolManager = manager
        };

        Assert.Equal(builder.SessionPoolManager, manager);

        cmConnection = new(builder);

        await cmConnection.OpenAsync();

        return cmConnection;
    }

    protected static async Task<string> CreateTempRobotsTableAsync(CamusConnection connection)
    {
        await schemaLock.WaitAsync();
        try
        {
            for (int attempt = 0; attempt < 5; attempt++)
            {
                string tableName = "robots_" + Guid.NewGuid().ToString("n");
                string sql =
                    $"CREATE TABLE {tableName} (" +
                    " id OID PRIMARY KEY NOT NULL," +
                    " name STRING NOT NULL," +
                    " type STRING," +
                    " year INT64," +
                    " price FLOAT64," +
                    " enabled BOOL)";

                try
                {
                    await using CamusCommand cmd = connection.CreateCamusCommand(sql);
                    Assert.True(await cmd.ExecuteDDLAsync());
                    return tableName;
                }
                catch (CamusException ex) when (
                    ex.Message.Contains("AlreadyLocked", StringComparison.Ordinal) ||
                    ex.Message.Contains("MustRetry", StringComparison.Ordinal) ||
                    ex.Message.Contains("commit returned Aborted", StringComparison.Ordinal) ||
                    ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
                {
                    if (attempt == 4)
                        throw;

                    await Task.Delay(50 * (attempt + 1));
                }
            }
        }
        finally
        {
            schemaLock.Release();
        }

        throw new InvalidOperationException("Unreachable.");
    }

    protected static async Task InsertRobotAsync(
        CamusConnection connection,
        string tableName,
        string? id = null,
        string name = "aaa",
        string? type = "mechanical",
        long? year = 2000,
        double? price = 10.0,
        bool? enabled = true,
        CamusTransaction? transaction = null)
    {
        await using CamusCommand cmd = connection.CreateInsertCommand(tableName);
        cmd.Transaction = transaction;

        cmd.Parameters.Add("id", ColumnType.Id, id ?? CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString());
        cmd.Parameters.Add("name", ColumnType.String, name);

        if (type is null)
            cmd.Parameters.Add("type", ColumnType.Null, null);
        else
            cmd.Parameters.Add("type", ColumnType.String, type);

        if (year is null)
            cmd.Parameters.Add("year", ColumnType.Null, null);
        else
            cmd.Parameters.Add("year", ColumnType.Integer64, year.Value);

        if (price is null)
            cmd.Parameters.Add("price", ColumnType.Null, null);
        else
            cmd.Parameters.Add("price", ColumnType.Float64, price.Value);

        if (enabled is null)
            cmd.Parameters.Add("enabled", ColumnType.Null, null);
        else
            cmd.Parameters.Add("enabled", ColumnType.Bool, enabled.Value);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }
}
