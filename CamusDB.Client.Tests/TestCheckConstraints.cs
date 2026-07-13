
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestCheckConstraints : BaseTest
{
    private static readonly SemaphoreSlim schemaLock = new(1, 1);

    private static async Task<string> CreateTableAsync(CamusConnection connection, string columnsDdl)
    {
        await schemaLock.WaitAsync();
        try
        {
            await connection.CreateDatabaseAsync(ifNotExists: true);

            for (int attempt = 0; attempt < 5; attempt++)
            {
                string tableName = "checks_" + Guid.NewGuid().ToString("n");
                string sql = $"CREATE TABLE {tableName} ({columnsDdl})";

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

    private static async Task ExecDDLAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand cmd = connection.CreateCamusCommand(sql);
        Assert.True(await cmd.ExecuteDDLAsync());
    }

    private static async Task InsertPriceAsync(CamusConnection connection, string tableName, string id, long? price)
    {
        await using CamusCommand insert = connection.CreateInsertCommand(tableName);
        insert.Parameters.Add("id", ColumnType.Id, id);
        if (price is null)
            insert.Parameters.Add("price", ColumnType.Null, null);
        else
            insert.Parameters.Add("price", ColumnType.Integer64, price.Value);

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task TestCheckRejectsViolatingRow()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTableAsync(connection,
            "id OID PRIMARY KEY NOT NULL, price INT64 CHECK (price > 0)");

        // price > 0 → TRUE → accepted
        await InsertPriceAsync(connection, tableName, CamusObjectIdGenerator.GenerateAsString(), 10);

        // price > 0 → FALSE → rejected with CADB0303
        var ex = await Assert.ThrowsAnyAsync<CamusException>(() =>
            InsertPriceAsync(connection, tableName, CamusObjectIdGenerator.GenerateAsString(), -5));
        Assert.Equal("CADB0303", ex.Code);
    }

    [Fact]
    public async Task TestCheckThreeValuedNullPasses()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTableAsync(connection,
            "id OID PRIMARY KEY NOT NULL, price INT64 CHECK (price > 0)");

        // NULL > 0 → UNKNOWN → accepted (a nullable checked column still accepts NULL)
        await InsertPriceAsync(connection, tableName, CamusObjectIdGenerator.GenerateAsString(), null);
    }

    [Fact]
    public async Task TestAlterAddAndDropCheckConstraint()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTableAsync(connection,
            "id OID PRIMARY KEY NOT NULL, price INT64");

        // Add a named check after the fact.
        await ExecDDLAsync(connection, $"ALTER TABLE {tableName} ADD CONSTRAINT positive_price CHECK (price > 0)");

        var ex = await Assert.ThrowsAnyAsync<CamusException>(() =>
            InsertPriceAsync(connection, tableName, CamusObjectIdGenerator.GenerateAsString(), -1));
        Assert.Equal("CADB0303", ex.Code);

        // Drop it by name; the previously-rejected row is now accepted.
        await ExecDDLAsync(connection, $"ALTER TABLE {tableName} DROP CONSTRAINT positive_price");
        await InsertPriceAsync(connection, tableName, CamusObjectIdGenerator.GenerateAsString(), -1);
    }

    [Fact]
    public async Task TestNamedNotNullDropAndSet()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTableAsync(connection,
            "id OID PRIMARY KEY NOT NULL, name STRING CONSTRAINT name_required NOT NULL");

        // NOT NULL is enforced.
        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("name", ColumnType.Null, null);
            var ex = await Assert.ThrowsAnyAsync<CamusException>(() => insert.ExecuteNonQueryAsync());
            Assert.Equal("CADB0301", ex.Code);
        }

        // Drop the named NOT NULL; a null name is now accepted.
        await ExecDDLAsync(connection, $"ALTER TABLE {tableName} DROP CONSTRAINT name_required");

        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("name", ColumnType.Null, null);
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }
    }
}
