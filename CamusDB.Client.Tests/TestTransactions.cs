


using CamusDB.Core.Util.ObjectIds;
/**
* This file is part of CamusDB  
*
* For the full copyright and license information, please view the LICENSE.txt
* file that was distributed with this source code.
*/
namespace CamusDB.Client.Tests;

public class TestTransactions : BaseTest
{
    public TestTransactions()
    {

    }

    [Fact]
    public async Task TestBeginTxAndCommit()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync();        

        using CamusCommand cmd = connection.CreateInsertCommand(tableName);

        cmd.Transaction = transaction;

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());        

        await transaction.CommitAsync();
    }

    [Fact]
    public async Task TestBeginTxAndRollback()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync();        

        using CamusCommand cmd = connection.CreateInsertCommand(tableName);

        cmd.Transaction = transaction;

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());

        await transaction.RollbackAsync();
    }

    [Fact]
    public async Task TestDisposeWithoutCommitReleasesTheLocks()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        CamusObjectIdValue id = CamusObjectIdGenerator.Generate();

        using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("name", ColumnType.String, "aaa");
            insert.Parameters.Add("type", ColumnType.String, "mechanical");
            insert.Parameters.Add("year", ColumnType.Integer64, 2000);
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        // Read and write the row, then leave the transaction without a commit or a rollback,
        // as EF Core does when its unit of work throws.
        await using (CamusTransaction abandoned = await connection.BeginTransactionAsync())
        {
            using CamusCommand update = connection.CreateCamusCommand($"UPDATE {tableName} SET year = 2001 WHERE id = @id");
            update.Transaction = abandoned;
            update.Parameters.Add("@id", ColumnType.Id, id);
            Assert.Equal(1, await update.ExecuteNonQueryAsync());
        }

        // The dispose rolled the transaction back: its write is gone, and its locks do not block the next writer.
        CamusTransaction next = await connection.BeginTransactionAsync();
        using (CamusCommand update = connection.CreateCamusCommand($"UPDATE {tableName} SET year = 2002 WHERE id = @id"))
        {
            update.Transaction = next;
            update.Parameters.Add("@id", ColumnType.Id, id);
            Assert.Equal(1, await update.ExecuteNonQueryAsync());
        }
        await next.CommitAsync();

        using CamusCommand select = connection.CreateSelectCommand($"SELECT year FROM {tableName} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);
        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2002, reader.GetInt64(0));
    }
}
