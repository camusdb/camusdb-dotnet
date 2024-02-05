


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
    public async void TestBeginTxAndCommit()
    {
        CamusConnection connection = await GetConnection();

        CamusTransaction transaction = await connection.BeginTransactionAsync();        

        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Transaction = transaction;

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());        

        await transaction.CommitAsync();
    }

    [Fact]
    public async void TestBeginTxAndRollback()
    {
        CamusConnection connection = await GetConnection();

        CamusTransaction transaction = await connection.BeginTransactionAsync();        

        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Transaction = transaction;

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());

        await transaction.RollbackAsync();
    }
}