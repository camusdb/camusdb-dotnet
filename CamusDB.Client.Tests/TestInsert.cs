
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestInsert : BaseTest
{
	public TestInsert()
	{
	}


    [Fact]
    public async void TestSimpleInsert()
	{
        CamusConnection connection = await GetConnection();

        using CamusCommand cmd = connection.CreateInsertCommand("robots2");

        cmd.Parameters.Add("id", ColumnType.Id, ObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);        

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }
}

