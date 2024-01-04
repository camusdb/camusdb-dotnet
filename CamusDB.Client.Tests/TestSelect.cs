
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestSelect : BaseTest
{
    public TestSelect()
    {
    }

    [Fact]
    public async void TestBasicSelect()
    {
        CamusConnection connection = await GetConnection();

        string sql = "SELECT * FROM robots";

        using CamusCommand cmd = connection.CreateSelectCommand(sql);

        cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

        //Assert.Equal(1, await cmd.ExecuteNonQueryAsync());        

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            Console.WriteLine(reader.GetString(0));
            Console.WriteLine(reader.GetString(1));
            Console.WriteLine(reader.GetString(2));
            Console.WriteLine(reader.GetInt64(3));
        }
    }

    private static async Task ExecuteSelect(CamusConnection connection)
    {
        string sql = "SELECT * FROM robots WHERE year = 2002";

        using CamusCommand cmd = connection.CreateSelectCommand(sql);

        /*cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));*/

        //Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        //
        int number = 0;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            /*Console.WriteLine(reader.GetString(0));
            Console.WriteLine(reader.GetString(1));
            Console.WriteLine(reader.GetString(2));
            Console.WriteLine(reader.GetInt64(3));*/

            number++;
        }
    }

    [Fact]
    public async void TestMultiSelect()
    {
        CamusConnection connection = await GetConnection();

        List<Task> tasks = new();

        for (int j = 0; j < 100; j++)
            tasks.Add(ExecuteSelect(connection));

        await Task.WhenAll(tasks);
    }
}