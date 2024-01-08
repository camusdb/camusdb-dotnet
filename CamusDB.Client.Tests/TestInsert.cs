
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestInsertNulls()
    {
        CamusConnection connection = await GetConnection();

        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.Null, null);
        cmd.Parameters.Add("year", ColumnType.Null, null);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestMultiInsert()
    {
        CamusConnection connection = await GetConnection();

        for (int i = 0; i < 10; i++)
        {
            using CamusCommand cmd = connection.CreateInsertCommand("robots");

            cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
            cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
            cmd.Parameters.Add("type", ColumnType.String, "mechanical");
            cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        }
    }

    private async Task CreateRow(CamusConnection connection)
    {
        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    private async Task CreateSqlRow(CamusConnection connection)
    {
        string sql = "INSERT INTO robots (id, name, year, type) VALUES (GEN_ID(), \"optimus prime\", 2017, \"transformer\")";

        using CamusCommand cmd = connection.CreateCamusCommand(sql);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestMultiInsertParallel()
    {
        CamusConnection connection = await GetConnection();

        List<Task> tasks = new();

        for (int j = 0; j < 100; j++)
            tasks.Add(CreateRow(connection));

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async void TestMultiSqlInsertParallel()
    {
        CamusConnection connection = await GetConnection();

        List<Task> tasks = new();

        for (int j = 0; j < 100; j++)
        {
            for (int i = 0; i < 100; i++)
                tasks.Add(CreateSqlRow(connection));

            await Task.WhenAll(tasks);
        }
    }

    [Fact]
    public async void TestBasicInsert()
    {
        CamusConnection connection = await GetConnection();

        string sql = "INSERT INTO robots (id, name, year, type) VALUES (GEN_ID(), @name, @year, @type)";

        using CamusCommand cmd = connection.CreateCamusCommand(sql);

        cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }
}
