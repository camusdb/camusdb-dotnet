
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
    private readonly string[] types = { "mechanical", "electronic", "cyborg" };

    public TestInsert()
    {
    }

    [Fact]
    public async void TestSimpleInsert()
    {
        CamusConnection connection = await GetConnection();

        await using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);
        cmd.Parameters.Add("price", ColumnType.Float64, 10.0);
        cmd.Parameters.Add("enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestInsertNulls()
    {
        CamusConnection connection = await GetConnection();

        await using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.Null, null);
        cmd.Parameters.Add("year", ColumnType.Null, null);
        cmd.Parameters.Add("price", ColumnType.Null, null);
        cmd.Parameters.Add("enabled", ColumnType.Null, null);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestMultiInsert()
    {
        CamusConnection connection = await GetConnection();

        for (int i = 0; i < 10; i++)
        {
            await using CamusCommand cmd = connection.CreateInsertCommand("robots");

            cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
            cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
            cmd.Parameters.Add("type", ColumnType.String, "mechanical");
            cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
            cmd.Parameters.Add("price", ColumnType.Float64, Random.Shared.NextDouble());
            cmd.Parameters.Add("enabled", ColumnType.Bool, true);

            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        }
    }

    private async Task CreateRow(CamusConnection connection)
    {
        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("type", ColumnType.String, types[Random.Shared.Next(0, types.Length - 1)]);
        cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
        cmd.Parameters.Add("price", ColumnType.Float64, 10 * Random.Shared.NextDouble());
        cmd.Parameters.Add("enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    private async Task CreateSqlRow(CamusConnection connection)
    {
        const string sql = "INSERT INTO robots (id, name, year, type, price, enabled) VALUES (GEN_ID(), @name, @year, @type, @price, @enabled)";

        await using CamusCommand cmd = connection.CreateCamusCommand(sql);
        
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, types[Random.Shared.Next(0, types.Length)]);
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
        cmd.Parameters.Add("@price", ColumnType.Float64, 10 * Random.Shared.NextDouble());
        cmd.Parameters.Add("@enabled", ColumnType.Bool, true);

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
            tasks.Clear();

            for (int i = 0; i < 300; i++)
                tasks.Add(CreateSqlRow(connection));

            await Task.WhenAll(tasks);
        }
    }

    [Fact]
    public async void TestBasicInsert()
    {
        CamusConnection connection = await GetConnection();

        const string sql = "INSERT INTO robots (id, name, year, type, price, enabled) VALUES (GEN_ID(), @name, @year, @type, @price, @enabled)";

        await using CamusCommand cmd = connection.CreateCamusCommand(sql);

        cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
        cmd.Parameters.Add("@price", ColumnType.Float64, 10 * Random.Shared.NextDouble());
        cmd.Parameters.Add("@enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }
}
