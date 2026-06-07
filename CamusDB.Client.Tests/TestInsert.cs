
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
    public async Task TestSimpleInsert()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        await using CamusCommand cmd = connection.CreateInsertCommand(tableName);

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);
        cmd.Parameters.Add("price", ColumnType.Float64, 10.0);
        cmd.Parameters.Add("enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task TestInsertNulls()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        await using CamusCommand cmd = connection.CreateInsertCommand(tableName);

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.Null, null);
        cmd.Parameters.Add("year", ColumnType.Null, null);
        cmd.Parameters.Add("price", ColumnType.Null, null);
        cmd.Parameters.Add("enabled", ColumnType.Null, null);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task TestMultiInsert()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        for (int i = 0; i < 10; i++)
        {
            await using CamusCommand cmd = connection.CreateInsertCommand(tableName);

            cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
            cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
            cmd.Parameters.Add("type", ColumnType.String, "mechanical");
            cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
            cmd.Parameters.Add("price", ColumnType.Float64, Random.Shared.NextDouble());
            cmd.Parameters.Add("enabled", ColumnType.Bool, true);

            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        }
    }

    private async Task CreateRow(CamusConnection connection, string tableName)
    {
        using CamusCommand cmd = connection.CreateInsertCommand(tableName);

        cmd.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("type", ColumnType.String, types[Random.Shared.Next(0, types.Length - 1)]);
        cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
        cmd.Parameters.Add("price", ColumnType.Float64, 10 * Random.Shared.NextDouble());
        cmd.Parameters.Add("enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    private async Task CreateSqlRow(CamusConnection connection, string tableName)
    {
        string sql = $"INSERT INTO {tableName} (id, name, year, type, price, enabled) VALUES (GEN_ID(), @name, @year, @type, @price, @enabled)";

        await using CamusCommand cmd = connection.CreateCamusCommand(sql);
        
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, types[Random.Shared.Next(0, types.Length)]);
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));
        cmd.Parameters.Add("@price", ColumnType.Float64, 10 * Random.Shared.NextDouble());
        cmd.Parameters.Add("@enabled", ColumnType.Bool, true);

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task TestMultiInsertParallel()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        List<Task> tasks = [];

        for (int j = 0; j < 100; j++)
            tasks.Add(CreateRow(connection, tableName));

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task TestMultiSqlInsertParallel()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        List<Task> tasks = [];

        for (int j = 0; j < 10; j++)
        {
            tasks.Clear();

            for (int i = 0; i < 30; i++)
                tasks.Add(CreateSqlRow(connection, tableName));

            await Task.WhenAll(tasks);
        }
    }

    [Fact]
    public async Task TestBasicInsert()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        string sql = $"INSERT INTO {tableName} (id, name, year, type, price, enabled) VALUES (GEN_ID(), @name, @year, @type, @price, @enabled)";

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
