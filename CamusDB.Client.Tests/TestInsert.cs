﻿
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

        cmd.Parameters.Add("id", ColumnType.Id, ObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, "aaa");
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, 2000);        

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestMultiInsert()
    {
        CamusConnection connection = await GetConnection();

        for (int i = 0; i < 100; i++)
        {
            using CamusCommand cmd = connection.CreateInsertCommand("robots");

            cmd.Parameters.Add("id", ColumnType.Id, ObjectIdGenerator.Generate());
            cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
            cmd.Parameters.Add("type", ColumnType.String, "mechanical");
            cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
        }
    }

    private async Task CreateRow(CamusConnection connection)
    {
        using CamusCommand cmd = connection.CreateInsertCommand("robots");

        cmd.Parameters.Add("id", ColumnType.Id, ObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    private async Task CreateSqlRow(CamusConnection connection)
    {
        string sql = "INSERT INTO robots (id, name, year, type) VALUES (GEN_ID(), \"optimus prime\", 2017, \"transformer\")";

        using CamusCommand cmd = new CamusCommand(sql, builder!);

        /*cmd.Parameters.Add("id", ColumnType.Id, ObjectIdGenerator.Generate());
        cmd.Parameters.Add("name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));*/

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async void TestMultiInsertParallel()
    {
        CamusConnection connection = await GetConnection();

        List<Task> tasks = new();

        for (int j = 0; j < 100; j++)
        {
            for (int i = 0; i < 100; i++)
                tasks.Add(CreateRow(connection));

            await Task.WhenAll(tasks);
        }
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
}
