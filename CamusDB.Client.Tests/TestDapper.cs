/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Dapper;
using Dapper;

namespace CamusDB.Client.Tests;

public sealed class TestDapper : BaseTest
{
    public sealed class Robot
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public string? Type { get; set; }
        public int? Year { get; set; }
        public decimal? Price { get; set; }
        public bool? Enabled { get; set; }
    }

    public sealed class Event
    {
        public CamusObjectIdValue Id { get; set; }
        public Guid Ref { get; set; }
        public DateTime Happened { get; set; }
        public DateOnly Day { get; set; }
        public byte[]? Payload { get; set; }
        public float Score { get; set; }
    }

    public sealed class Tagged
    {
        public string Id { get; set; } = "";
        public long[]? Tags { get; set; }
    }

    public TestDapper()
    {
        CamusDapper.Register();
    }

    private static Task<string> CreateEventsTableAsync(CamusConnection connection) =>
        CreateTempTableAsync(
            connection,
            "events",
            " id OID PRIMARY KEY NOT NULL," +
            " ref UUID NOT NULL," +
            " happened DATETIME NOT NULL," +
            " day DATE NOT NULL," +
            " payload BYTES," +
            " score FLOAT32 NOT NULL");

    [Fact]
    public async Task QueryMapsRowsWithBareParameterNames()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "R2-D2", year: 1977, price: 25.5);
        await InsertRobotAsync(connection, table, name: "T-800", type: null, year: 1984, enabled: null);

        List<Robot> robots = (await connection.QueryAsync<Robot>(
            $"SELECT id, name, type, year, price, enabled FROM {table} WHERE year >= @from ORDER BY year",
            new { from = 1977 })).ToList();

        Assert.Equal(2, robots.Count);
        Assert.Equal("R2-D2", robots[0].Name);
        Assert.Equal(1977, robots[0].Year);
        Assert.Equal(25.5m, robots[0].Price);
        Assert.True(robots[0].Enabled);
        Assert.Equal(24, robots[0].Id.Length);

        Assert.Equal("T-800", robots[1].Name);
        Assert.Null(robots[1].Type);
        Assert.Null(robots[1].Enabled);
    }

    [Fact]
    public async Task ExecuteInsertsOneRowPerElement()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        int inserted = await connection.ExecuteAsync(
            $"INSERT INTO {table} (id, name, type, year, price, enabled) VALUES (GEN_ID(), @Name, @Type, @Year, @Price, @Enabled)",
            new[]
            {
                new { Name = "a", Type = (string?)"x", Year = 1, Price = 1.5, Enabled = true },
                new { Name = "b", Type = (string?)null, Year = 2, Price = 2.5, Enabled = false },
            });

        Assert.Equal(2, inserted);

        long count = await connection.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM {table}");
        Assert.Equal(2, count);

        Robot? b = await connection.QuerySingleOrDefaultAsync<Robot>(
            $"SELECT * FROM {table} WHERE name = @name", new { name = "b" });

        Assert.NotNull(b);
        Assert.Null(b.Type);
        Assert.False(b.Enabled);

        Robot? missing = await connection.QuerySingleOrDefaultAsync<Robot>(
            $"SELECT * FROM {table} WHERE name = @name", new { name = "zzz" });

        Assert.Null(missing);
    }

    [Fact]
    public async Task ExecuteUpdatesAndDeletesReturnAffectedRows()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "a", year: 2000);
        await InsertRobotAsync(connection, table, name: "b", year: 2000);

        Assert.Equal(2, await connection.ExecuteAsync(
            $"UPDATE {table} SET year = @year WHERE year = @old", new { year = 2001, old = 2000 }));

        Assert.Equal(1, await connection.ExecuteAsync(
            $"DELETE FROM {table} WHERE name = @name", new { name = "a" }));

        Assert.Equal(1, connection.ExecuteScalar<long>($"SELECT COUNT(*) FROM {table} WHERE year = @year", new { year = 2001 }));
    }

    [Fact]
    public async Task InListExpandsEnumerableParameters()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "a");
        await InsertRobotAsync(connection, table, name: "b");
        await InsertRobotAsync(connection, table, name: "c");

        IEnumerable<string> names = await connection.QueryAsync<string>(
            $"SELECT name FROM {table} WHERE name IN @names ORDER BY name",
            new { names = new List<string> { "a", "c" } });

        Assert.Equal(["a", "c"], names);
    }

    [Fact]
    public async Task ArrayValueBindsAsOneNativeArray()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "a", year: 1977);
        await InsertRobotAsync(connection, table, name: "b", year: 1984);
        await InsertRobotAsync(connection, table, name: "c", year: 2001);

        IEnumerable<string> names = await connection.QueryAsync<string>(
            $"SELECT name FROM {table} WHERE year = ANY(@years) ORDER BY name",
            new { years = CamusValue.Array(new long[] { 1977, 2001 }) });

        Assert.Equal(["a", "c"], names);

        IEnumerable<string> none = await connection.QueryAsync<string>(
            $"SELECT name FROM {table} WHERE year = ANY(@years)",
            new { years = CamusValue.Array(Array.Empty<long>()) });

        Assert.Empty(none);
    }

    [Fact]
    public async Task DynamicParametersAcceptCamusValues()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        string id = CamusObjectIdGenerator.GenerateAsString();
        await InsertRobotAsync(connection, table, id: id, name: "a");

        DynamicParameters parameters = new();
        parameters.Add("id", CamusValue.Id(id));

        Robot robot = await connection.QuerySingleAsync<Robot>($"SELECT * FROM {table} WHERE id = @id", parameters);

        Assert.Equal(id, robot.Id);
    }

    [Fact]
    public async Task TypedColumnsRoundTrip()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateEventsTableAsync(connection);

        Event written = new()
        {
            Id = CamusObjectIdGenerator.Generate(),
            Ref = Guid.NewGuid(),
            Happened = new DateTime(2026, 5, 1, 10, 30, 0, DateTimeKind.Utc),
            Day = new DateOnly(2026, 5, 1),
            Payload = [0xDE, 0xAD],
            Score = 9.5f,
        };

        Assert.Equal(1, await connection.ExecuteAsync(
            $"INSERT INTO {table} (id, ref, happened, day, payload, score) VALUES (@Id, @Ref, @Happened, @Day, @Payload, @Score)",
            written));

        Event read = await connection.QuerySingleAsync<Event>(
            $"SELECT * FROM {table} WHERE ref = @Ref", new { written.Ref });

        Assert.Equal(written.Id, read.Id);
        Assert.Equal(written.Ref, read.Ref);
        Assert.Equal(written.Happened, read.Happened);
        Assert.Equal(written.Day, read.Day);
        Assert.Equal(written.Payload, read.Payload);
        Assert.Equal(written.Score, read.Score);

        Event byId = await connection.QuerySingleAsync<Event>(
            $"SELECT * FROM {table} WHERE id = @Id", new { written.Id });

        Assert.Equal(written.Ref, byId.Ref);
    }

    [Fact]
    public async Task ArrayColumnReadsThroughARegisteredHandler()
    {
        CamusDapper.RegisterArrayTypeHandler<long>();

        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempTableAsync(connection, "tagged", " id OID PRIMARY KEY NOT NULL, tags ARRAY(INT64)");

        await connection.ExecuteAsync(
            $"INSERT INTO {table} (id, tags) VALUES (GEN_ID(), @tags)", new { tags = new long[] { 1, 2, 3 } });

        await connection.ExecuteAsync(
            $"INSERT INTO {table} (id, tags) VALUES (GEN_ID(), @tags)", new { tags = (long[]?)null });

        List<Tagged> rows = (await connection.QueryAsync<Tagged>($"SELECT id, tags FROM {table}")).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Tags is [1, 2, 3]);
        Assert.Contains(rows, r => r.Tags is null);
    }

    [Fact]
    public async Task RolledBackTransactionLeavesNoRows()
    {
        await using CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        await using (CamusTransaction transaction = await connection.BeginTransactionAsync())
        {
            await connection.ExecuteAsync(
                $"INSERT INTO {table} (id, name) VALUES (GEN_ID(), @name)", new { name = "a" }, transaction);

            Assert.Equal(1, await connection.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {table}", transaction: transaction));

            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await connection.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM {table}"));
    }
}
