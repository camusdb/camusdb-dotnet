/**
 * This file is part of CamusDB
 *
 * Offline coverage for the statements that the server added with INSERT … ON CONFLICT and set
 * operations: the endpoint that each ADO.NET entry point picks for UPSERT, and the statements that the
 * driver prepares. The HTTP calls are faked with Flurl's HttpTest, so no server is needed.
 */

using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestUpsertRouting
{
    private static CamusConnection Connection(int port)
        => new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test"));

    [Theory]
    [InlineData(9471, "UPSERT INTO counters (k, n) VALUES ('a', 1)")]
    [InlineData(9472, "  upsert into counters (k, n) values ('a', 1)")]
    public async Task ReaderSendsUpsertToTheNonQueryEndpoint(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-non-query").RespondWith("""{"status":"ok","rows":1}""");

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(1, reader.RecordsAffected);
        Assert.False(await reader.ReadAsync());
        httpTest.ShouldNotHaveCalled($"http://localhost:{port}/execute-sql-query");
    }

    [Fact]
    public async Task ReaderKeepsTheReturningRowsOfAnUpsert()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9473/execute-sql-non-query").RespondWith(
            """{"status":"ok","rows":1,"columns":[{"name":"n","type":2}],"returningRows":[[7]]}""");

        using CamusConnection connection = Connection(9473);
        await using CamusCommand command = connection.CreateCamusCommand("UPSERT INTO counters (k, n) VALUES ('a', 7) RETURNING n");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(1, reader.RecordsAffected);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(7L, reader.GetInt64(0));
        httpTest.ShouldNotHaveCalled("http://localhost:9473/execute-sql-query");
    }

    [Theory]
    [InlineData(9474, "UPSERT INTO counters (k, n) VALUES (@k, @n)")]
    [InlineData(9475, "(SELECT k FROM counters ORDER BY k LIMIT 2) UNION ALL SELECT k FROM archive")]
    [InlineData(9476, "INSERT INTO counters (k, n) VALUES (@k, @n) ON CONFLICT (k) DO UPDATE SET n = excluded.n")]
    public async Task PrepareRegistersTheStatement(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/prepare-sql-statement").RespondWith(
            """{"status":"ok","statementId":"s1","parameterNames":[]}""");

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await command.PrepareAsync();

        httpTest.ShouldHaveCalled($"http://localhost:{port}/prepare-sql-statement").Times(1);
    }
}
