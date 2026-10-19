/**
 * This file is part of CamusDB
 *
 * Offline coverage for CALL, which runs a procedure that a C# extension on the server defines: the
 * endpoint that each ADO.NET entry point picks, and the CALL statement that CommandType.StoredProcedure
 * composes. The HTTP calls are faked with Flurl's HttpTest, so no server is needed.
 */

using System.Data;
using System.Text.Json;
using CamusDB.Dapper;
using Dapper;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestProcedureCalls
{
    public TestProcedureCalls()
    {
        CamusDapper.Register();
    }

    /// <summary>The JSON body of the one call that the test made.</summary>
    private static async Task<JsonDocument> RequestBodyAsync(HttpTest httpTest)
        => JsonDocument.Parse(await Assert.Single(httpTest.CallLog).HttpRequestMessage.Content!.ReadAsStringAsync());

    private static CamusConnection Connection(int port)
        => new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test"));

    private static void RespondWithCount(HttpTest httpTest, int port, int rows)
        => httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-non-query").RespondWith($$"""{"status":"ok","rows":{{rows}}}""");

    [Fact]
    public async Task NonQueryCallReturnsTheChangedRowCount()
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, 9411, 2);

        using CamusConnection connection = Connection(9411);
        await using CamusCommand command = connection.CreateCamusCommand("CALL transfer('alice', 'bob', 30)");

        Assert.Equal(2, await command.ExecuteNonQueryAsync());

        using JsonDocument body = await RequestBodyAsync(httpTest);
        Assert.Equal("CALL transfer('alice', 'bob', 30)", body.RootElement.GetProperty("sql").GetString());
    }

    [Theory]
    [InlineData(9412, "CALL transfer('alice', 'bob', 30)")]
    [InlineData(9412, "  call transfer('alice', 'bob', 30)")]
    public async Task ReaderSendsCallToTheNonQueryEndpoint(int port, string sql)
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, port, 2);

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);
        Assert.False(await reader.ReadAsync());
        httpTest.ShouldNotHaveCalled($"http://localhost:{port}/execute-sql-query");
    }

    [Fact]
    public async Task StreamReaderSendsCallToTheNonQueryEndpoint()
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, 9413, 1);

        using CamusConnection connection = Connection(9413);
        await using CamusCommand command = connection.CreateCamusCommand("CALL refresh_totals()");
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.Equal(1, reader.RecordsAffected);
        Assert.False(await reader.ReadAsync());
        httpTest.ShouldNotHaveCalled("http://localhost:9413/execute-sql-query-stream");
    }

    [Fact]
    public async Task StoredProcedureComposesCallWithParametersInOrder()
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, 9414, 2);

        using CamusConnection connection = Connection(9414);
        await using CamusCommand command = connection.CreateCamusCommand("transfer");
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.Add("@from", ColumnType.String, "alice");
        command.Parameters.Add("to", ColumnType.String, "bob");
        command.Parameters.Add("@amount", ColumnType.Integer64, 30L);

        Assert.Equal(2, await command.ExecuteNonQueryAsync());

        using JsonDocument body = await RequestBodyAsync(httpTest);
        Assert.Equal("CALL transfer(@from, @to, @amount)", body.RootElement.GetProperty("sql").GetString());

        JsonElement parameters = body.RootElement.GetProperty("parameters");
        Assert.True(parameters.TryGetProperty("@from", out _));
        Assert.True(parameters.TryGetProperty("@to", out _));
        Assert.True(parameters.TryGetProperty("@amount", out _));
    }

    [Fact]
    public async Task StoredProcedureWithoutParametersCallsWithEmptyArguments()
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, 9415, 0);

        using CamusConnection connection = Connection(9415);
        await using CamusCommand command = connection.CreateCamusCommand(" refresh_totals ");
        command.CommandType = CommandType.StoredProcedure;

        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        using JsonDocument body = await RequestBodyAsync(httpTest);
        Assert.Equal("CALL refresh_totals()", body.RootElement.GetProperty("sql").GetString());
    }

    [Theory]
    [InlineData("transfer('a', 'b', 1)")]
    [InlineData("CALL transfer")]
    [InlineData("`transfer`")]
    [InlineData("")]
    public async Task StoredProcedureRefusesTextThatIsNotAName(string text)
    {
        using HttpTest httpTest = new();

        using CamusConnection connection = Connection(9416);
        await using CamusCommand command = connection.CreateCamusCommand(text);
        command.CommandType = CommandType.StoredProcedure;

        await Assert.ThrowsAsync<InvalidOperationException>(() => command.ExecuteNonQueryAsync());
        Assert.Empty(httpTest.CallLog);
    }

    [Fact]
    public async Task TableDirectIsNotSupported()
    {
        using HttpTest httpTest = new();

        using CamusConnection connection = Connection(9417);
        await using CamusCommand command = connection.CreateCamusCommand("robots");
        command.CommandType = CommandType.TableDirect;

        await Assert.ThrowsAsync<NotSupportedException>(() => command.ExecuteReaderAsync());
        Assert.Empty(httpTest.CallLog);
    }

    [Fact]
    public async Task DapperStoredProcedureBindsArgumentsInDeclarationOrder()
    {
        using HttpTest httpTest = new();
        RespondWithCount(httpTest, 9418, 2);

        using CamusConnection connection = Connection(9418);

        int rows = await connection.ExecuteAsync(
            "transfer", new { from = "alice", to = "bob", amount = 30L }, commandType: CommandType.StoredProcedure);

        Assert.Equal(2, rows);

        using JsonDocument body = await RequestBodyAsync(httpTest);
        Assert.Equal("CALL transfer(@from, @to, @amount)", body.RootElement.GetProperty("sql").GetString());
    }
}
