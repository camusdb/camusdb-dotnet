using System.Data;
using System.Data.Common;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestProviderFactory
{
    private const string ConnectionString = "Endpoint=http://localhost:8082;Database=test";

    private static Task<string> RequestBodyAsync(HttpTest httpTest)
        => Assert.Single(httpTest.CallLog).HttpRequestMessage.Content!.ReadAsStringAsync();

    [Fact]
    public void TestFactoryCreatesProviderTypes()
    {
        DbProviderFactory factory = CamusProviderFactory.Instance;

        Assert.IsType<CamusConnection>(factory.CreateConnection());
        Assert.IsType<CamusCommand>(factory.CreateCommand());
        Assert.IsType<CamusParameter>(factory.CreateParameter());
        Assert.IsType<CamusDataSource>(factory.CreateDataSource(ConnectionString));
        Assert.Null(factory.CreateConnectionStringBuilder());
    }

    [Fact]
    public void TestFactoryResolvesByInvariantName()
    {
        DbProviderFactories.RegisterFactory(CamusProviderFactory.InvariantName, typeof(CamusProviderFactory));

        Assert.Same(CamusProviderFactory.Instance, DbProviderFactories.GetFactory(CamusProviderFactory.InvariantName));
    }

    [Fact]
    public void TestFactoryConnectionAcceptsOneConnectionString()
    {
        using DbConnection connection = CamusProviderFactory.Instance.CreateConnection()!;

        Assert.Equal("", connection.ConnectionString);

        connection.ConnectionString = ConnectionString;

        Assert.Equal(ConnectionString, connection.ConnectionString);
        Assert.Equal("test", connection.Database);
        Assert.Equal("http://localhost:8082", connection.DataSource);

        // The same value again is accepted; a different one is not.
        connection.ConnectionString = ConnectionString;
        Assert.Throws<NotSupportedException>(() => connection.ConnectionString = "Endpoint=http://localhost:9000;Database=other");

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public void TestConnectionBuiltFromAStringStaysFixed()
    {
        using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnectionString));

        Assert.Throws<NotSupportedException>(() => connection.ConnectionString = "Endpoint=http://localhost:9000;Database=other");
        Assert.Equal(ConnectionString, connection.ConnectionString);
    }

    [Fact]
    public void TestRejectedConnectionStringLeavesTheConnectionUnset()
    {
        using CamusConnection connection = new();

        // Credentials against a remote plaintext endpoint are refused while the string is parsed.
        Assert.Throws<CamusException>(() => connection.ConnectionString = "Endpoint=http://db.example.com:8082;Database=test;User=a;Password=b");

        Assert.Equal("", connection.ConnectionString);

        connection.ConnectionString = ConnectionString;
        Assert.Equal("test", connection.Database);
    }

    [Fact]
    public async Task TestFactoryCommandRunsOnTheConnectionItIsGiven()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 1 });

        DbConnection connection = CamusProviderFactory.Instance.CreateConnection()!;
        connection.ConnectionString = "Endpoint=http://localhost:8084;Database=test";

        await using DbCommand command = CamusProviderFactory.Instance.CreateCommand()!;
        command.CommandText = "INSERT INTO robots (name) VALUES ('R2-D2')";
        command.Connection = connection;

        int rows = await command.ExecuteNonQueryAsync();

        Assert.Equal(1, rows);
        httpTest.ShouldHaveCalled("http://localhost:8084/execute-sql-non-query").Times(1);
        Assert.Contains("\"databaseName\":\"test\"", await RequestBodyAsync(httpTest));
    }

    [Fact]
    public async Task TestCommandFollowsAConnectionStringAssignedAfterItWasCreated()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 1 });

        using CamusConnection connection = new();
        await using DbCommand command = connection.CreateCommand();

        connection.ConnectionString = "Endpoint=http://localhost:8086;Database=test";
        command.CommandText = "DELETE FROM robots WHERE id = 1";

        await command.ExecuteNonQueryAsync();

        httpTest.ShouldHaveCalled("http://localhost:8086/execute-sql-non-query").Times(1);
    }

    [Fact]
    public async Task TestCommandMovedToAnotherConnectionRunsThere()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 1 });

        using CamusConnection first = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:8082;Database=one"));
        using CamusConnection second = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:8088;Database=two"));

        await using CamusCommand command = first.CreateCamusCommand("DELETE FROM robots WHERE id = 1");
        command.Connection = second;

        await command.ExecuteNonQueryAsync();

        httpTest.ShouldHaveCalled("http://localhost:8088/execute-sql-non-query").Times(1);
        Assert.Contains("\"databaseName\":\"two\"", await RequestBodyAsync(httpTest));
        httpTest.ShouldNotHaveCalled("http://localhost:8082/*");
    }

    [Fact]
    public void TestDataSourceConnectionsDoNotShareTheirDatabase()
    {
        using CamusDataSource dataSource = new(ConnectionString);

        using CamusConnection first = dataSource.CreateConnection();
        using CamusConnection second = dataSource.CreateConnection();

        first.ChangeDatabase("other");

        Assert.Equal("other", first.Database);
        Assert.Equal("test", second.Database);
        Assert.Equal(ConnectionString, dataSource.ConnectionString);
        Assert.Equal(ConnectionString, second.ConnectionString);
    }

    [Fact]
    public async Task TestDataSourceCommandOpensItsOwnConnection()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 3 });

        await using DbDataSource dataSource = CamusProviderFactory.Instance.CreateDataSource(ConnectionString);
        await using DbCommand command = dataSource.CreateCommand("UPDATE robots SET name = 'x'");

        Assert.Equal(3, await command.ExecuteNonQueryAsync());
        httpTest.ShouldHaveCalled("http://localhost:8082/execute-sql-non-query").Times(1);
    }

    [Fact]
    public async Task TestDataSourceOpensConnections()
    {
        await using DbDataSource dataSource = CamusProviderFactory.Instance.CreateDataSource(ConnectionString);
        await using DbConnection connection = await dataSource.OpenConnectionAsync();

        Assert.IsType<CamusConnection>(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public void TestDataSourceRejectsAnInsecureConnectionStringAtOnce()
    {
        Assert.Throws<CamusException>(() => new CamusDataSource("Endpoint=http://db.example.com:8082;Database=test;User=a;Password=b"));
    }
}
