using System.Data;
using System.Data.Common;
using Flurl.Http;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestProviderSurface
{
    [Fact]
    public void TestConnectionOpenCloseAndMetadata()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082,http://localhost:8084;Database=test");
        using CamusConnection connection = new(builder);

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Equal("test", connection.Database);
        Assert.Equal("Endpoint=http://localhost:8082,http://localhost:8084;Database=test", connection.ConnectionString);
        Assert.Equal("http://localhost:8082,http://localhost:8084", connection.DataSource);

        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);

        connection.ChangeDatabase("other");

        Assert.Equal("other", connection.Database);

        connection.Close();

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void TestCreateCommandWiresConnectionAndParameterCollection()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
        using CamusConnection connection = new(builder);

        using DbCommand command = connection.CreateCommand();

        Assert.IsType<CamusCommand>(command);
        Assert.Same(connection, command.Connection);
        Assert.IsType<CamusParameterCollection>(command.Parameters);

        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = "@id";
        parameter.Value = 7L;

        command.Parameters.Add(parameter);

        Assert.Single(command.Parameters);
        Assert.Equal(parameter, command.Parameters[0]);
    }

    [Fact]
    public void TestParameterCollectionCrud()
    {
        CamusParameterCollection parameters = new();

        CamusParameter id = parameters.Add("@id", ColumnType.Integer64, 7L);
        CamusParameter name = parameters.Add("@name", ColumnType.String, "abc");

        Assert.True(parameters.Contains(id));
        Assert.True(parameters.Contains("@name"));
        Assert.Equal(0, parameters.IndexOf(id));
        Assert.Equal(1, parameters.IndexOf("@name"));

        parameters.RemoveAt("@id");
        Assert.False(parameters.Contains("@id"));

        parameters.Insert(0, id);
        Assert.Equal(id, parameters[0]);

        parameters.Clear();
        Assert.Empty(parameters.Cast<CamusParameter>());
    }

    [Fact]
    public void TestParameterCloneAndDbTypeMapping()
    {
        CamusParameter parameter = new("@enabled", ColumnType.Bool, true)
        {
            IsNullable = true,
            Size = 32,
            SourceColumn = "enabled",
            SourceColumnNullMapping = true
        };

        Assert.Equal(DbType.Boolean, parameter.DbType);

        CamusParameter clone = (CamusParameter)parameter.Clone();

        Assert.Equal(parameter.ParameterName, clone.ParameterName);
        Assert.Equal(parameter.ColumnType, clone.ColumnType);
        Assert.Equal(parameter.Value, clone.Value);
        Assert.Equal(parameter.SourceColumn, clone.SourceColumn);
        Assert.Equal(parameter.DbType, clone.DbType);
    }

    [Fact]
    public void TestParameterDbTypeMappings()
    {
        Assert.Equal(ColumnType.Integer64, new CamusParameter { DbType = DbType.Int64 }.ColumnType);
        Assert.Equal(ColumnType.Float64, new CamusParameter { DbType = DbType.Double }.ColumnType);
        Assert.Equal(ColumnType.Id, new CamusParameter { DbType = DbType.Guid }.ColumnType);
        Assert.Equal(ColumnType.Bool, new CamusParameter { DbType = DbType.Boolean }.ColumnType);
        Assert.Equal(ColumnType.String, new CamusParameter { DbType = DbType.String }.ColumnType);
    }

    [Fact]
    public void TestDataReaderBasicAccessors()
    {
        List<Dictionary<string, ColumnValue>> rows =
        [
            new()
            {
                ["id"] = new() { Type = ColumnType.Id, StrValue = "a" },
                ["name"] = new() { Type = ColumnType.String, StrValue = "robot" },
                ["enabled"] = new() { Type = ColumnType.Bool, BoolValue = true },
                ["year"] = new() { Type = ColumnType.Integer64, LongValue = 1977 },
                ["price"] = new() { Type = ColumnType.Float64, FloatValue = 12.5 }
            }
        ];

        using CamusDataReader reader = new(rows);

        Assert.True(reader.HasRows);
        Assert.Equal(5, reader.FieldCount);
        Assert.True(reader.Read());
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal("a", reader.GetString(0));
        Assert.Equal("robot", reader["name"]);
        Assert.True(reader.GetBoolean(2));
        Assert.Equal(1977L, reader.GetInt64(3));
        Assert.Equal(12.5, reader.GetDouble(4));
        Assert.Equal(typeof(double), reader.GetFieldType(4));
        Assert.False(reader.Read());
    }

    [Fact]
    public async Task TestDataReaderNullHandlingAndSyncAsyncParity()
    {
        List<Dictionary<string, ColumnValue>> rows =
        [
            new()
            {
                ["name"] = new() { Type = ColumnType.String, StrValue = "robot" },
                ["notes"] = new() { Type = ColumnType.Null }
            },
            new()
            {
                ["name"] = new() { Type = ColumnType.String, StrValue = "droid" },
                ["notes"] = new() { Type = ColumnType.String, StrValue = "active" }
            }
        ];

        using CamusDataReader reader = new(rows);

        Assert.True(await reader.ReadAsync());
        Assert.Equal("robot", reader.GetString(reader.GetOrdinal("name")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("notes")));
        Assert.Equal(DBNull.Value, reader.GetValue(reader.GetOrdinal("notes")));

        Assert.True(reader.Read());
        Assert.Equal("active", reader.GetString(reader.GetOrdinal("notes")));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public void TestExecuteScalarUsesFirstColumnOfFirstRow()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
        using ScalarTestCommand command = new(builder);

        object? value = command.ExecuteScalar();

        Assert.Equal("first", value);
    }

    [Fact]
    public void TestTransactionMetadataUsesPinnedEndpoint()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082,http://localhost:8084;Database=test");
        using CamusConnection connection = new(builder);
        CamusTransaction transaction = new(10, 20, "http://localhost:8084", connection, builder);

        Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);
        Assert.Equal("10:20", transaction.TransactionId);
        Assert.Same(connection, transaction.Connection);
    }

    [Fact]
    public async Task TestEndpointPoolMarksUnreachableTransportFailures()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:8082/*")
            .SimulateTimeout();

        httpTest
            .ForCallsTo("http://localhost:8084/*")
            .RespondWithJson(new { status = "ok", rows = 1 });

        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082,http://localhost:8084;Database=test");
        using CamusConnection connection = new(builder);

        await using CamusCommand firstPing = connection.CreatePingCommand();
        await Assert.ThrowsAsync<CamusException>(() => firstPing.ExecuteNonQueryAsync());

        await using CamusCommand secondPing = connection.CreatePingCommand();
        await using CamusCommand thirdPing = connection.CreatePingCommand();

        Assert.Equal(1, await secondPing.ExecuteNonQueryAsync());
        Assert.Equal(1, await thirdPing.ExecuteNonQueryAsync());

        httpTest.ShouldHaveCalled("http://localhost:8082/ping").Times(1);
        httpTest.ShouldHaveCalled("http://localhost:8084/ping").Times(2);
    }

    [Fact]
    public async Task TestTransactionCommandsStayOnPinnedEndpoint()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:8082/start-transaction")
            .RespondWithJson(new { status = "ok", txnIdPT = 10, txnIdCounter = 20u });

        httpTest
            .ForCallsTo("http://localhost:8082/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 1 });

        httpTest
            .ForCallsTo("http://localhost:8082/commit-transaction")
            .RespondWithJson(new { status = "ok", txnIdPT = 10, txnIdCounter = 20u });

        httpTest
            .ForCallsTo("http://localhost:8084/*")
            .RespondWithJson(new { status = "ok", rows = 1 });

        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082,http://localhost:8084;Database=test");
        using CamusConnection connection = new(builder);

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        await using CamusCommand command = connection.CreateCamusCommand("INSERT INTO robots (name) VALUES (@name)");
        command.Transaction = transaction;
        command.Parameters.Add("@name", ColumnType.String, "r2");

        Assert.Equal(1, await command.ExecuteNonQueryAsync());
        await transaction.CommitAsync();

        httpTest.ShouldHaveCalled("http://localhost:8082/start-transaction").Times(1);
        httpTest.ShouldHaveCalled("http://localhost:8082/execute-sql-non-query").Times(1);
        httpTest.ShouldHaveCalled("http://localhost:8082/commit-transaction").Times(1);
        httpTest.ShouldNotHaveCalled("http://localhost:8084/execute-sql-non-query");
    }

    [Theory]
    [InlineData("CREATE TABLE robots (id OID PRIMARY KEY NOT NULL)")]
    [InlineData("DROP TABLE robots")]
    [InlineData("ALTER TABLE robots ADD COLUMN name STRING NOT NULL")]
    [InlineData("ALTER TABLE robots DROP COLUMN name")]
    [InlineData("CREATE INDEX idx ON robots (name)")]
    [InlineData("CREATE UNIQUE INDEX idx ON robots (name)")]
    [InlineData("  create table robots (id OID PRIMARY KEY NOT NULL)")]  // leading whitespace + lowercase
    public async Task TestExecuteNonQueryRoutesDdlToExecuteSqlDdl(string sql)
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-ddl")
            .RespondWithJson(new { status = "ok" });

        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
        using CamusConnection connection = new(builder);
        await using CamusCommand command = connection.CreateCamusCommand(sql);

        int rows = await command.ExecuteNonQueryAsync();

        Assert.Equal(0, rows);
        httpTest.ShouldHaveCalled("http://localhost:8082/execute-sql-ddl").Times(1);
        httpTest.ShouldNotHaveCalled("http://localhost:8082/execute-sql-non-query");
    }

    [Theory]
    [InlineData("INSERT INTO robots (name) VALUES (@name)")]
    [InlineData("UPDATE robots SET name = @name WHERE id = @id")]
    [InlineData("DELETE FROM robots WHERE id = @id")]
    public async Task TestExecuteNonQueryRoutesDmlToExecuteSqlNonQuery(string sql)
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("*/execute-sql-non-query")
            .RespondWithJson(new { status = "ok", rows = 1 });

        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");
        using CamusConnection connection = new(builder);
        await using CamusCommand command = connection.CreateCamusCommand(sql);

        int rows = await command.ExecuteNonQueryAsync();

        Assert.Equal(1, rows);
        httpTest.ShouldHaveCalled("http://localhost:8082/execute-sql-non-query").Times(1);
        httpTest.ShouldNotHaveCalled("http://localhost:8082/execute-sql-ddl");
    }

    private sealed class ScalarTestCommand : CamusCommand
    {
        public ScalarTestCommand(CamusConnectionStringBuilder builder) : base("SELECT 1", builder)
        {
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
            new CamusDataReader(
            [
                new()
                {
                    ["value"] = new() { Type = ColumnType.String, StrValue = "first" },
                    ["other"] = new() { Type = ColumnType.Integer64, LongValue = 2 }
                }
            ]);

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult<DbDataReader>(ExecuteDbDataReader(behavior));
    }
}
