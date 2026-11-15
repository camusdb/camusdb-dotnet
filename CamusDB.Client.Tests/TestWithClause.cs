/**
 * This file is part of CamusDB
 *
 * Offline coverage for a leading WITH clause (server commit b78d8428). The server runs
 * WITH s AS (…) UPDATE … as an UPDATE and refuses it on the query endpoint (CADB0407), so the driver
 * reads the statement after the clause to pick the endpoint, the prepare decision and the span name.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestWithClause
{
    [Theory]
    [InlineData("SELECT 1", "SELECT 1")]
    [InlineData("  \n SELECT 1", "SELECT 1")]
    [InlineData("-- note\n/* more */ UPDATE t SET a = 1", "UPDATE t SET a = 1")]
    [InlineData("WITH s AS (SELECT 1) SELECT * FROM s", "SELECT * FROM s")]
    [InlineData("with s as (select 1) update t set a = 1", "update t set a = 1")]
    [InlineData("WITH s (a, b) AS (SELECT 1, 2) SELECT a FROM s", "SELECT a FROM s")]
    [InlineData("WITH s AS MATERIALIZED (SELECT 1), u AS NOT MATERIALIZED (SELECT 2) DELETE FROM t WHERE TRUE", "DELETE FROM t WHERE TRUE")]
    [InlineData("WITH `my cte` AS (SELECT ')' AS p, \"(\" AS q, $$ ) $$ AS r) INSERT INTO t SELECT * FROM `my cte`", "INSERT INTO t SELECT * FROM `my cte`")]
    [InlineData("WITH s AS (SELECT (1 + (2)) AS x /* ) */ -- )\n), u AS (SELECT * FROM s) UPSERT INTO t SELECT * FROM u", "UPSERT INTO t SELECT * FROM u")]
    [InlineData("WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r", "SELECT * FROM r")]
    [InlineData("WITH s AS (SELECT 1) (SELECT * FROM s) UNION SELECT 2", "(SELECT * FROM s) UNION SELECT 2")]
    [InlineData("WITHIN", "WITHIN")]
    public void SkipLeadingWith_ReturnsTheStatementAfterTheClause(string sql, string expected)
        => Assert.Equal(expected, CamusSqlSyntax.SkipLeadingWith(sql).ToString());

    [Theory]
    [InlineData("WITH")]
    [InlineData("WITH s")]
    [InlineData("WITH s AS SELECT 1")]
    [InlineData("WITH s AS (SELECT 1")]
    [InlineData("WITH 1s AS (SELECT 1) SELECT 1")]
    [InlineData("WITH s AS (SELECT 1),")]
    public void SkipLeadingWith_LeavesTextThatIsNotAClauseAsItIs(string sql)
        => Assert.Equal(sql, CamusSqlSyntax.SkipLeadingWith(sql).ToString());

    private static CamusConnection Connect()
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:1;Database=db_{CamusObjectIdGenerator.Generate()};MaxAutoPrepare=0");
        builder.UseTransport(new TestTelemetry.FakeTransport());

        CamusConnection connection = new(builder);
        connection.Open();
        return connection;
    }

    // The fake transport answers a query with two rows and a non-query with three affected rows, so the
    // reader shows which endpoint the command used.
    [Theory]
    [InlineData("WITH s AS (SELECT 1 AS id) UPDATE t SET n = 1 WHERE id IN (SELECT id FROM s)")]
    [InlineData("WITH s AS (SELECT 1 AS id) DELETE FROM t WHERE id IN (SELECT id FROM s)")]
    [InlineData("WITH s AS (SELECT 1 AS id) INSERT INTO t SELECT id FROM s")]
    [InlineData("WITH s AS (SELECT 1 AS id) UPSERT INTO t SELECT id FROM s")]
    public async Task ReaderSendsAWriteAfterWithToTheNonQueryEndpoint(string sql)
    {
        await using CamusConnection connection = Connect();
        await using CamusCommand command = connection.CreateCamusCommand(sql);

        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.False(await reader.ReadAsync());
        Assert.Equal(3, reader.RecordsAffected);
    }

    [Fact]
    public async Task StreamReaderSendsAWriteAfterWithToTheNonQueryEndpoint()
    {
        await using CamusConnection connection = Connect();
        await using CamusCommand command = connection.CreateCamusCommand(
            "WITH s AS (SELECT 1 AS id) UPDATE t SET n = 1 WHERE id IN (SELECT id FROM s)");

        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.False(await reader.ReadAsync());
        Assert.Equal(3, reader.RecordsAffected);
    }

    [Fact]
    public async Task ReaderSendsASelectAfterWithToTheQueryEndpoint()
    {
        await using CamusConnection connection = Connect();
        await using CamusCommand command = connection.CreateCamusCommand("WITH s AS (SELECT 'a' AS name) SELECT name FROM s");

        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("a", reader.GetString(0));
    }
}
