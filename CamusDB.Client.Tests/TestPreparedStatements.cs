
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

/// <summary>
/// Prepared statements end to end, over both transports.
///
/// <para>What every test here is really asserting is that a prepared execution is
/// <em>indistinguishable</em> from an inline one: same rows, same affected counts, same transaction and
/// isolation behavior. The optimization is only worth having if it changes nothing a caller can observe
/// except how much travels on the wire, so that — rather than the presence of a handle — is what is
/// checked.</para>
/// </summary>
public class TestPreparedStatements : BaseTest
{
    private static CamusConnectionStringBuilder BuilderFor(string extra = "")
        => new($"Endpoint=http://localhost:5095;Database=test{extra}");

    private static CamusConnectionStringBuilder GrpcBuilderFor(string extra = "")
        => new($"Endpoint=http://localhost:5096;Database=test;Protocol=grpc{extra}");

    private static async Task<CamusConnection> OpenAsync(CamusConnectionStringBuilder builder)
    {
        CamusConnection connection = new(builder);
        await connection.OpenAsync();
        return connection;
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task TestExplicitPrepareThenExecuteRepeatedly(string protocol)
    {
        // Every test gets a table name of its own, so its SQL is unique and its assertions are unaffected
        // by the auto-prepare decisions other tests make — the policy is shared per deployment, so
        // counting anything process-wide would be counting the whole suite.
        CamusConnectionStringBuilder builder = protocol == "grpc" ? GrpcBuilderFor() : BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)");

        await insert.PrepareAsync();

        for (int i = 0; i < 5; i++)
        {
            insert.Parameters.Clear();
            insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("@name", ColumnType.String, "robot" + i);
            insert.Parameters.Add("@year", ColumnType.Integer64, 2000 + i);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT name, year FROM {table} WHERE year = @year");

        await select.PrepareAsync();

        select.Parameters.Add("@year", ColumnType.Integer64, 2003);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("robot3", reader.GetString(0));
        Assert.Equal(2003, reader.GetInt64(1));
        Assert.False(await reader.ReadAsync());
    }

    /// <summary>
    /// The path Entity Framework actually takes: nothing calls <c>Prepare</c>, the same SQL simply runs
    /// again, and from the second execution on it is prepared without the caller doing anything.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task TestAutoPrepareAfterThreshold(string protocol)
    {
        CamusConnectionStringBuilder builder = protocol == "grpc" ? GrpcBuilderFor() : BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        // Seeded over SQL rather than the /insert route, which is REST-only.
        await using (CamusCommand seed = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)"))
        {
            seed.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            seed.Parameters.Add("@name", ColumnType.String, "auto");
            seed.Parameters.Add("@year", ColumnType.Integer64, 1984);

            Assert.Equal(1, await seed.ExecuteNonQueryAsync());
        }

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        for (int i = 0; i < 4; i++)
        {
            await using CamusCommand select = connection.CreateCamusCommand(sql);
            select.Parameters.Add("@year", ColumnType.Integer64, 1984);

            await using CamusDataReader reader = await select.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal("auto", reader.GetString(0));
        }

        // Registered without anyone asking, and registered once no matter how many commands ran it.
        Assert.True(builder.IsPrepared(sql));
    }

    /// <summary>A statement whose first execution is its only one is not worth a registration, and must
    /// not get one — that is the whole reason for the threshold.</summary>
    [Fact]
    public async Task TestSingleExecutionIsNotPrepared()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        await using CamusCommand select = connection.CreateCamusCommand(sql);
        select.Parameters.Add("@year", ColumnType.Integer64, 1);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());

        Assert.False(builder.IsPrepared(sql));
    }

    /// <summary><c>MaxAutoPrepare=0</c> turns the whole thing off, for a deployment that would rather not
    /// hold server-side state at all.</summary>
    [Fact]
    public async Task TestAutoPrepareCanBeDisabled()
    {
        CamusConnectionStringBuilder builder = BuilderFor(";MaxAutoPrepare=0");

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        for (int i = 0; i < 3; i++)
        {
            await using CamusCommand select = connection.CreateCamusCommand(sql);
            select.Parameters.Add("@year", ColumnType.Integer64, 1);

            await using CamusDataReader reader = await select.ExecuteReaderAsync();
            Assert.False(await reader.ReadAsync());
        }

        Assert.False(builder.IsPrepared(sql));
        Assert.Equal(0, builder.PreparedStatementCount);
    }

    /// <summary>
    /// DDL cannot be prepared — the server refuses to register it — and a client that tried anyway would
    /// break schema creation. <c>Prepare()</c> on one is a no-op, and the statement still runs.
    /// </summary>
    [Fact]
    public async Task TestDdlIsNeverPrepared()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = "robots_" + Guid.NewGuid().ToString("n");

        await using CamusCommand ddl = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL)");

        await ddl.PrepareAsync();

        Assert.False(builder.IsPrepared(ddl.CommandText));

        // And it still runs. Retried like the suite's other schema helpers: concurrent test classes
        // creating their own tables make the server report retryable lock contention.
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                Assert.True(await ddl.ExecuteDDLAsync());
                break;
            }
            catch (CamusException ex) when (attempt < 4 && (
                ex.Message.Contains("MustRetry", StringComparison.Ordinal) ||
                ex.Message.Contains("AlreadyLocked", StringComparison.Ordinal)))
            {
                await Task.Delay(100 * (attempt + 1));
            }
        }

        Assert.False(builder.IsPrepared(ddl.CommandText));
    }

    /// <summary>
    /// Values still bind by name in this driver's API, and the mapping onto the server's ordinals must
    /// survive the parameters being added in a different order than the statement declares them — an
    /// ordering the caller has no reason to know about.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task TestParametersBindByNameRegardlessOfOrder(string protocol)
    {
        CamusConnectionStringBuilder builder = protocol == "grpc" ? GrpcBuilderFor() : BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)");

        await insert.PrepareAsync();

        // Declared @id, @name, @year — added @year, @id, @name.
        insert.Parameters.Add("@year", ColumnType.Integer64, 1999);
        insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@name", ColumnType.String, "shuffled");

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name, year FROM {table}");
        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("shuffled", reader.GetString(0));
        Assert.Equal(1999, reader.GetInt64(1));
    }

    /// <summary>
    /// Placeholder names bind exactly, with the leading <c>@</c>, because that is what an inline
    /// execution requires: the engine looks a placeholder up by its literal text. Preparing a command
    /// must not quietly widen that, or a statement would work one way before it crossed the auto-prepare
    /// threshold and fail after it.
    /// </summary>
    [Fact]
    public async Task TestParameterNamesBindExactlyAsInline()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "bare", year: 2020);

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        // Inline: a name missing its '@' does not resolve.
        await using (CamusCommand inline = connection.CreateCamusCommand(sql))
        {
            inline.Parameters.Add("year", ColumnType.Integer64, 2020);
            await Assert.ThrowsAnyAsync<CamusException>(async () => await inline.ExecuteReaderAsync());
        }

        // Prepared: the same, rather than the looser spelling suddenly working.
        await using CamusCommand select = connection.CreateCamusCommand(sql);
        await select.PrepareAsync();

        select.Parameters.Add("year", ColumnType.Integer64, 2020);
        await Assert.ThrowsAnyAsync<CamusException>(async () => await select.ExecuteReaderAsync());

        select.Parameters.Clear();
        select.Parameters.Add("@year", ColumnType.Integer64, 2020);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("bare", reader.GetString(0));
    }

    /// <summary>
    /// A prepared execution inside an explicit transaction sees that transaction, not a short one of its
    /// own — the handle names the statement, never its execution context.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task TestPreparedExecutionInsideTransaction(string protocol)
    {
        CamusConnectionStringBuilder builder = protocol == "grpc" ? GrpcBuilderFor() : BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        await using (CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)"))
        {
            insert.Transaction = transaction;
            await insert.PrepareAsync();

            insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("@name", ColumnType.String, "txn");
            insert.Parameters.Add("@year", ColumnType.Integer64, 2030);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await transaction.CommitAsync();

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name FROM {table} WHERE year = @year");
        select.Parameters.Add("@year", ColumnType.Integer64, 2030);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("txn", reader.GetString(0));
    }

    /// <summary>
    /// A rolled-back transaction leaves nothing behind, prepared or not: the handle is a statement, not a
    /// commitment.
    /// </summary>
    [Fact]
    public async Task TestPreparedExecutionRollsBackWithItsTransaction()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        await using (CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)"))
        {
            insert.Transaction = transaction;
            await insert.PrepareAsync();

            insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("@name", ColumnType.String, "gone");
            insert.Parameters.Add("@year", ColumnType.Integer64, 2031);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await transaction.RollbackAsync();

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name FROM {table} WHERE year = @year");
        select.Parameters.Add("@year", ColumnType.Integer64, 2031);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.False(await reader.ReadAsync());
    }

    /// <summary>
    /// Streaming reads go through a different endpoint than buffered ones, so they get their own check
    /// that the handle path produces the same rows.
    /// </summary>
    [Fact]
    public async Task TestPreparedStreamReader()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        for (int i = 0; i < 3; i++)
            await InsertRobotAsync(connection, table, name: "stream" + i, year: 2100);

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name FROM {table} WHERE year = @year");
        await select.PrepareAsync();

        select.Parameters.Add("@year", ColumnType.Integer64, 2100);

        await using CamusDataReader reader = await select.ExecuteStreamReaderAsync();

        int rows = 0;
        while (await reader.ReadAsync())
        {
            Assert.StartsWith("stream", reader.GetString(0));
            rows++;
        }

        Assert.Equal(3, rows);
    }

    /// <summary>
    /// Registering the same statement twice — from two commands, as an ORM would — must cost one handle,
    /// not two, or a busy application would burn through the server's per-principal cap.
    /// </summary>
    [Fact]
    public async Task TestRepeatedPrepareRegistersOnce()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        for (int i = 0; i < 3; i++)
        {
            await using CamusCommand select = connection.CreateCamusCommand(sql);
            await select.PrepareAsync();
        }

        Assert.True(builder.IsPrepared(sql));
    }

    /// <summary>
    /// A placeholder the caller never bound is reported, and reported the same way whether or not the
    /// statement was prepared — a prepared execution cannot invent the missing value, and must not
    /// silently produce a different result than the inline one would.
    /// </summary>
    [Fact]
    public async Task TestMissingParameterIsReported()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        await InsertRobotAsync(connection, table, name: "present", year: 2000);

        string sql = $"SELECT name FROM {table} WHERE year = @year";

        await using (CamusCommand inline = connection.CreateCamusCommand(sql))
            await Assert.ThrowsAnyAsync<CamusException>(async () => await inline.ExecuteReaderAsync());

        await using CamusCommand select = connection.CreateCamusCommand(sql);
        await select.PrepareAsync();

        await Assert.ThrowsAnyAsync<CamusException>(async () => await select.ExecuteReaderAsync());
    }

    /// <summary>
    /// When the server will not register a statement, the statement still runs and still reports its own
    /// problem — preparing must never be the reason something fails, or turning it on would be a risk.
    /// A statement that does not parse is the reachable case: the server rejects it at registration, and
    /// the caller must see the parse error from executing it, not a registration error.
    /// </summary>
    [Fact]
    public async Task TestRefusedRegistrationFallsBackToInline()
    {
        CamusConnectionStringBuilder builder = BuilderFor();

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        // Refused at registration because it does not parse — the reachable stand-in for the other
        // reasons a server declines (no support for the feature at all, or a full server-side cap).
        string sql = $"SELECT name FROM {table} WHERE";

        await using CamusCommand select = connection.CreateCamusCommand(sql);

        // Registration fails, and says so by not throwing here.
        await select.PrepareAsync();
        Assert.False(builder.IsPrepared(sql));

        // Executing still reports the statement's own error rather than anything about preparing.
        CamusException error = await Assert.ThrowsAnyAsync<CamusException>(async () => await select.ExecuteReaderAsync());
        Assert.DoesNotContain("prepare", error.Message, StringComparison.OrdinalIgnoreCase);

        // And a statement that does work is unaffected by the refusal of a different one.
        await using CamusCommand ok = connection.CreateCamusCommand($"SELECT name FROM {table}");
        await using CamusDataReader reader = await ok.ExecuteReaderAsync();
        Assert.False(await reader.ReadAsync());
    }

    /// <summary>
    /// Statements are tracked with an LRU cap so unbounded distinct SQL cannot grow the client's memory
    /// or the server's registration count without limit.
    /// </summary>
    [Fact]
    public async Task TestPreparedStatementsAreCapped()
    {
        CamusConnectionStringBuilder builder = BuilderFor(";MaxAutoPrepare=2;AutoPrepareMinUsages=1");

        await using CamusConnection connection = await OpenAsync(builder);
        string table = await CreateTempRobotsTableAsync(connection);

        for (int i = 0; i < 6; i++)
        {
            await using CamusCommand select = connection.CreateCamusCommand(
                $"SELECT name FROM {table} WHERE year = @year AND price > {i}");
            select.Parameters.Add("@year", ColumnType.Integer64, i);

            await using CamusDataReader reader = await select.ExecuteReaderAsync();
            Assert.False(await reader.ReadAsync());
        }

        Assert.True(builder.PreparedStatementCount <= 2, $"expected at most 2 tracked statements, got {builder.PreparedStatementCount}");
    }
}
