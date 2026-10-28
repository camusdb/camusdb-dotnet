/**
 * This file is part of CamusDB
 *
 * Live coverage for INSERT … ON CONFLICT and UPSERT INTO on both transports, through every ADO.NET
 * entry point: the non-query, the reader, the scalar, the stream reader and a prepared statement.
 * Requires a server with ON CONFLICT support, so the tests are opt-in: set CAMUSDB_TEST_ON_CONFLICT=true.
 * The REST endpoint comes from CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint
 * from CAMUSDB_TEST_GRPC_ENDPOINT (default localhost:5096).
 */

namespace CamusDB.Client.Tests;

public class TestOnConflictLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_ON_CONFLICT"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync("test", ifNotExists: true);
        return connection;
    }

    private static async Task<string> CreateTableAsync(CamusConnection connection)
    {
        string table = "upc_" + Guid.NewGuid().ToString("n")[..12];
        await using CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (k STRING NOT NULL, n INT64 NOT NULL, PRIMARY KEY (k))");
        await cmd.ExecuteDDLAsync();
        return table;
    }

    private static async Task<long?> StoredAsync(CamusConnection connection, string table, string key)
    {
        await using CamusCommand select = connection.CreateCamusCommand($"SELECT n FROM {table} WHERE k = @k");
        select.Parameters.Add("@k", ColumnType.String, key);
        return (long?)await select.ExecuteScalarAsync();
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task NonQueryCountsInsertedAndUpdatedRows(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using (CamusCommand insert = connection.CreateCamusCommand($"INSERT INTO {table} (k, n) VALUES ('a', 1)"))
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());

        // 'a' is updated and 'b' is inserted: the count is both.
        await using (CamusCommand upsert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (k, n) VALUES ('a', 5), ('b', 2) ON CONFLICT (k) DO UPDATE SET n = {table}.n + excluded.n"))
            Assert.Equal(2, await upsert.ExecuteNonQueryAsync());

        Assert.Equal(6L, await StoredAsync(connection, table, "a"));
        Assert.Equal(2L, await StoredAsync(connection, table, "b"));

        await using (CamusCommand skip = connection.CreateCamusCommand($"INSERT INTO {table} (k, n) VALUES ('a', 9) ON CONFLICT DO NOTHING"))
            Assert.Equal(0, await skip.ExecuteNonQueryAsync());

        Assert.Equal(6L, await StoredAsync(connection, table, "a"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ReaderRunsAnUpsertWithoutReturning(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        // Before 0.16.7 the reader sent UPSERT to the query endpoint, which refused it with CADB0407.
        await using (CamusCommand upsert = connection.CreateCamusCommand($"UPSERT INTO {table} (k, n) VALUES ('a', 1)"))
        await using (CamusDataReader reader = await upsert.ExecuteReaderAsync())
        {
            Assert.Equal(1, reader.RecordsAffected);
            Assert.False(await reader.ReadAsync());
        }

        await using (CamusCommand upsert = connection.CreateCamusCommand($"UPSERT INTO {table} (k, n) VALUES ('a', 3)"))
        await using (CamusDataReader reader = await upsert.ExecuteReaderAsync())
            Assert.Equal(1, reader.RecordsAffected);

        Assert.Equal(3L, await StoredAsync(connection, table, "a"));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ReaderReturnsTheUpsertedRowsAndTheirCount(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using (CamusCommand insert = connection.CreateCamusCommand($"INSERT INTO {table} (k, n) VALUES ('a', 1)"))
            await insert.ExecuteNonQueryAsync();

        await using CamusCommand upsert = connection.CreateCamusCommand(
            $"UPSERT INTO {table} (k, n) VALUES ('a', 7), ('b', 8) RETURNING k, n");
        await using CamusDataReader reader = await upsert.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);

        List<(string K, long N)> rows = [];
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetInt64(1)));

        Assert.Equal([("a", 7L), ("b", 8L)], rows);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ScalarAndStreamReadTheNewValue(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        string sql = $"INSERT INTO {table} (k, n) VALUES ('hits', 1) ON CONFLICT (k) DO UPDATE SET n = {table}.n + excluded.n RETURNING n";

        await using (CamusCommand first = connection.CreateCamusCommand(sql))
            Assert.Equal(1L, await first.ExecuteScalarAsync());

        await using (CamusCommand second = connection.CreateCamusCommand(sql))
            Assert.Equal(2L, await second.ExecuteScalarAsync());

        await using CamusCommand third = connection.CreateCamusCommand($"UPSERT INTO {table} (k, n) VALUES ('hits', 10) RETURNING n");
        await using CamusDataReader stream = await third.ExecuteStreamReaderAsync();

        Assert.True(await stream.ReadAsync());
        Assert.Equal(10L, stream.GetInt64(0));
        Assert.False(await stream.ReadAsync());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task PreparedUpsertRunsWithNewValues(string protocol)
    {
        if (!Enabled)
            return;

        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTableAsync(connection);

        await using CamusCommand upsert = connection.CreateCamusCommand($"UPSERT INTO {table} (k, n) VALUES (@k, @n)");
        CamusParameter key = upsert.Parameters.Add("@k", ColumnType.String, "a");
        CamusParameter value = upsert.Parameters.Add("@n", ColumnType.Integer64, 1L);
        await upsert.PrepareAsync();

        Assert.Equal(1, await upsert.ExecuteNonQueryAsync());

        value.Value = 4L;
        Assert.Equal(1, await upsert.ExecuteNonQueryAsync());

        key.Value = "b";
        Assert.Equal(1, await upsert.ExecuteNonQueryAsync());

        Assert.Equal(4L, await StoredAsync(connection, table, "a"));
        Assert.Equal(4L, await StoredAsync(connection, table, "b"));
    }
}
