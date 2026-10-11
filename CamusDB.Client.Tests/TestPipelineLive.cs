/**
 * This file is part of CamusDB
 *
 * End-to-end coverage for pipelined transactions against a real server: a bank transfer whose BEGIN
 * rides the first frame and whose commit rides the second costs two exchanges over gRPC, every
 * statement gets its own result, a failed statement fails the conditional commit with that failure and
 * leaves nothing stored, and REST runs the same code one exchange per step. Opt-in, like the other
 * live frame tests: the whole class no-ops unless CAMUSDB_TEST_BATCH_FRAMES=true, because a server
 * built before the pipeline contract announces a lower version and the exchange count would differ.
 *
 * To run it, start a server that reads the pipeline contract, then:
 *   export CAMUSDB_TEST_BATCH_FRAMES=true
 *   export CAMUSDB_TEST_ENDPOINT=http://localhost:5095        # optional, this is the default
 *   export CAMUSDB_TEST_GRPC_ENDPOINT=http://localhost:5096   # optional, this is the default
 *   dotnet test --filter FullyQualifiedName~TestPipelineLive
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public sealed class TestPipelineLive : BaseTest
{
    private static bool Configured
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_BATCH_FRAMES"), "true", StringComparison.OrdinalIgnoreCase);

    private static string GrpcEndpoint
        => Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096";

    private static string RestEndpoint
        => Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095";

    private static async Task<CamusConnection> OpenAsync(string endpoint, string protocol)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder($"Endpoint={endpoint};Database=test;Protocol={protocol}"));
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<(string Table, string A, string B)> SeedAccountsAsync(CamusConnection connection)
    {
        string table = await CreateTempTableAsync(connection, "accounts", "id OID PRIMARY KEY NOT NULL, balance INT64 NOT NULL");
        string a = CamusObjectIdGenerator.GenerateAsString();
        string b = CamusObjectIdGenerator.GenerateAsString();

        foreach ((string id, long balance) in new[] { (a, 100L), (b, 50L) })
        {
            await using CamusCommand insert = connection.CreateCamusCommand($"INSERT INTO {table} (id, balance) VALUES (@id, @b)");
            insert.Parameters.Add("@id", ColumnType.Id, id);
            insert.Parameters.Add("@b", ColumnType.Integer64, balance);
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        return (table, a, b);
    }

    private static async Task<long> BalanceAsync(CamusConnection connection, string table, string id)
    {
        await using CamusCommand read = connection.CreateSelectCommand($"SELECT balance FROM {table} WHERE id = @id");
        read.Parameters.Add("@id", ColumnType.Id, id);
        using CamusDataReader reader = await read.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return reader.GetInt64(0);
    }

    private static CamusCommand Read(CamusConnection connection, string table, string id)
    {
        CamusCommand read = connection.CreateSelectCommand($"SELECT balance FROM {table} WHERE id = @id");
        read.Parameters.Add("@id", ColumnType.Id, id);
        return read;
    }

    private static CamusCommand Write(CamusConnection connection, string table, string id, long balance)
    {
        CamusCommand update = connection.CreateCamusCommand($"UPDATE {table} SET balance = @b WHERE id = @id");
        update.Parameters.Add("@b", ColumnType.Integer64, balance);
        update.Parameters.Add("@id", ColumnType.Id, id);
        return update;
    }

    [Theory]
    [InlineData("grpc", 2)]
    [InlineData("rest", 6)]
    public async Task ATransferIsTwoExchangesOverGrpcAndSixOverRest(string protocol, int expectedExchanges)
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await OpenAsync(protocol == "grpc" ? GrpcEndpoint : RestEndpoint, protocol);
        (string table, string a, string b) = await SeedAccountsAsync(connection);

        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions
        {
            Locking = CamusLocking.Optimistic,
            DeferBegin = true,
        });
        Assert.False(tx.IsStarted);
        CamusPipeline pipeline = tx.CreatePipeline();

        Task<CamusDataReader> readA = pipeline.QueueReader(Read(connection, table, a));
        Task<CamusDataReader> readB = pipeline.QueueReader(Read(connection, table, b));
        await pipeline.SendAsync();
        Assert.True(tx.IsStarted);

        long balanceA, balanceB;
        using (CamusDataReader reader = await readA)
        {
            Assert.True(await reader.ReadAsync());
            balanceA = reader.GetInt64(0);
        }
        using (CamusDataReader reader = await readB)
        {
            Assert.True(await reader.ReadAsync());
            balanceB = reader.GetInt64(0);
        }
        Assert.Equal(100L, balanceA);
        Assert.Equal(50L, balanceB);

        Task<int> debit = pipeline.QueueNonQuery(Write(connection, table, a, balanceA - 30));
        Task<int> credit = pipeline.QueueNonQuery(Write(connection, table, b, balanceB + 30));
        await pipeline.CommitAsync();

        Assert.Equal(1, await debit);
        Assert.Equal(1, await credit);
        Assert.Equal(expectedExchanges, pipeline.Exchanges);

        Assert.Equal(70L, await BalanceAsync(connection, table, a));
        Assert.Equal(80L, await BalanceAsync(connection, table, b));

        // Committed by the pipeline: a disposing scope's rollback is quiet.
        await tx.RollbackAsync();
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("rest")]
    public async Task AFailedStatementFailsTheCommitWithItsOwnFailureAndStoresNothing(string protocol)
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await OpenAsync(protocol == "grpc" ? GrpcEndpoint : RestEndpoint, protocol);
        (string table, string a, string b) = await SeedAccountsAsync(connection);

        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = tx.CreatePipeline();

        Task<int> ok = pipeline.QueueNonQuery(Write(connection, table, a, 0));
        Task<CamusDataReader> bad = pipeline.QueueReader(connection.CreateSelectCommand("SELECT nope FROM no_such_table_anywhere"));
        Task<int> after = pipeline.QueueNonQuery(Write(connection, table, b, 0));

        CamusException thrown = await Assert.ThrowsAsync<CamusException>(() => pipeline.CommitAsync());
        CamusException read = await Assert.ThrowsAsync<CamusException>(() => bad);
        Assert.Equal(read.Code, thrown.Code);
        Assert.Equal(1, await ok);
        await Assert.ThrowsAsync<CamusException>(() => after);

        await tx.RollbackAsync();

        Assert.Equal(100L, await BalanceAsync(connection, table, a));
        Assert.Equal(50L, await BalanceAsync(connection, table, b));
    }

    [Fact]
    public async Task AOneExchangeTransferWithServerSideArithmetic()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await OpenAsync(GrpcEndpoint, "grpc");
        (string table, string a, string b) = await SeedAccountsAsync(connection);

        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = tx.CreatePipeline();

        CamusCommand debit = connection.CreateCamusCommand($"UPDATE {table} SET balance = balance - @d WHERE id = @id");
        debit.Parameters.Add("@d", ColumnType.Integer64, 10);
        debit.Parameters.Add("@id", ColumnType.Id, a);
        CamusCommand credit = connection.CreateCamusCommand($"UPDATE {table} SET balance = balance + @d WHERE id = @id");
        credit.Parameters.Add("@d", ColumnType.Integer64, 10);
        credit.Parameters.Add("@id", ColumnType.Id, b);

        Task<int> debited = pipeline.QueueNonQuery(debit);
        Task<int> credited = pipeline.QueueNonQuery(credit);
        await pipeline.CommitAsync();

        Assert.Equal(1, await debited);
        Assert.Equal(1, await credited);
        Assert.Equal(1, pipeline.Exchanges);
        Assert.Equal(90L, await BalanceAsync(connection, table, a));
        Assert.Equal(60L, await BalanceAsync(connection, table, b));
    }

    [Fact]
    public async Task ConcurrentPipelinedTransfersConserveTheTotal()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await OpenAsync(GrpcEndpoint, "grpc");
        (string table, string a, string b) = await SeedAccountsAsync(connection);

        const int Workers = 16;
        const int Rounds = 10;
        int committed = 0;

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(worker => Task.Run(async () =>
        {
            for (int round = 0; round < Rounds; round++)
            {
                (string from, string to) = (worker + round) % 2 == 0 ? (a, b) : (b, a);

                for (int attempt = 0; ; attempt++)
                {
                    CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions
                    {
                        Locking = CamusLocking.Optimistic,
                        DeferBegin = true,
                    });
                    CamusPipeline pipeline = tx.CreatePipeline();

                    try
                    {
                        Task<CamusDataReader> readFrom = pipeline.QueueReader(Read(connection, table, from));
                        Task<CamusDataReader> readTo = pipeline.QueueReader(Read(connection, table, to));
                        await pipeline.SendAsync();

                        long fromBalance, toBalance;
                        using (CamusDataReader reader = await readFrom) { Assert.True(await reader.ReadAsync()); fromBalance = reader.GetInt64(0); }
                        using (CamusDataReader reader = await readTo) { Assert.True(await reader.ReadAsync()); toBalance = reader.GetInt64(0); }

                        _ = pipeline.QueueNonQuery(Write(connection, table, from, fromBalance - 1));
                        _ = pipeline.QueueNonQuery(Write(connection, table, to, toBalance + 1));
                        await pipeline.CommitAsync();

                        Assert.Equal(2, pipeline.Exchanges);
                        Interlocked.Increment(ref committed);
                        break;
                    }
                    catch (CamusException ex) when (SerializableRetryHelper.IsRetryable(ex) && attempt < 50)
                    {
                        try { await tx.RollbackAsync(); } catch { /* already ended */ }
                        await Task.Delay(5 * (attempt + 1));
                    }
                }
            }
        })));

        Assert.Equal(Workers * Rounds, committed);
        Assert.Equal(150L, await BalanceAsync(connection, table, a) + await BalanceAsync(connection, table, b));
    }
}
