/**
 * This file is part of CamusDB
 *
 * End-to-end coverage for BatchExecute stream frames against a real server: the server announces frames
 * on the stream's response headers, concurrent ops really travel as request frames, their answers really
 * come back in response frames, and every caller still gets its own result — inside and outside
 * transactions. Opt-in: the whole class no-ops unless CAMUSDB_TEST_BATCH_FRAMES=true, because a server
 * built before frames announces nothing, and then no frame may be seen at all.
 *
 * To run it, start a server that reads frames, then:
 *   export CAMUSDB_TEST_BATCH_FRAMES=true
 *   export CAMUSDB_TEST_ENDPOINT=http://localhost:5095        # optional, this is the default
 *   export CAMUSDB_TEST_GRPC_ENDPOINT=http://localhost:5096   # optional, this is the default
 *   dotnet test --filter FullyQualifiedName~TestGrpcBatchFramesLive
 */

using CamusDB.Client.Transport.Batching;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Grpc;
using Grpc.Net.Client;

namespace CamusDB.Client.Tests;

public sealed class TestGrpcBatchFramesLive : BaseTest
{
    private static bool Configured
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_BATCH_FRAMES"), "true", StringComparison.OrdinalIgnoreCase);

    private static string GrpcEndpoint
        => Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096";

    [Fact]
    public async Task TheServerAnnouncesFramesWhenTheStreamOpens()
    {
        if (!Configured)
            return;

        using GrpcChannel channel = GrpcChannel.ForAddress(GrpcEndpoint);
        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(channel));

        // Before any op is written: the announcement must not depend on a first answer.
        Assert.True(await EventuallyAsync(() => transport.FramesAnnounced));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConcurrentCallersGetTheirOwnResults(bool requestFrames)
    {
        if (!Configured)
            return;

        CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        using GrpcChannel channel = GrpcChannel.ForAddress(GrpcEndpoint);
        CamusSql.CamusSqlClient client = new(channel);
        Counters counters = new();

        await using GrpcBatcher batcher = new(
            new GrpcBatchOptions { ChannelPoolSize = 1, RequestFrames = requestFrames },
            id => new CountingTransport(new GrpcBatchTransport(id, client, acceptResponseFrames: requestFrames), counters));

        // Announced before the burst, so the burst is what decides whether frames are written.
        await batcher.EnqueueQueryAsync(Select(table, "none"), slotIndex: 0, default);

        const int Callers = 64;
        const int Rounds = 8;

        await Task.WhenAll(Enumerable.Range(0, Callers).Select(caller => Task.Run(async () =>
        {
            for (int round = 0; round < Rounds; round++)
            {
                string name = $"c{caller}r{round}";

                BatchNonQueryResult inserted = await batcher.EnqueueNonQueryAsync(Insert(table, name, caller), slotIndex: 0, default);
                Assert.Equal(1, inserted.AffectedRows);

                BatchQueryResult read = await batcher.EnqueueQueryAsync(Select(table, name), slotIndex: 0, default);
                Assert.Single(read.Rows);
                Assert.Equal(name, read.Rows[0].Values[0].StringValue);
                Assert.Equal(caller, read.Rows[0].Values[1].Int64Value);
            }
        })));

        if (requestFrames)
        {
            Assert.True(counters.RequestFrames > 0, "no request frame was written under 64 concurrent callers");
            Assert.True(counters.ResponseFrames > 0, "the server never answered with a response frame");
            Assert.True(counters.Messages < counters.Ops, "frames did not reduce the number of stream messages");
        }
        else
        {
            Assert.Equal(0, counters.RequestFrames);
            Assert.Equal(0, counters.ResponseFrames);   // the server frames only for a peer that sent one
            Assert.Equal(counters.Ops, counters.Messages);
        }
    }

    /// <summary>
    /// A single caller never sends a request frame, so the accept header is the server's only proof that
    /// it may pack this stream's answers. With it, a small result is one message and a 100-row result a
    /// handful; without it, every row is its own message, as before frames.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ASingleCallerReadsAMultiRowResultInFewMessages(bool acceptResponseFrames)
    {
        if (!Configured)
            return;

        CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        using GrpcChannel channel = GrpcChannel.ForAddress(GrpcEndpoint);
        CamusSql.CamusSqlClient client = new(channel);
        Counters counters = new();

        await using GrpcBatcher batcher = new(
            new GrpcBatchOptions { ChannelPoolSize = 1 },
            id => new CountingTransport(new GrpcBatchTransport(id, client, acceptResponseFrames: acceptResponseFrames), counters));

        const int Rows = 100;

        // One at a time: nothing waits together, so no request frame ever forms on this stream.
        for (int i = 0; i < Rows; i++)
            Assert.Equal(1, (await batcher.EnqueueNonQueryAsync(Insert(table, $"r{i}", 7), slotIndex: 0, default)).AffectedRows);

        Assert.Equal(0, counters.RequestFrames);

        int before = Volatile.Read(ref counters.ResponseMessages);
        BatchQueryResult one = await batcher.EnqueueQueryAsync(Select(table, "r0"), slotIndex: 0, default);
        Assert.Single(one.Rows);
        int oneRowMessages = Volatile.Read(ref counters.ResponseMessages) - before;

        before = Volatile.Read(ref counters.ResponseMessages);
        SqlRequest all = new() { Database = "test", Sql = $"SELECT name, year FROM {table} WHERE year = @year" };
        all.Parameters.Add("@year", new Value { Int64Value = 7 });
        BatchQueryResult many = await batcher.EnqueueQueryAsync(all, slotIndex: 0, default);
        Assert.Equal(Rows, many.Rows.Count);
        int manyRowsMessages = Volatile.Read(ref counters.ResponseMessages) - before;

        if (acceptResponseFrames)
        {
            Assert.Equal(1, oneRowMessages);                 // schema + row + terminator, packed
            Assert.InRange(manyRowsMessages, 1, 8);          // groups grow from 2 rows up to 64
            Assert.True(counters.ResponseFrames > 0);
        }
        else
        {
            Assert.Equal(3, oneRowMessages);                 // schema, row, terminator
            Assert.Equal(Rows + 2, manyRowsMessages);        // schema, one message per row, terminator
            Assert.Equal(0, counters.ResponseFrames);
        }
    }

    [Fact]
    public async Task ConcurrentTransactionsCommitAndRollBackThroughFrames()
    {
        if (!Configured)
            return;

        CamusConnection connection = await GetConnection();
        string table = await CreateTempRobotsTableAsync(connection);

        using GrpcChannel channel = GrpcChannel.ForAddress(GrpcEndpoint);
        CamusSql.CamusSqlClient client = new(channel);
        Counters counters = new();

        await using GrpcBatcher batcher = new(
            new GrpcBatchOptions { ChannelPoolSize = 2 },
            id => new CountingTransport(new GrpcBatchTransport(id, client), counters));

        const int Transactions = 32;

        await Task.WhenAll(Enumerable.Range(0, Transactions).Select(n => Task.Run(async () =>
        {
            int slot = batcher.ReserveSlot();
            TxnHandle handle = await batcher.EnqueueStartAsync(new SqlRequest { Database = "test" }, slot, default);

            // Pipelined: both inserts are written before either is answered, so they can share a frame,
            // and the server must still run them in order on the transaction's chain.
            Task<BatchNonQueryResult> first = batcher.EnqueueNonQueryAsync(Insert(table, $"t{n}a", n, handle), slot, default);
            Task<BatchNonQueryResult> second = batcher.EnqueueNonQueryAsync(Insert(table, $"t{n}b", n, handle), slot, default);
            await Task.WhenAll(first, second);

            if (n % 2 == 0)
                await batcher.EnqueueCommitAsync(new SqlRequest { Database = "test", TxnHandle = handle }, slot, default);
            else
                await batcher.EnqueueRollbackAsync(new SqlRequest { Database = "test", TxnHandle = handle }, slot, default);
        })));

        for (int n = 0; n < Transactions; n++)
        {
            int expected = n % 2 == 0 ? 1 : 0;

            Assert.Equal(expected, (await batcher.EnqueueQueryAsync(Select(table, $"t{n}a"), slotIndex: null, default)).Rows.Count);
            Assert.Equal(expected, (await batcher.EnqueueQueryAsync(Select(table, $"t{n}b"), slotIndex: null, default)).Rows.Count);
        }

        Assert.True(counters.RequestFrames > 0, "no request frame was written under 32 concurrent transactions");
    }

    private static SqlRequest Insert(string table, string name, long year, TxnHandle? handle = null)
    {
        SqlRequest request = new()
        {
            Database = "test",
            Sql = $"INSERT INTO {table} (id, name, year) VALUES (@id, @name, @year)",
            TxnHandle = handle,
        };

        request.Parameters.Add("@id", new Value { IdValue = CamusObjectIdGenerator.GenerateAsString() });
        request.Parameters.Add("@name", new Value { StringValue = name });
        request.Parameters.Add("@year", new Value { Int64Value = year });
        return request;
    }

    private static SqlRequest Select(string table, string name)
    {
        SqlRequest request = new() { Database = "test", Sql = $"SELECT name, year FROM {table} WHERE name = @name" };
        request.Parameters.Add("@name", new Value { StringValue = name });
        return request;
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        for (int i = 0; i < 500 && !condition(); i++)
            await Task.Delay(10);

        return condition();
    }

    private sealed class Counters
    {
        public int Messages;
        public int Ops;
        public int RequestFrames;
        public int ResponseFrames;
        public int ResponseMessages;
    }

    /// <summary>The real transport, counted: what was written as frames, and what came back as frames.</summary>
    private sealed class CountingTransport(GrpcBatchTransport inner, Counters counters) : IBatchTransport
    {
        public long Id => inner.Id;

        public bool FramesAnnounced => inner.FramesAnnounced;

        public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            bool frame = request.Kind == BatchStatementKind.Frame;

            Interlocked.Increment(ref counters.Messages);
            Interlocked.Add(ref counters.Ops, frame ? request.Items.Count : 1);

            if (frame)
                Interlocked.Increment(ref counters.RequestFrames);

            return inner.SendAsync(request, cancellationToken);
        }

        public async IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (BatchExecuteResponse response in inner.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                Interlocked.Increment(ref counters.ResponseMessages);

                if (response.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Frame)
                    Interlocked.Increment(ref counters.ResponseFrames);

                yield return response;
            }
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
