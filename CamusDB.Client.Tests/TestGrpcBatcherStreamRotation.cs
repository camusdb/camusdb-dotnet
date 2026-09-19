/**
 * This file is part of CamusDB
 *
 * Offline coverage for stream rotation in the gRPC batcher. A BatchExecute stream presents its bearer
 * token once, when it opens, and is meant to outlive it. A server that re-checks that opening token per
 * operation ends the stream — and every transaction on it — at the token's expiry, so the batcher moves
 * each slot onto a fresh stream when the token is renewed, and lets the old stream finish only the
 * transactions that began on it. Driven by a fake IBatchTransport, so no server is needed.
 */

using System.Collections.Concurrent;
using System.Threading.Channels;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcBatcherStreamRotation
{
    /// <summary>The credential the batcher is told a new stream would carry; a test renews it by assignment.</summary>
    private sealed class Credential
    {
        public volatile string? Token = "token-1";
    }

    private sealed class Harness : IAsyncDisposable
    {
        public readonly Credential Credential = new();
        public readonly ConcurrentQueue<RecordingTransport> Transports = new();
        public readonly GrpcBatcher Batcher;

        public Harness(int drainTimeoutMs = 300_000)
        {
            Batcher = new GrpcBatcher(
                new GrpcBatchOptions { ChannelPoolSize = 1, StreamDrainTimeoutMs = drainTimeoutMs },
                id =>
                {
                    RecordingTransport transport = new(id);
                    Transports.Enqueue(transport);
                    return transport;
                },
                () => Credential.Token);
        }

        public RecordingTransport Transport(int ordinal) => Transports.ElementAt(ordinal);

        public ValueTask DisposeAsync() => Batcher.DisposeAsync();
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        for (int i = 0; i < 200 && !condition(); i++)
            await Task.Delay(10);

        return condition();
    }

    [Fact]
    public async Task ARenewedCredentialMovesTheSlotOntoAFreshStream()
    {
        await using Harness h = new();

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "before" }, slotIndex: 0, default);
        Assert.Single(h.Transports);

        h.Credential.Token = "token-2";

        // Rotation is lazy, like reconnecting: renewing the token opens nothing by itself.
        await Task.Delay(100);
        Assert.Single(h.Transports);

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "after" }, slotIndex: 0, default);

        Assert.Equal(2, h.Transports.Count);
        Assert.Equal(["before"], h.Transport(0).Sent);
        Assert.Equal(["after"], h.Transport(1).Sent);

        // Nothing was riding on the old stream, so it is closed at once rather than left to expire.
        Assert.True(await EventuallyAsync(() => h.Transport(0).Disposed), "the retired stream was left open");
        Assert.False(h.Transport(1).Disposed);
    }

    [Fact]
    public async Task AnUnchangedCredentialNeverRotates()
    {
        await using Harness h = new();

        for (int i = 0; i < 50; i++)
            await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "u" }, slotIndex: 0, default);

        Assert.Single(h.Transports);
    }

    [Fact]
    public async Task AMissingCredentialDoesNotTradeAWorkingStreamForAnUnauthenticatedOne()
    {
        await using Harness h = new();

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "u" }, slotIndex: 0, default);

        // What the provider reports between discarding a rejected token and minting its replacement.
        h.Credential.Token = null;

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "u" }, slotIndex: 0, default);

        Assert.Single(h.Transports);
    }

    [Fact]
    public async Task AnOpenTransactionFinishesOnTheStreamItBeganOn()
    {
        await using Harness h = new();

        TxnHandle handle = await h.Batcher.EnqueueStartAsync(new SqlRequest(), 0, default);

        h.Credential.Token = "token-2";

        // New work rotates the slot...
        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "autocommit" }, slotIndex: 0, default);
        Assert.Equal(2, h.Transports.Count);

        // ...but the transaction cannot follow it: the server holds it on the stream that began it, and
        // rolls it back when that stream closes. So the old stream must still be open, and must get
        // every remaining op of the transaction.
        Assert.False(h.Transport(0).Disposed, "a stream was closed under an open transaction");

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "in-txn", TxnHandle = handle }, slotIndex: 0, default);
        await h.Batcher.EnqueueCommitAsync(new SqlRequest { TxnHandle = handle }, 0, default);

        Assert.Equal(["<Start>", "in-txn", "<Commit>"], h.Transport(0).Sent);
        Assert.Equal(["autocommit"], h.Transport(1).Sent);

        // The commit was the last thing riding on it.
        Assert.True(await EventuallyAsync(() => h.Transport(0).Disposed), "the drained stream was left open");
        Assert.False(h.Transport(1).Disposed);
    }

    /// <summary>
    /// A slot's prepared-statement registration lives on its current stream. A transaction finishing on
    /// a retired one must be told so, or it would execute a handle on a stream that never registered it.
    /// </summary>
    [Fact]
    public async Task ATransactionKnowsWhenItIsFinishingOnARetiredStream()
    {
        await using Harness h = new();

        TxnHandle handle = await h.Batcher.EnqueueStartAsync(new SqlRequest(), 0, default);
        Assert.False(h.Batcher.IsBoundToRetiredStream(handle.TxnIdPt, handle.TxnIdCounter));

        h.Credential.Token = "token-2";
        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "autocommit" }, slotIndex: 0, default);

        Assert.True(h.Batcher.IsBoundToRetiredStream(handle.TxnIdPt, handle.TxnIdCounter));

        await h.Batcher.EnqueueCommitAsync(new SqlRequest { TxnHandle = handle }, 0, default);

        Assert.False(h.Batcher.IsBoundToRetiredStream(handle.TxnIdPt, handle.TxnIdCounter));
    }

    [Fact]
    public async Task ATransactionBegunAfterTheRenewalStartsOnTheFreshStream()
    {
        await using Harness h = new();

        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "u" }, slotIndex: 0, default);
        h.Credential.Token = "token-2";

        TxnHandle handle = await h.Batcher.EnqueueStartAsync(new SqlRequest(), 0, default);
        await h.Batcher.EnqueueCommitAsync(new SqlRequest { TxnHandle = handle }, 0, default);

        Assert.Equal(["<Start>", "<Commit>"], h.Transport(1).Sent);
    }

    [Fact]
    public async Task AServerRefusalOfTheCommitStillEndsTheTransaction()
    {
        await using Harness h = new();

        TxnHandle handle = await h.Batcher.EnqueueStartAsync(new SqlRequest(), 0, default);
        h.Credential.Token = "token-2";
        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "autocommit" }, slotIndex: 0, default);

        await Assert.ThrowsAsync<CamusException>(
            () => h.Batcher.EnqueueCommitAsync(new SqlRequest { Sql = "boom", TxnHandle = handle }, 0, default));

        Assert.True(await EventuallyAsync(() => h.Transport(0).Disposed),
            "a refused commit left its stream waiting for a transaction the server already ended");
    }

    [Fact]
    public async Task AnAbandonedTransactionDoesNotHoldARetiredStreamOpenForGood()
    {
        await using Harness h = new(drainTimeoutMs: 150);

        await h.Batcher.EnqueueStartAsync(new SqlRequest(), 0, default);   // never finalized
        h.Credential.Token = "token-2";
        await h.Batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "autocommit" }, slotIndex: 0, default);

        Assert.False(h.Transport(0).Disposed);

        // Closing the stream is what makes the server roll the abandoned transaction back.
        Assert.True(await EventuallyAsync(() => h.Transport(0).Disposed), "the drain timeout never closed the stream");
        Assert.False(h.Transport(1).Disposed);
    }

    [Fact]
    public async Task ConcurrentWorkAcrossARenewalLosesNothing()
    {
        await using Harness h = new();

        Task<BatchQueryResult>[] queries = new Task<BatchQueryResult>[400];
        for (int i = 0; i < queries.Length; i++)
        {
            if (i % 100 == 50)
                h.Credential.Token = $"token-{i}";

            queries[i] = h.Batcher.EnqueueQueryAsync(new SqlRequest { Sql = $"q{i}" }, slotIndex: 0, default);
        }

        BatchQueryResult[] results = await Task.WhenAll(queries);

        for (int i = 0; i < results.Length; i++)
            Assert.Equal($"q{i}", results[i].Rows[0].Values[0].StringValue);

        Assert.InRange(h.Transports.Count, 2, 5);
    }

    /// <summary>
    /// An in-process stream that records what was written to it and whether it was closed, and answers
    /// each request by kind. A request whose SQL is <c>boom</c> is answered with an in-band error.
    /// </summary>
    private sealed class RecordingTransport(long id) : IBatchTransport
    {
        private readonly Channel<BatchExecuteResponse> channel = Channel.CreateUnbounded<BatchExecuteResponse>();
        private readonly ConcurrentQueue<string> sent = new();
        private int disposed;

        public long Id { get; } = id;

        public string[] Sent => [.. sent];

        public bool Disposed => Volatile.Read(ref disposed) != 0;

        public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            if (Disposed)
                return Task.FromException(new IOException("written to a closed stream"));

            string sql = request.Request?.Sql ?? "";
            sent.Enqueue(sql.Length > 0 ? sql : $"<{request.Kind}>");

            channel.Writer.TryWrite(Respond(request));
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken)
            => ReadAsync(cancellationToken);

        private async IAsyncEnumerable<BatchExecuteResponse> ReadAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (BatchExecuteResponse response in channel.Reader.ReadAllAsync(cancellationToken))
            {
                // A QUERY is answered with its row and then its terminator, as the server does.
                if (response.Row is not null)
                {
                    yield return response;
                    yield return new BatchExecuteResponse { RequestId = response.RequestId, QueryComplete = new QueryComplete { Total = 1 } };
                    continue;
                }

                yield return response;
            }
        }

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref disposed, 1);
            channel.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }

        private static BatchExecuteResponse Respond(BatchExecuteRequest request)
        {
            int id = request.RequestId;

            if (request.Request?.Sql == "boom")
                return new BatchExecuteResponse { RequestId = id, Error = new BatchError { Code = "CADB0502", Message = "conflict" } };

            switch (request.Kind)
            {
                case BatchStatementKind.Query:
                    ResultRow row = new();
                    row.Values.Add(new Value { StringValue = request.Request?.Sql ?? "" });
                    return new BatchExecuteResponse { RequestId = id, Row = row };

                case BatchStatementKind.Start:
                    return new BatchExecuteResponse { RequestId = id, StartReply = new TxnHandle { TxnIdPt = 111, TxnIdCounter = 222 } };

                case BatchStatementKind.Commit:
                    return new BatchExecuteResponse { RequestId = id, CommitReply = new CommitReply() };

                case BatchStatementKind.Rollback:
                    return new BatchExecuteResponse { RequestId = id, RollbackReply = new RollbackReply() };

                default:
                    return new BatchExecuteResponse { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = 1 } };
            }
        }
    }
}
