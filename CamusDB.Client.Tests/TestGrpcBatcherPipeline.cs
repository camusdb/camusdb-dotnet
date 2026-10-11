/**
 * This file is part of CamusDB
 *
 * Offline coverage for pipelines in the gRPC batcher: the ops of one transaction are written as one
 * frame — a START its followers name by txn_start_ref, statements, a COMMIT_IF_OK — only to a stream
 * whose server announced the pipeline contract; on any other stream nothing is written and every op
 * faults before a byte left; a pipeline is all-or-nothing under cancellation; the transaction a
 * pipeline begins is tracked by the handle its START minted. Driven by a fake IBatchTransport that
 * models the server's pipeline rules, so what is written and answered is decided by the test.
 */

using System.Collections.Concurrent;
using System.Threading.Channels;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcBatcherPipeline
{
    private static SqlRequest Sql(string sql) => new() { Sql = sql, Database = "db" };

    private static SqlRequest ByHandle(string sql, TxnHandle handle) => new() { Sql = sql, Database = "db", TxnHandle = handle };

    private static PipelineOp Start() => new(BatchStatementKind.Start, new SqlRequest { Database = "db" });

    private static PipelineOp Query(string sql) => new(BatchStatementKind.Query, Sql(sql));

    private static PipelineOp NonQuery(string sql) => new(BatchStatementKind.NonQuery, Sql(sql));

    private static PipelineOp NonQuery(string sql, TxnHandle handle) => new(BatchStatementKind.NonQuery, ByHandle(sql, handle));

    private static PipelineOp CommitIfOk(TxnHandle? handle = null)
    {
        SqlRequest request = new() { Database = "db" };
        if (handle is not null)
            request.TxnHandle = handle;
        return new PipelineOp(BatchStatementKind.CommitIfOk, request);
    }

    [Fact]
    public async Task AWholeTransactionIsOneFrameWhoseOpsNameTheirStart()
    {
        await using Harness h = new();

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline(
            [Start(), Query("select a"), NonQuery("update a"), CommitIfOk()], slotIndex: 0, default);
        await Task.WhenAll(tasks);

        Written frame = Assert.Single(h.Transport(0).Messages);
        Assert.True(frame.Frame, "the pipeline travelled as one frame");
        Assert.Equal(["<Start>", "select a", "update a", "<CommitIfOk>"], frame.Ops);

        TxnHandle handle = Assert.IsType<TxnHandle>(tasks[0].Result);
        int startId = frame.Items[0].RequestId;
        foreach (BatchExecuteRequest item in frame.Items.Skip(1))
        {
            Assert.Equal(startId, item.TxnStartRef);
            Assert.Null(item.Request.TxnHandle);
        }

        Assert.Equal(2, Assert.IsType<BatchQueryResult>(tasks[1].Result).Rows.Count);
        Assert.Equal(1, Assert.IsType<BatchNonQueryResult>(tasks[2].Result).AffectedRows);
        Assert.Equal(5, Assert.IsType<BatchCausalToken>(tasks[3].Result).L);
        Assert.Equal([handle.TxnIdPt], h.Transport(0).Committed);
    }

    [Fact]
    public async Task TheSecondFrameOfATransactionGoesByHandleAndEndsIt()
    {
        await using Harness h = new();

        Task<object?>[] first = h.Batcher.EnqueuePipeline([Start(), Query("select a")], slotIndex: 0, default);
        await Task.WhenAll(first);
        TxnHandle handle = (TxnHandle)first[0].Result!;

        Task<object?>[] second = h.Batcher.EnqueuePipeline(
            [NonQuery("update a", handle), NonQuery("update b", handle), CommitIfOk(handle)], slotIndex: 0, default);
        await Task.WhenAll(second);

        Assert.Equal(2, h.Transport(0).Messages.Length);
        Assert.Equal(["update a", "update b", "<CommitIfOk>"], h.Transport(0).Messages[1].Ops);
        Assert.All(h.Transport(0).Messages[1].Items, item => Assert.Equal(0, item.TxnStartRef));
        Assert.Equal([handle.TxnIdPt], h.Transport(0).Committed);
    }

    [Fact]
    public async Task AFailedStatementFailsTheConditionalCommitWithTheSameFailure()
    {
        await using Harness h = new();

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline(
            [Start(), NonQuery("update a"), NonQuery("fail b"), NonQuery("update c"), CommitIfOk()], slotIndex: 0, default);
        await Task.WhenAll(tasks.Select(t => t.ContinueWith(_ => { })));

        Assert.True(tasks[0].IsCompletedSuccessfully);
        Assert.True(tasks[1].IsCompletedSuccessfully);
        CamusException failed = Assert.IsType<CamusException>(tasks[2].Exception!.InnerException);
        Assert.Equal("CADB0400", failed.Code);
        Assert.IsType<CamusException>(tasks[3].Exception!.InnerException);
        CamusException commit = Assert.IsType<CamusException>(tasks[4].Exception!.InnerException);
        Assert.Equal(failed.Code, commit.Code);
        Assert.Equal(failed.Message, commit.Message);
        Assert.Empty(h.Transport(0).Committed);
    }

    [Fact]
    public async Task AServerBelowThePipelineContractGetsNothingAndEveryOpFaultsFirst()
    {
        await using Harness h = new(version: 1);

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline([Start(), NonQuery("update a"), CommitIfOk()], slotIndex: 0, default);

        foreach (Task<object?> task in tasks)
            await Assert.ThrowsAsync<PipelineUnsupportedException>(() => task);

        Assert.Empty(h.Transport(0).Messages);

        // The stream is untouched by the refusal: ordinary work still flows on it.
        await h.Batcher.EnqueueNonQueryAsync(Sql("after"), slotIndex: 0, default);
        Assert.Equal(["after"], Assert.Single(h.Transport(0).Messages).Ops);
    }

    [Fact]
    public async Task FramesOffRefusesAPipelineToo()
    {
        await using Harness h = new(requestFrames: false);

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline([Start(), NonQuery("update a")], slotIndex: 0, default);

        foreach (Task<object?> task in tasks)
            await Assert.ThrowsAsync<PipelineUnsupportedException>(() => task);
        Assert.Empty(h.Transport(0).Messages);
    }

    [Fact]
    public async Task APipelineThatDoesNotFitOneFrameIsRefusedWhole()
    {
        await using Harness h = new();

        List<PipelineOp> ops = [Start()];
        for (int i = 0; i < BatchFrames.MaxItems; i++)
            ops.Add(NonQuery($"update {i}"));
        ops.Add(CommitIfOk());

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline(ops, slotIndex: 0, default);

        foreach (Task<object?> task in tasks)
            await Assert.ThrowsAsync<PipelineUnsupportedException>(() => task);
        Assert.Empty(h.Transport(0).Messages);
    }

    [Fact]
    public async Task PlainStatementsByHandleNeedNoContractAndStayInOrder()
    {
        await using Harness h = new(version: 1);

        TxnHandle handle = await h.Batcher.EnqueueStartAsync(new SqlRequest { Database = "db" }, slotIndex: 0, default);

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline(
            [NonQuery("update a", handle), NonQuery("update b", handle)], slotIndex: 0, default);
        await Task.WhenAll(tasks);

        // Written in order, as ordinary ops: on this version-1 stream they may share a plain frame,
        // but nothing of the pipeline contract is in them.
        Assert.Equal(["update a", "update b"], h.Transport(0).Messages.Skip(1).SelectMany(m => m.Ops));
        Assert.All(h.Transport(0).Messages.SelectMany(m => m.Items), item => Assert.Equal(0, item.TxnStartRef));
    }

    [Fact]
    public async Task ACancelledPipelineIsLeftOutWhole()
    {
        await using Harness h = new();
        using CancellationTokenSource cts = new();

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task<object?>[] tasks = h.Batcher.EnqueuePipeline([Start(), NonQuery("update a"), CommitIfOk()], slotIndex: 0, cts.Token);
        Task after = h.Batcher.EnqueueNonQueryAsync(Sql("after"), slotIndex: 0, default);

        cts.Cancel();
        h.Unpark();

        foreach (Task<object?> task in tasks)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        await Task.WhenAll(blocker, after);

        Assert.DoesNotContain(h.Transport(0).Messages, m => m.Ops.Contains("<Start>") || m.Ops.Contains("<CommitIfOk>"));
        Assert.Contains(h.Transport(0).Messages, m => m.Ops.Contains("after"));
    }

    [Fact]
    public async Task ATransactionBegunByAPipelineKeepsItsStreamAcrossARotationAndReleasesItWhenCommitted()
    {
        await using Harness h = new();

        Task<object?>[] first = h.Batcher.EnqueuePipeline([Start(), Query("select a")], slotIndex: 0, default);
        await Task.WhenAll(first);
        TxnHandle handle = (TxnHandle)first[0].Result!;

        // The credential renewal retires the first stream; the transaction must finish there.
        h.Credential.Token = "token-2";
        await h.Batcher.EnqueueNonQueryAsync(Sql("elsewhere"), slotIndex: 0, default);
        Assert.Equal(2, h.Transports.Count);
        Assert.False(h.Transport(0).Disposed, "the retired stream stays open for the transaction that began on it");

        Task<object?>[] second = h.Batcher.EnqueuePipeline([NonQuery("update a", handle), CommitIfOk(handle)], slotIndex: 0, default);
        await Task.WhenAll(second);

        Assert.Equal(["update a", "<CommitIfOk>"], h.Transport(0).Messages[^1].Ops);
        Assert.Equal(["elsewhere"], h.Transport(1).Messages.Single().Ops);
        Assert.True(await EventuallyAsync(() => h.Transport(0).Disposed), "the conditional commit ended the transaction, so the retired stream closed");
    }

    [Fact]
    public async Task AStalePreparedExecutionRefusesThePipelineBeforeAnyWrite()
    {
        await using Harness h = new();

        PipelineOp stale = new(BatchStatementKind.NonQuery, Sql("update a"), ExpectedTransportId: 999);
        Task<object?>[] tasks = h.Batcher.EnqueuePipeline([Start(), stale, CommitIfOk()], slotIndex: 0, default);

        foreach (Task<object?> task in tasks)
            await Assert.ThrowsAsync<PreparedStatementStaleException>(() => task);
        Assert.Empty(h.Transport(0).Messages);
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        for (int i = 0; i < 500 && !condition(); i++)
            await Task.Delay(10);

        return condition();
    }

    private sealed class Credential
    {
        public volatile string? Token = "token-1";
    }

    /// <summary>What one <c>SendAsync</c> carried: a frame or a single message, its ops by SQL (or by
    /// kind), and the items as the server would read them.</summary>
    private sealed record Written(bool Frame, string[] Ops, BatchExecuteRequest[] Items);

    private sealed class Harness : IAsyncDisposable
    {
        public readonly Credential Credential = new();
        public readonly ConcurrentQueue<PipelineTransport> Transports = new();
        public readonly GrpcBatcher Batcher;

        private volatile TaskCompletionSource? parked;

        public Harness(int version = BatchFrames.Version, bool requestFrames = true)
        {
            Batcher = new GrpcBatcher(
                new GrpcBatchOptions { ChannelPoolSize = 1, RequestFrames = requestFrames },
                id =>
                {
                    PipelineTransport transport = new(id, this, version);
                    Transports.Enqueue(transport);
                    return transport;
                },
                () => Credential.Token);
        }

        public PipelineTransport Transport(int ordinal) => Transports.ElementAt(ordinal);

        public Task? Parked => parked?.Task;

        public void Park() => parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Unpark()
        {
            TaskCompletionSource? gate = parked;
            parked = null;
            gate?.TrySetResult();
        }

        public ValueTask DisposeAsync()
        {
            Unpark();
            return Batcher.DisposeAsync();
        }
    }

    /// <summary>
    /// An in-process stream that models the server's pipeline rules: a START mints a handle; an op that
    /// names a START runs on that handle; a statement whose SQL starts with "fail" fails and ends its
    /// transaction; a COMMIT_IF_OK answers the transaction's first failure, or commits. Everything else
    /// answers by kind.
    /// </summary>
    private sealed class PipelineTransport(long id, Harness harness, int version) : IBatchTransport
    {
        private readonly Channel<BatchExecuteResponse> channel = Channel.CreateUnbounded<BatchExecuteResponse>();
        private readonly ConcurrentQueue<Written> messages = new();
        private readonly ConcurrentQueue<long> committed = new();
        private readonly Dictionary<int, TxnHandle> startsById = new();
        private readonly Dictionary<long, BatchError?> transactions = new();
        private long nextPt = 100;
        private int parkedWrites;
        private int disposed;

        public long Id { get; } = id;

        public int AnnouncedVersion => version;

        public Written[] Messages => [.. messages];

        public long[] Committed => [.. committed];

        public int ParkedWrites => Volatile.Read(ref parkedWrites);

        public bool Disposed => Volatile.Read(ref disposed) != 0;

        public async Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            if (Disposed)
                throw new IOException("written to a closed stream");

            bool frame = request.Kind == BatchStatementKind.Frame;
            Assert.True(!frame || version >= 1, "a frame was written to a stream that never announced frames");

            BatchExecuteRequest[] ops = frame ? [.. request.Items.Select(item => item.Clone())] : [request.Clone()];

            foreach (BatchExecuteRequest op in ops)
            {
                Assert.True(frame || op.TxnStartRef == 0, "txn_start_ref outside a frame");
                Assert.True(version >= BatchFrames.PipelineVersion || op.TxnStartRef == 0, "txn_start_ref on a stream below the pipeline contract");
                Assert.True(version >= BatchFrames.PipelineVersion || op.Kind != BatchStatementKind.CommitIfOk, "COMMIT_IF_OK on a stream below the pipeline contract");
            }

            if (harness.Parked is { } gate)
            {
                Interlocked.Increment(ref parkedWrites);
                await gate.WaitAsync(cancellationToken);
            }

            messages.Enqueue(new Written(frame, [.. ops.Select(Name)], ops));

            foreach (BatchExecuteRequest op in ops)
                foreach (BatchExecuteResponse response in Respond(op))
                    channel.Writer.TryWrite(response);
        }

        public IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken)
            => channel.Reader.ReadAllAsync(cancellationToken);

        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref disposed, 1);
            channel.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }

        private static string Name(BatchExecuteRequest op)
            => op.Request?.Sql is { Length: > 0 } sql ? sql : $"<{op.Kind}>";

        private IEnumerable<BatchExecuteResponse> Respond(BatchExecuteRequest request)
        {
            int id = request.RequestId;

            if (request.Kind == BatchStatementKind.Start)
            {
                TxnHandle handle = new() { TxnIdPt = nextPt++, TxnIdCounter = 1 };
                startsById[id] = handle;
                transactions[handle.TxnIdPt] = null;
                yield return new BatchExecuteResponse { RequestId = id, StartReply = handle };
                yield break;
            }

            TxnHandle? txn = request.TxnStartRef != 0
                ? startsById.GetValueOrDefault(request.TxnStartRef)
                : request.Request?.TxnHandle;
            BatchError? failed = txn is not null ? transactions.GetValueOrDefault(txn.TxnIdPt) : null;

            switch (request.Kind)
            {
                case BatchStatementKind.Query:
                    if (failed is not null)
                    {
                        yield return new BatchExecuteResponse { RequestId = id, Error = Unknown() };
                        yield break;
                    }
                    ResultSchema schema = new();
                    schema.Columns.Add(new ColumnSchema { Name = "echo", Type = CamusDB.Grpc.ColumnType.String });
                    yield return new BatchExecuteResponse { RequestId = id, Schema = schema };
                    for (int i = 0; i < 2; i++)
                    {
                        ResultRow row = new();
                        row.Values.Add(new Value { StringValue = request.Request?.Sql ?? "" });
                        yield return new BatchExecuteResponse { RequestId = id, Row = row };
                    }
                    yield return new BatchExecuteResponse { RequestId = id, QueryComplete = new QueryComplete { Total = 2, CausalTokenL = 5 } };
                    break;

                case BatchStatementKind.NonQuery:
                    if (failed is not null)
                    {
                        yield return new BatchExecuteResponse { RequestId = id, Error = Unknown() };
                        yield break;
                    }
                    if (request.Request?.Sql.StartsWith("fail", StringComparison.Ordinal) == true)
                    {
                        BatchError error = new() { Code = "CADB0400", Message = "asked to fail: " + request.Request.Sql };
                        if (txn is not null)
                            transactions[txn.TxnIdPt] = error;
                        yield return new BatchExecuteResponse { RequestId = id, Error = error };
                        yield break;
                    }
                    yield return new BatchExecuteResponse { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = 1 } };
                    break;

                case BatchStatementKind.Commit:
                case BatchStatementKind.CommitIfOk:
                    if (txn is null)
                    {
                        yield return new BatchExecuteResponse { RequestId = id, Error = new BatchError { Code = "CADB0400", Message = "no transaction" } };
                        yield break;
                    }
                    if (failed is not null)
                    {
                        transactions.Remove(txn.TxnIdPt);
                        yield return new BatchExecuteResponse { RequestId = id, Error = failed };
                        yield break;
                    }
                    transactions.Remove(txn.TxnIdPt);
                    committed.Enqueue(txn.TxnIdPt);
                    yield return new BatchExecuteResponse { RequestId = id, CommitReply = new CommitReply { CausalTokenL = 5 } };
                    break;

                case BatchStatementKind.Rollback:
                    if (txn is not null)
                        transactions.Remove(txn.TxnIdPt);
                    yield return new BatchExecuteResponse { RequestId = id, RollbackReply = new RollbackReply() };
                    break;
            }
        }

        private static BatchError Unknown() => new() { Code = "CADB0400", Message = "Unknown transaction" };
    }
}
