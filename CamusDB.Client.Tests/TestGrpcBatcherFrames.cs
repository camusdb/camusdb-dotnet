/**
 * This file is part of CamusDB
 *
 * Offline coverage for BatchExecute stream frames in the gRPC batcher: ops that wait together are written
 * as one stream message, but only to a stream whose server announced it reads frames; every per-op duty
 * (routing, the prepared-statement check, cancellation, the hold on the stream) stays per op; both limits
 * hold; a response frame fans out exactly as the same messages would one by one. Driven by a fake
 * IBatchTransport whose writes can be parked, so what "waits together" is decided by the test and not by
 * timing.
 */

using System.Collections.Concurrent;
using System.Threading.Channels;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcBatcherFrames
{
    private static readonly TxnHandle Handle = new() { TxnIdPt = 111, TxnIdCounter = 222 };

    private static SqlRequest Sql(string sql) => new() { Sql = sql };

    private static SqlRequest InTransaction(string sql) => new() { Sql = sql, TxnHandle = Handle };

    [Fact]
    public async Task OpsThatWaitTogetherShareOneFrameInInboxOrder()
    {
        await using Harness h = new();

        Task[] ops = await h.EnqueueBehindParkedWriteAsync("a", "b", "c", "d");
        await Task.WhenAll(ops);

        Assert.Equal(
            [new Written(false, ["blocker"]), new Written(true, ["a", "b", "c", "d"])],
            h.Transport(0).Messages);
    }

    [Fact]
    public async Task ALoneOpIsThePlainSingleMessage()
    {
        await using Harness h = new();

        await h.Batcher.EnqueueNonQueryAsync(Sql("one"), slotIndex: 0, default);
        await h.Batcher.EnqueueNonQueryAsync(Sql("two"), slotIndex: 0, default);

        Assert.Equal([new Written(false, ["one"]), new Written(false, ["two"])], h.Transport(0).Messages);
    }

    [Fact]
    public async Task AServerThatAnnouncesNothingNeverReceivesAFrame()
    {
        await using Harness h = new(announce: false);

        Task[] ops = await h.EnqueueBehindParkedWriteAsync("a", "b", "c");
        await Task.WhenAll(ops);

        Assert.All(h.Transport(0).Messages, message => Assert.False(message.Frame));
        Assert.Equal(["blocker", "a", "b", "c"], h.Transport(0).Messages.SelectMany(message => message.Ops));
    }

    [Fact]
    public async Task FramesStartOnlyAfterTheAnnouncementArrives()
    {
        await using Harness h = new(announce: false);

        Task[] before = await h.EnqueueBehindParkedWriteAsync("a", "b");
        await Task.WhenAll(before);
        Assert.All(h.Transport(0).Messages, message => Assert.False(message.Frame));

        h.Transport(0).Announce();

        Task[] after = await h.EnqueueBehindParkedWriteAsync("c", "d");
        await Task.WhenAll(after);

        Assert.Equal(new Written(true, ["c", "d"]), h.Transport(0).Messages[^1]);
    }

    [Fact]
    public async Task TheOptionTurnsFramesOff()
    {
        await using Harness h = new(requestFrames: false);

        Task[] ops = await h.EnqueueBehindParkedWriteAsync("a", "b", "c");
        await Task.WhenAll(ops);

        Assert.All(h.Transport(0).Messages, message => Assert.False(message.Frame));
        Assert.Equal(4, h.Transport(0).Messages.Length);
    }

    [Fact]
    public async Task OneDrainForTwoStreamsPutsEachOpOnItsOwnStreamInOrder()
    {
        await using Harness h = new();

        TxnHandle started = await h.Batcher.EnqueueStartAsync(Sql(""), slotIndex: 0, default);
        Assert.Equal(Handle.TxnIdPt, started.TxnIdPt);

        // The renewal retires the first stream; the transaction that began on it must finish there.
        h.Credential.Token = "token-2";

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transports.Count == 2 && h.Transport(1).ParkedWrites == 1));

        Task[] ops =
        [
            h.Batcher.EnqueueNonQueryAsync(InTransaction("t1"), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(InTransaction("t2"), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql("a1"), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql("a2"), slotIndex: 0, default),
        ];

        h.Unpark();
        await Task.WhenAll([blocker, .. ops]);

        Assert.Equal([new Written(false, ["<Start>"]), new Written(true, ["t1", "t2"])], h.Transport(0).Messages);
        Assert.Equal([new Written(false, ["blocker"]), new Written(true, ["a1", "a2"])], h.Transport(1).Messages);
    }

    [Fact]
    public async Task AStalePreparedExecutionInsideADrainFaultsAlone()
    {
        await using Harness h = new();

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task a = h.Batcher.EnqueueNonQueryAsync(Sql("a"), slotIndex: 0, default);
        Task stale = h.Batcher.EnqueueNonQueryAsync(Sql("stale"), slotIndex: 0, default, expectedTransportId: 999);
        Task b = h.Batcher.EnqueueNonQueryAsync(Sql("b"), slotIndex: 0, default);

        h.Unpark();

        await Assert.ThrowsAsync<PreparedStatementStaleException>(() => stale);
        await Task.WhenAll(blocker, a, b);

        Assert.Equal(new Written(true, ["a", "b"]), h.Transport(0).Messages[^1]);
    }

    [Fact]
    public async Task AnOpCancelledInTheInboxIsLeftOut()
    {
        await using Harness h = new();
        using CancellationTokenSource cts = new();

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task a = h.Batcher.EnqueueNonQueryAsync(Sql("a"), slotIndex: 0, default);
        Task cancelled = h.Batcher.EnqueueNonQueryAsync(Sql("cancelled"), slotIndex: 0, cts.Token);
        Task c = h.Batcher.EnqueueNonQueryAsync(Sql("c"), slotIndex: 0, default);

        cts.Cancel();
        h.Unpark();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
        await Task.WhenAll(blocker, a, c);

        Assert.Equal(new Written(true, ["a", "c"]), h.Transport(0).Messages[^1]);
    }

    [Fact]
    public async Task ACancelledRollbackIsStillWritten()
    {
        // Nothing else would release the transaction's locks on the server.
        await using Harness h = new();
        using CancellationTokenSource cts = new();

        await h.Batcher.EnqueueStartAsync(Sql(""), slotIndex: 0, default);

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task rollback = h.Batcher.EnqueueRollbackAsync(new SqlRequest { TxnHandle = Handle }, slotIndex: 0, cts.Token);
        cts.Cancel();
        h.Unpark();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rollback);
        await blocker;

        Assert.True(await EventuallyAsync(() => h.Transport(0).Messages.Any(message => message.Ops.Contains("<Rollback>"))));
    }

    [Fact]
    public async Task TheItemLimitSplitsADrain()
    {
        await using Harness h = new();

        string[] names = [.. Enumerable.Range(0, BatchFrames.MaxItems + 44).Select(i => $"op{i}")];
        Task[] ops = await h.EnqueueBehindParkedWriteAsync(names);
        await Task.WhenAll(ops);

        Written[] frames = [.. h.Transport(0).Messages.Skip(1)];

        Assert.Equal([BatchFrames.MaxItems, 44], frames.Select(frame => frame.Ops.Length));
        Assert.Equal(names, frames.SelectMany(frame => frame.Ops));
    }

    [Fact]
    public async Task TheByteBudgetSplitsADrainAndAnOversizedOpTravelsAlone()
    {
        await using Harness h = new();

        string large = new('x', 400 * 1024);              // two fit in a frame, three do not
        string oversized = new('y', BatchFrames.MaxBytes + 1024);

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task[] ops =
        [
            h.Batcher.EnqueueNonQueryAsync(Sql(large), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql(large), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql(large), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql(oversized), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql("small"), slotIndex: 0, default),
        ];

        h.Unpark();
        await Task.WhenAll([blocker, .. ops]);

        Written[] written = [.. h.Transport(0).Messages.Skip(1)];

        Assert.Equal([2, 1, 1, 1], written.Select(message => message.Ops.Length));
        Assert.Equal([true, false, false, false], written.Select(message => message.Frame));
        Assert.All(h.Transport(0).FrameSizes, size => Assert.InRange(size, 1, BatchFrames.MaxBytes + 16));
    }

    [Fact]
    public async Task AFramedResultIsTheSameAsTheUnframedOne()
    {
        await using Harness framed = new(frameResponses: true);
        await using Harness plain = new();

        BatchQueryResult a = await framed.Batcher.EnqueueQueryAsync(Sql("select"), slotIndex: 0, default);
        BatchQueryResult b = await plain.Batcher.EnqueueQueryAsync(Sql("select"), slotIndex: 0, default);

        Assert.Equal(b.Schema, a.Schema);
        Assert.Equal(b.Rows, a.Rows);
        Assert.Equal(2, a.Rows.Count);
        Assert.Equal("select", a.Rows[0].Values[0].StringValue);
        Assert.Equal(b.Token.L, a.Token.L);
    }

    [Fact]
    public async Task AResponseFrameIsReadFromAServerThatNeverAnnounced()
    {
        // Both forms are accepted at any time; reading one needs no negotiation.
        await using Harness h = new(announce: false, frameResponses: true);

        BatchNonQueryResult result = await h.Batcher.EnqueueNonQueryAsync(Sql("u"), slotIndex: 0, default);

        Assert.Equal(3, result.AffectedRows);
    }

    [Fact]
    public async Task AFrameInsideAResponseFrameIsDropped()
    {
        await using Harness h = new(frameResponses: true, nestResponses: true);

        using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(300));

        // The only answer travels inside a nested frame, so it must never arrive.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => h.Batcher.EnqueueNonQueryAsync(Sql("u"), slotIndex: 0, cts.Token));
    }

    [Fact]
    public async Task AFailedFrameWriteFaultsEveryOpInItAndIsNotResent()
    {
        await using Harness h = new();

        h.Park();
        Task blocker = h.Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
        Assert.True(await EventuallyAsync(() => h.Transport(0).ParkedWrites == 1));

        Task[] ops =
        [
            h.Batcher.EnqueueNonQueryAsync(Sql("a"), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql("b"), slotIndex: 0, default),
            h.Batcher.EnqueueNonQueryAsync(Sql("c"), slotIndex: 0, default),
        ];

        h.Transport(0).FailFrames = true;
        h.Unpark();
        await blocker;

        foreach (Task op in ops)
            await Assert.ThrowsAsync<IOException>(() => op);

        Assert.Equal(1, h.Transport(0).FrameAttempts);
    }

    [Fact]
    public async Task ConcurrentCallersAreAllAnsweredWithTheirOwnResult()
    {
        await using Harness h = new(frameResponses: true);

        Task<BatchQueryResult>[] ops =
        [
            .. Enumerable.Range(0, 500).Select(i => Task.Run(
                () => h.Batcher.EnqueueQueryAsync(Sql($"q{i}"), slotIndex: null, default))),
        ];

        BatchQueryResult[] results = await Task.WhenAll(ops);

        for (int i = 0; i < results.Length; i++)
            Assert.Equal($"q{i}", results[i].Rows[0].Values[0].StringValue);
    }

    [Theory]
    [InlineData("1", true)]
    [InlineData("2", true)]
    [InlineData("0", false)]
    [InlineData("", false)]
    [InlineData("yes", false)]
    [InlineData(null, false)]
    public void OnlyAVersionThisClientWritesIsAnAnnouncement(string? headerValue, bool expected)
        => Assert.Equal(expected, BatchFrames.Announces(headerValue));

    [Fact]
    public void RequestFramesIsOnByDefaultAndReadFromTheConnectionString()
    {
        Assert.True(new CamusConnectionStringBuilder("Endpoint=http://localhost:8082;Database=test").BatchOptions.RequestFrames);
        Assert.False(new CamusConnectionStringBuilder("Endpoint=http://localhost:8082;Database=test;RequestFrames=false").BatchOptions.RequestFrames);
        Assert.True(new CamusConnectionStringBuilder("Endpoint=http://localhost:8082;Database=test;RequestFrames=nope").BatchOptions.RequestFrames);
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

    /// <summary>What one <c>SendAsync</c> carried: a frame or a single message, and its ops by SQL (or by
    /// kind, for an op with no SQL).</summary>
    private sealed record Written(bool Frame, string[] Ops)
    {
        public bool Equals(Written? other)
            => other is not null && Frame == other.Frame && Ops.SequenceEqual(other.Ops);

        public override int GetHashCode() => HashCode.Combine(Frame, Ops.Length);

        public override string ToString() => $"{(Frame ? "frame" : "single")}[{string.Join(",", Ops)}]";
    }

    private sealed class Harness : IAsyncDisposable
    {
        public readonly Credential Credential = new();
        public readonly ConcurrentQueue<FrameTransport> Transports = new();
        public readonly GrpcBatcher Batcher;

        private volatile TaskCompletionSource? parked;

        public Harness(bool announce = true, bool requestFrames = true, bool frameResponses = false, bool nestResponses = false)
        {
            Batcher = new GrpcBatcher(
                new GrpcBatchOptions { ChannelPoolSize = 1, RequestFrames = requestFrames },
                id =>
                {
                    FrameTransport transport = new(id, this, announce, frameResponses, nestResponses);
                    Transports.Enqueue(transport);
                    return transport;
                },
                () => Credential.Token);
        }

        public FrameTransport Transport(int ordinal) => Transports.ElementAt(ordinal);

        public Task? Parked => parked?.Task;

        /// <summary>From now on every write waits, so ops enqueued meanwhile pile up in the inbox.</summary>
        public void Park() => parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Unpark()
        {
            TaskCompletionSource? gate = parked;
            parked = null;
            gate?.TrySetResult();
        }

        /// <summary>Parks a first write, enqueues <paramref name="sqls"/> behind it in order, and lets go —
        /// so they are all waiting when the pump comes back, which is what makes a frame.</summary>
        public async Task<Task[]> EnqueueBehindParkedWriteAsync(params string[] sqls)
        {
            FrameTransport transport = Transport(0);
            int parkedBefore = transport.ParkedWrites;

            Park();
            Task blocker = Batcher.EnqueueNonQueryAsync(Sql("blocker"), slotIndex: 0, default);
            Assert.True(await EventuallyAsync(() => transport.ParkedWrites == parkedBefore + 1));

            Task[] ops = [.. sqls.Select(sql => (Task)Batcher.EnqueueNonQueryAsync(Sql(sql), slotIndex: 0, default))];

            Unpark();
            return [blocker, .. ops];
        }

        public ValueTask DisposeAsync()
        {
            Unpark();
            return Batcher.DisposeAsync();
        }
    }

    /// <summary>
    /// An in-process stream that reads request frames when told to announce them, records every message
    /// as written, and answers each op by kind — optionally packing all the answers to one message into a
    /// response frame. It copies what it records: the batcher reuses its frame envelope.
    /// </summary>
    private sealed class FrameTransport(long id, Harness harness, bool announce, bool frameResponses, bool nestResponses)
        : IBatchTransport
    {
        private readonly Channel<BatchExecuteResponse> channel = Channel.CreateUnbounded<BatchExecuteResponse>();
        private readonly ConcurrentQueue<Written> messages = new();
        private readonly ConcurrentQueue<int> frameSizes = new();
        private volatile bool announced = announce;
        private int parkedWrites;
        private int frameAttempts;
        private int disposed;

        public long Id { get; } = id;

        public bool FramesAnnounced => announced;

        public volatile bool FailFrames;

        public Written[] Messages => [.. messages];

        public int[] FrameSizes => [.. frameSizes];

        public int ParkedWrites => Volatile.Read(ref parkedWrites);

        public int FrameAttempts => Volatile.Read(ref frameAttempts);

        public void Announce() => announced = true;

        public async Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            if (Volatile.Read(ref disposed) != 0)
                throw new IOException("written to a closed stream");

            bool frame = request.Kind == BatchStatementKind.Frame;

            Assert.True(!frame || announced, "a frame was written to a stream that never announced frames");
            Assert.True(frame ? request.Request is null : request.Items.Count == 0, "a message must carry items or a request, not both");

            // Copied before anything is awaited, as a real transport serializes before it returns.
            BatchExecuteRequest[] ops = frame ? [.. request.Items.Select(item => item.Clone())] : [request.Clone()];

            if (frame)
            {
                Interlocked.Increment(ref frameAttempts);
                frameSizes.Enqueue(request.CalculateSize());

                if (FailFrames)
                    throw new IOException("stream reset");
            }

            if (harness.Parked is { } gate)
            {
                Interlocked.Increment(ref parkedWrites);
                await gate.WaitAsync(cancellationToken);
            }

            messages.Enqueue(new Written(frame, [.. ops.Select(Name)]));

            List<BatchExecuteResponse> responses = [.. ops.SelectMany(Respond)];

            if (!frameResponses)
            {
                foreach (BatchExecuteResponse response in responses)
                    channel.Writer.TryWrite(response);

                return;
            }

            BatchResponseFrame packed = new();
            packed.Items.AddRange(responses);

            if (nestResponses)
            {
                BatchResponseFrame outer = new();
                outer.Items.Add(new BatchExecuteResponse { Frame = packed });
                packed = outer;
            }

            channel.Writer.TryWrite(new BatchExecuteResponse { Frame = packed });
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

        private static IEnumerable<BatchExecuteResponse> Respond(BatchExecuteRequest request)
        {
            int id = request.RequestId;

            switch (request.Kind)
            {
                case BatchStatementKind.Query:
                    ResultSchema schema = new();
                    schema.Columns.Add(new ColumnSchema { Name = "echo", Type = Grpc.ColumnType.String });
                    yield return new BatchExecuteResponse { RequestId = id, Schema = schema };

                    for (int i = 0; i < 2; i++)
                    {
                        ResultRow row = new();
                        row.Values.Add(new Value { StringValue = request.Request?.Sql ?? "" });
                        yield return new BatchExecuteResponse { RequestId = id, Row = row };
                    }

                    yield return new BatchExecuteResponse
                    {
                        RequestId = id,
                        QueryComplete = new QueryComplete { Total = 2, CausalTokenN = 7, CausalTokenL = 5, CausalTokenC = 6 },
                    };
                    break;

                case BatchStatementKind.NonQuery:
                    yield return new BatchExecuteResponse { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = 3 } };
                    break;

                case BatchStatementKind.Start:
                    yield return new BatchExecuteResponse { RequestId = id, StartReply = Handle.Clone() };
                    break;

                case BatchStatementKind.Commit:
                    yield return new BatchExecuteResponse { RequestId = id, CommitReply = new CommitReply() };
                    break;

                case BatchStatementKind.Rollback:
                    yield return new BatchExecuteResponse { RequestId = id, RollbackReply = new RollbackReply() };
                    break;
            }
        }
    }
}
