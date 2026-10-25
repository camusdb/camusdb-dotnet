/**
 * This file is part of CamusDB
 *
 * Offline coverage for the gRPC streaming reader (CamusDB.Client.Transport.GrpcStreamRowSource): schema
 * first, rows decoded one message at a time, HasRows via first-row prefetch, trailing messages skipped,
 * the causal token read from the trailers, a failure after rows surfacing as a CamusException, the
 * per-read timeout, cancellation, and an early dispose cancelling the call. Driven from an in-memory
 * stream, so no server is required.
 */

using System.Threading.Channels;
using CamusDB.Client.Transport;
using Grpc.Core;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcStreamRowSource
{
    /// <summary>
    /// A response stream fed through a channel, so a test decides when each message arrives. The call's
    /// own token (the one the source's idle timer cancels) ends a pending read the way gRPC does: with
    /// <c>StatusCode.Cancelled</c>.
    /// </summary>
    private sealed class FakeStream(CancellationToken callToken) : IAsyncStreamReader<Grpc.QueryStreamMessage>
    {
        private readonly Channel<Func<Grpc.QueryStreamMessage>> channel = Channel.CreateUnbounded<Func<Grpc.QueryStreamMessage>>();

        public Grpc.QueryStreamMessage Current { get; private set; } = null!;

        public void Send(Grpc.QueryStreamMessage message) => channel.Writer.TryWrite(() => message);

        public void Fail(RpcException exception) => channel.Writer.TryWrite(() => throw exception);

        public void End() => channel.Writer.TryComplete();

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, callToken);
            try
            {
                if (!await channel.Reader.WaitToReadAsync(linked.Token))
                    return false;
            }
            catch (OperationCanceledException)
            {
                throw new RpcException(new Status(StatusCode.Cancelled, "Call canceled by the client."));
            }

            channel.Reader.TryRead(out Func<Grpc.QueryStreamMessage>? next);
            Current = next!();
            return true;
        }
    }

    private sealed class Harness
    {
        public readonly CancellationTokenSource? Idle;
        public readonly FakeStream Stream;
        public readonly AsyncServerStreamingCall<Grpc.QueryStreamMessage> Call;
        public global::Grpc.Core.Metadata Trailers = [];
        public global::Grpc.Core.Metadata? Observed;
        public bool Disposed;

        public Harness(bool withTimeout = false)
        {
            Idle = withTimeout ? new CancellationTokenSource() : null;
            Stream = new FakeStream(Idle?.Token ?? default);
            Call = new AsyncServerStreamingCall<Grpc.QueryStreamMessage>(
                Stream,
                Task.FromResult(new global::Grpc.Core.Metadata()),
                () => Status.DefaultSuccess,
                () => Trailers,
                () => Disposed = true);
        }

        public async Task<CamusDataReader> ReaderAsync(int timeoutSeconds = 0, CancellationToken cancellationToken = default)
        {
            GrpcStreamRowSource source = await GrpcStreamRowSource.CreateAsync(
                Call,
                Idle,
                timeoutSeconds,
                ex => GrpcTransport.TranslateFailure(null, "http://test:5096", ex),
                trailers => Observed = trailers,
                cancellationToken);

            return new CamusDataReader(source);
        }
    }

    private static Grpc.QueryStreamMessage Schema() => new()
    {
        Schema = new Grpc.ResultSchema
        {
            Columns =
            {
                new Grpc.ColumnSchema { Name = "id", Type = Grpc.ColumnType.Id },
                new Grpc.ColumnSchema { Name = "n", Type = Grpc.ColumnType.Integer64 },
                new Grpc.ColumnSchema { Name = "ok", Type = Grpc.ColumnType.Bool },
            },
        },
    };

    private static Grpc.QueryStreamMessage Row(string id, long n, bool ok) => new()
    {
        Row = new Grpc.ResultRow
        {
            Values =
            {
                new Grpc.Value { IdValue = id },
                new Grpc.Value { Int64Value = n },
                new Grpc.Value { BoolValue = ok },
            },
        },
    };

    [Fact]
    public async Task SchemaIsReadableBeforeAndWithoutRows()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.End();

        await using CamusDataReader reader = await harness.ReaderAsync();

        Assert.Equal(3, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("n", reader.GetName(1));
        Assert.Equal("ok", reader.GetName(2));
        Assert.False(reader.HasRows);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task RowsDecodePositionallyAndTrailingMessagesAreSkipped()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("6849f3aa", 42, true));
        harness.Stream.Send(new Grpc.QueryStreamMessage { Row = new Grpc.ResultRow { Values = { new Grpc.Value { IdValue = "6849f3bb" } } } });
        harness.Stream.Send(new Grpc.QueryStreamMessage { CacheMetadata = new Grpc.CacheMetadata() });
        harness.Stream.Send(new Grpc.QueryStreamMessage { RoutingAdvice = new Grpc.RoutingAdvice() });
        harness.Stream.End();

        await using CamusDataReader reader = await harness.ReaderAsync();

        Assert.True(reader.HasRows);

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.Equal(42L, reader.GetInt64(1));
        Assert.True(reader.GetBoolean(2));

        // A row shorter than the schema reads as null in the missing columns, as on the buffered path.
        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3bb", reader.GetString(0));
        Assert.True(reader.IsDBNull(1));
        Assert.True(reader.IsDBNull(2));

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SyncReadDrainsAllRows()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.Send(Row("b", 2, true));
        harness.Stream.Send(Row("c", 3, false));
        harness.Stream.End();

        using CamusDataReader reader = await harness.ReaderAsync();

        int count = 0;
        while (reader.Read())
            count++;

        Assert.Equal(3, count);
    }

    [Fact]
    public async Task TrailersAreObservedOnlyAtTheEnd()
    {
        Harness harness = new();
        harness.Trailers = new global::Grpc.Core.Metadata
        {
            { "camus-causal-token-n", "1" },
            { "camus-causal-token-l", "1000" },
            { "camus-causal-token-c", "7" },
        };
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.Send(Row("b", 2, true));
        harness.Stream.End();

        await using CamusDataReader reader = await harness.ReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Null(harness.Observed);

        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
        Assert.Same(harness.Trailers, harness.Observed);
    }

    [Fact]
    public async Task FailureAfterRowsSurfacesAsCamusExceptionMidRead()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.Fail(new RpcException(
            new Status(StatusCode.Aborted, "aborted"),
            new global::Grpc.Core.Metadata { { "camus-error-code", "CADB0503" }, { "camus-error-message", "Transaction conflict" } }));

        await using CamusDataReader reader = await harness.ReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("a", reader.GetString(0));

        CamusException ex = await Assert.ThrowsAsync<CamusException>(async () => await reader.ReadAsync());
        Assert.Equal("CADB0503", ex.Code);
        Assert.Contains("Transaction conflict", ex.Message);
        Assert.Null(harness.Observed);

        // The stream is over after a failure; a further read reports the end instead of reading again.
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task MissingSchemaIsAProtocolErrorAndReleasesTheCall()
    {
        Harness harness = new();
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.End();

        await Assert.ThrowsAsync<CamusException>(() => harness.ReaderAsync());
        Assert.True(harness.Disposed);
    }

    [Fact]
    public async Task EarlyDisposeCancelsTheCall()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.Send(Row("b", 2, true));

        CamusDataReader reader = await harness.ReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.False(harness.Disposed);

        await reader.DisposeAsync();
        Assert.True(harness.Disposed);
    }

    [Fact]
    public async Task ServerThatStopsAnsweringTimesOut()
    {
        Harness harness = new(withTimeout: true);
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));

        await using CamusDataReader reader = await harness.ReaderAsync(timeoutSeconds: 1);
        Assert.True(await reader.ReadAsync());

        // No second row and no end: the wait for the next message runs into the timeout.
        CamusException ex = await Assert.ThrowsAsync<CamusException>(async () => await reader.ReadAsync());
        Assert.Contains("No response arrived within 1 s", ex.Message);
        Assert.True(harness.Idle!.IsCancellationRequested);
    }

    [Fact]
    public async Task TimeBetweenReadsIsNotCounted()
    {
        Harness harness = new(withTimeout: true);
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));
        harness.Stream.Send(Row("b", 2, true));
        harness.Stream.End();

        await using CamusDataReader reader = await harness.ReaderAsync(timeoutSeconds: 1);

        // A slow caller — a dump writing each row out — spends longer than the timeout between reads.
        Assert.True(await reader.ReadAsync());
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
        Assert.False(harness.Idle!.IsCancellationRequested);
    }

    [Fact]
    public async Task CallerCancellationIsReportedAsCancellation()
    {
        Harness harness = new();
        harness.Stream.Send(Schema());
        harness.Stream.Send(Row("a", 1, true));

        await using CamusDataReader reader = await harness.ReaderAsync();
        Assert.True(await reader.ReadAsync());

        using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.ReadAsync(cts.Token));
    }
}
