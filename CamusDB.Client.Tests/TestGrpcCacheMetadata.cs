/**
 * This file is part of CamusDB
 *
 * Offline coverage for the gRPC cache-metadata path: the server folds a {cache=…} verdict into the
 * QUERY terminator (it is known only once the cursor has drained), the batcher carries it out of the
 * demultiplexer, and CamusCacheMetadata.FromProto maps it to the same shape the REST envelope yields.
 * Driven by a fake IBatchTransport, so no server is needed.
 */

using System.Threading.Channels;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcCacheMetadata
{
    [Fact]
    public void FromProtoReturnsNullWhenTheQueryWasNotHinted()
    {
        // Absent metadata is the wire signal for "no cache hint" — it must not become an
        // all-defaults object, or every ordinary query would look like a bypass.
        Assert.Null(CamusCacheMetadata.FromProto(null));
    }

    [Fact]
    public void FromProtoMapsAHitIncludingHlcAndAge()
    {
        CamusCacheMetadata? meta = CamusCacheMetadata.FromProto(new Grpc.CacheMetadata
        {
            Status = "hit",
            Name = "orders_all",
            CachedAtHlc = new HlcTimestamp { L = 1234, C = 7 },
            AgeMs = 42,
        });

        Assert.NotNull(meta);
        Assert.Equal(CamusCacheStatus.Hit, meta!.Status);
        Assert.True(meta.IsHit);
        Assert.Equal("orders_all", meta.Name);
        Assert.Null(meta.BypassReason);
        Assert.Equal(1234, meta.CachedAtHlc!.Value.L);
        Assert.Equal(7u, meta.CachedAtHlc!.Value.C);
        Assert.Equal(42, meta.AgeMs);
    }

    [Fact]
    public void FromProtoMapsABypassAndLeavesHitOnlyFieldsNull()
    {
        CamusCacheMetadata? meta = CamusCacheMetadata.FromProto(new Grpc.CacheMetadata
        {
            Status = "bypass",
            BypassReason = "in-flight-write",
            Name = "orders_all",
        });

        Assert.NotNull(meta);
        Assert.Equal(CamusCacheStatus.Bypass, meta!.Status);
        Assert.False(meta.IsHit);
        Assert.Equal("in-flight-write", meta.BypassReason);
        Assert.Null(meta.CachedAtHlc);
        Assert.Null(meta.AgeMs);
    }

    [Fact]
    public async Task BatcherCarriesTheTerminatorsCacheVerdict()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        BatchQueryResult result = await batcher.EnqueueQueryAsync(
            new SqlRequest { Sql = "hinted" }, slotIndex: null, default);

        Assert.NotNull(result.CacheMetadata);
        Assert.Equal("hit", result.CacheMetadata!.Status);
        Assert.Equal("orders_all", result.CacheMetadata.Name);
        Assert.True(result.CacheMetadata.HasAgeMs);
    }

    [Fact]
    public async Task BatcherReportsNoVerdictForAnUnhintedQuery()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        BatchQueryResult result = await batcher.EnqueueQueryAsync(
            new SqlRequest { Sql = "plain" }, slotIndex: null, default);

        Assert.Null(result.CacheMetadata);
    }

    /// <summary>An in-process <see cref="IBatchTransport"/> that answers a QUERY with a schema, one row, and a
    /// terminator whose cache verdict is present only when the request's SQL asks for it — mirroring a server
    /// that omits the message entirely for an unhinted statement.</summary>
    private sealed class FakeTransport(long id) : IBatchTransport
    {
        private readonly Channel<BatchExecuteResponse> channel = Channel.CreateUnbounded<BatchExecuteResponse>();

        public long Id { get; } = id;

        public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        {
            foreach (BatchExecuteResponse response in Respond(request))
                channel.Writer.TryWrite(response);
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken)
            => channel.Reader.ReadAllAsync(cancellationToken);

        public ValueTask DisposeAsync()
        {
            channel.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }

        private static IEnumerable<BatchExecuteResponse> Respond(BatchExecuteRequest request)
        {
            int id = request.RequestId;

            ResultSchema schema = new();
            schema.Columns.Add(new ColumnSchema { Name = "echo", Type = Grpc.ColumnType.String });
            yield return new BatchExecuteResponse { RequestId = id, Schema = schema };

            ResultRow row = new();
            row.Values.Add(new Value { StringValue = request.Request?.Sql ?? "" });
            yield return new BatchExecuteResponse { RequestId = id, Row = row };

            QueryComplete complete = new() { Total = 1, CausalTokenN = 7, CausalTokenL = 5, CausalTokenC = 6 };
            if (request.Request?.Sql == "hinted")
                complete.CacheMetadata = new Grpc.CacheMetadata
                {
                    Status = "hit",
                    Name = "orders_all",
                    CachedAtHlc = new HlcTimestamp { L = 99, C = 1 },
                    AgeMs = 5,
                };

            yield return new BatchExecuteResponse { RequestId = id, QueryComplete = complete };
        }
    }
}
