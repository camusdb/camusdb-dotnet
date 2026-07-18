/**
 * This file is part of CamusDB
 *
 * Offline coverage for the gRPC batcher (CamusDB.Client.Transport.Batching.GrpcBatcher): request/response
 * correlation, per-op response assembly (a QUERY accumulates schema + rows + terminator), the transaction
 * terminals, in-band error surfacing, and correct demultiplexing under concurrency. Driven by a fake
 * IBatchTransport, so no server is needed.
 */

using System.Threading.Channels;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcBatcher
{
    [Fact]
    public async Task NonQueryCompletesWithAffectedRowsAndToken()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        BatchNonQueryResult result = await batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "u" }, slotIndex: null, default);

        Assert.Equal(3, result.AffectedRows);
        Assert.Equal(7, result.Token.N);
        Assert.Equal(5, result.Token.L);
    }

    [Fact]
    public async Task QueryAssemblesSchemaRowsAndTerminator()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        BatchQueryResult result = await batcher.EnqueueQueryAsync(new SqlRequest { Sql = "echo-me" }, slotIndex: null, default);

        Assert.Single(result.Schema.Columns);
        Assert.Equal("echo", result.Schema.Columns[0].Name);
        Assert.Single(result.Rows);
        Assert.Equal("echo-me", result.Rows[0].Values[0].StringValue);
    }

    [Fact]
    public async Task StartReturnsHandle()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        int slot = batcher.ReserveSlot();
        TxnHandle handle = await batcher.EnqueueStartAsync(new SqlRequest(), slot, default);

        Assert.Equal(111, handle.TxnIdPt);
        Assert.Equal(222u, handle.TxnIdCounter);
    }

    [Fact]
    public async Task CommitAndRollbackComplete()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        int slot = batcher.ReserveSlot();
        BatchCausalToken token = await batcher.EnqueueCommitAsync(new SqlRequest(), slot, default);
        Assert.Equal(9, token.L);

        await batcher.EnqueueRollbackAsync(new SqlRequest(), slot, default);   // completes without throwing
    }

    [Fact]
    public async Task InBandErrorSurfacesAsCamusException()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 1 }, id => new FakeTransport(id));

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => batcher.EnqueueNonQueryAsync(new SqlRequest { Sql = "boom" }, slotIndex: null, default));

        Assert.Equal("CADB0502", ex.Code);
    }

    [Fact]
    public async Task ConcurrentQueriesEachGetTheirOwnResponse()
    {
        await using GrpcBatcher batcher = new(new GrpcBatchOptions { ChannelPoolSize = 2 }, id => new FakeTransport(id));

        // 100 concurrent queries; each echoes its own sql back. Correct demux means every task sees exactly
        // its own marker, never another op's — the core request_id correlation guarantee.
        BatchQueryResult[] results = await Task.WhenAll(
            Enumerable.Range(0, 100).Select(i => batcher.EnqueueQueryAsync(new SqlRequest { Sql = $"q{i}" }, slotIndex: null, default)));

        for (int i = 0; i < results.Length; i++)
            Assert.Equal($"q{i}", results[i].Rows[0].Values[0].StringValue);
    }

    /// <summary>An in-process <see cref="IBatchTransport"/> that answers each request deterministically by
    /// kind, echoing the request's SQL back for QUERY so correlation can be asserted under concurrency.</summary>
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

            if (request.Request?.Sql == "boom")
            {
                yield return new BatchExecuteResponse { RequestId = id, Error = new BatchError { Code = "CADB0502", Message = "conflict" } };
                yield break;
            }

            switch (request.Kind)
            {
                case BatchStatementKind.Query:
                    ResultSchema schema = new();
                    schema.Columns.Add(new ColumnSchema { Name = "echo", Type = Grpc.ColumnType.String });
                    yield return new BatchExecuteResponse { RequestId = id, Schema = schema };

                    ResultRow row = new();
                    row.Values.Add(new Value { StringValue = request.Request?.Sql ?? "" });
                    yield return new BatchExecuteResponse { RequestId = id, Row = row };

                    yield return new BatchExecuteResponse
                    {
                        RequestId = id,
                        QueryComplete = new QueryComplete { Total = 1, CausalTokenN = 7, CausalTokenL = 5, CausalTokenC = 6 },
                    };
                    break;

                case BatchStatementKind.NonQuery:
                    yield return new BatchExecuteResponse
                    {
                        RequestId = id,
                        NonQuery = new NonQueryReply { AffectedRows = 3, CausalTokenN = 7, CausalTokenL = 5, CausalTokenC = 6 },
                    };
                    break;

                case BatchStatementKind.Start:
                    yield return new BatchExecuteResponse { RequestId = id, StartReply = new TxnHandle { TxnIdPt = 111, TxnIdCounter = 222 } };
                    break;

                case BatchStatementKind.Commit:
                    yield return new BatchExecuteResponse { RequestId = id, CommitReply = new CommitReply { CausalTokenN = 1, CausalTokenL = 9, CausalTokenC = 1 } };
                    break;

                case BatchStatementKind.Rollback:
                    yield return new BatchExecuteResponse { RequestId = id, RollbackReply = new RollbackReply() };
                    break;
            }
        }
    }
}
