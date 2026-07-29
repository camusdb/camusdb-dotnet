
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc;
using Grpc.Core;

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// Real <see cref="IBatchTransport"/> over a gRPC <c>BatchExecute</c> duplex call. One instance owns one
/// long-lived stream; the batcher keeps several (the pool) and multiplexes ops across them. Ported from
/// the server's <c>CamusDB.Grpc.Client</c>.
///
/// <para>Any authentication metadata is attached when the stream opens, because the server resolves the
/// principal once for the whole <c>BatchExecute</c> call: every op multiplexed onto this stream runs as
/// that identity. A token refresh therefore reaches the server on the next stream the batcher builds, not
/// mid-stream.</para>
/// </summary>
internal sealed class GrpcBatchTransport : IBatchTransport
{
    private readonly AsyncDuplexStreamingCall<BatchExecuteRequest, BatchExecuteResponse> call;

    public long Id { get; }

    public GrpcBatchTransport(long id, CamusSql.CamusSqlClient client, global::Grpc.Core.Metadata? headers = null)
    {
        Id = id;
        call = headers is null ? client.BatchExecute() : client.BatchExecute(headers);
    }

    public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        => call.RequestStream.WriteAsync(request, cancellationToken);

    public IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken)
        => call.ResponseStream.ReadAllAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        try { await call.RequestStream.CompleteAsync().ConfigureAwait(false); }
        catch { /* stream already broken */ }
        call.Dispose();
    }
}
