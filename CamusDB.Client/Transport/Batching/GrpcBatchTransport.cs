
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
/// <para>Any authentication metadata is attached when the stream opens — gRPC metadata belongs to the
/// call, so this is the only token the stream ever presents, and every op multiplexed onto it runs as
/// that identity. A token refresh therefore reaches the server on the next stream the batcher builds, not
/// mid-stream, which is why the batcher builds one when the token is renewed instead of waiting for the
/// stream to fail.</para>
/// </summary>
internal sealed class GrpcBatchTransport : IBatchTransport
{
    private readonly AsyncDuplexStreamingCall<BatchExecuteRequest, BatchExecuteResponse> call;

    private volatile bool framesAnnounced;

    public long Id { get; }

    public bool FramesAnnounced => framesAnnounced;

    /// <param name="acceptResponseFrames">
    /// Tells the server, in this stream's request metadata, that response frames may be sent on it. Off
    /// when the caller opted out of frames, so that stream is a true no-frames stream in both directions.
    /// </param>
    public GrpcBatchTransport(
        long id, CamusSql.CamusSqlClient client, global::Grpc.Core.Metadata? headers = null, bool acceptResponseFrames = false)
    {
        Id = id;
        call = client.BatchExecute(WithAcceptHeader(headers, acceptResponseFrames));
        _ = ObserveAnnouncementAsync();
    }

    /// <summary>Builds the call metadata without mutating the caller's, which the factory may reuse.</summary>
    private static global::Grpc.Core.Metadata? WithAcceptHeader(global::Grpc.Core.Metadata? headers, bool acceptResponseFrames)
    {
        if (!acceptResponseFrames)
            return headers;

        global::Grpc.Core.Metadata merged = [];

        if (headers is not null)
            foreach (global::Grpc.Core.Metadata.Entry entry in headers)
                merged.Add(entry);

        merged.Add(BatchFrames.AcceptHeaderName, BatchFrames.Version.ToString());
        return merged;
    }

    /// <summary>
    /// Watches this stream's response headers for the frame announcement, off the operation path: ops are
    /// written one per message until it arrives, and for good if it never does. A stream that fails before
    /// its headers arrive announces nothing; the reader reports that failure, not this.
    /// </summary>
    private async Task ObserveAnnouncementAsync()
    {
        try
        {
            global::Grpc.Core.Metadata responseHeaders = await call.ResponseHeadersAsync.ConfigureAwait(false);

            if (BatchFrames.Announces(responseHeaders.GetValue(BatchFrames.HeaderName)))
                framesAnnounced = true;
        }
        catch
        {
            // No headers, no announcement.
        }
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
