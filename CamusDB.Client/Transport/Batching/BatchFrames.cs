
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// The contract of <c>BatchExecute</c> stream frames: one stream message that carries several ops, or
/// several response messages. Mirrors the constants the server keeps in <c>CamusDB.Grpc.Contracts</c>;
/// the two must agree, as the two copies of <c>camus_sql.proto</c> must.
/// </summary>
internal static class BatchFrames
{
    /// <summary>
    /// Response header a server writes when a <c>BatchExecute</c> stream opens to say it reads request
    /// frames. Its value is the highest contract version the server reads. A server built before frames
    /// runs an unknown op kind as a NON_QUERY, so its absence must never be probed past.
    /// </summary>
    public const string HeaderName = "camusdb-batch-frames";

    /// <summary>
    /// Request header a client writes when it opens a <c>BatchExecute</c> stream to say it reads response
    /// frames. Its value is the highest contract version the client reads. Without it a server has proof
    /// that the peer reads frames only once a request frame arrived, which a single caller never sends,
    /// so a lone multi-row read would stay one message per row. A server built before frames ignores it.
    /// </summary>
    public const string AcceptHeaderName = "camusdb-batch-frames-accept";

    /// <summary>The contract version this client writes, and the highest it reads.</summary>
    public const int Version = 1;

    /// <summary>Most items one frame may carry. A server refuses the items past it.</summary>
    public const int MaxItems = 256;

    /// <summary>
    /// Most serialized item bytes one frame may carry. Far below the 4 MB default gRPC message limit on
    /// purpose: a message over the transport's limit is rejected before it is parsed, which resets the
    /// stream every other op shares, so the sender must never build one. An op larger than this on its
    /// own travels as a plain single message.
    /// </summary>
    public const int MaxBytes = 1024 * 1024;

    /// <summary>True when a header value announces a contract version this client can write.</summary>
    public static bool Announces(string? headerValue)
        => int.TryParse(headerValue, out int version) && version >= Version;
}
