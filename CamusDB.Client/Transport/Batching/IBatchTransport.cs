
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc;

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// One long-lived duplex <c>BatchExecute</c> stream, abstracted so the batcher can be driven either by a
/// real gRPC channel or by an in-process fake in tests. A transport is a single ordered write side
/// (<see cref="SendAsync"/>) plus a single response side (<see cref="ReadAllAsync"/>); the batcher owns
/// exactly one reader loop per transport and serializes writes to it. Ported from the server's
/// <c>CamusDB.Grpc.Client</c>.
/// </summary>
internal interface IBatchTransport : IAsyncDisposable
{
    /// <summary>Stable id used to attribute pending ops to this transport for failure/reconnect.</summary>
    long Id { get; }

    /// <summary>Writes one request onto the stream. Called under a per-transport write lock, so
    /// implementations need not guard against concurrent writers (gRPC forbids concurrent stream writes).</summary>
    Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken);

    /// <summary>Yields responses in the order the server produced them. Completes when the stream closes
    /// and throws when it faults, which the batcher treats as a signal to fail this transport's pending
    /// ops and rebuild the slot.</summary>
    IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken);
}
