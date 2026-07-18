
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc;

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// The kind of a batched op, so the demultiplexer knows how many response messages to expect for a
/// <c>request_id</c> (a QUERY streams schema + rows + a terminator; everything else is one terminal).
/// </summary>
internal enum BatchOpKind
{
    Query,
    NonQuery,
    Start,
    Commit,
    Rollback,
}

/// <summary>
/// A Hybrid Logical Clock timestamp threaded through a session/connection for read-your-writes. All three
/// components travel together — dropping <see cref="N"/> makes the token a lossy copy (protocol doc §4.2).
/// </summary>
internal readonly struct BatchCausalToken(int n, long l, long c)
{
    public int N { get; } = n;

    public long L { get; } = l;

    public long C { get; } = c;

    public bool IsEmpty => N == 0 && L == 0 && C == 0;
}

/// <summary>Materialized result of a batched QUERY: the output-column schema, the positional rows, and the
/// trailing causal token. Rows align to <see cref="Schema"/> by position.</summary>
internal sealed class BatchQueryResult(ResultSchema schema, IReadOnlyList<ResultRow> rows, BatchCausalToken token)
{
    public ResultSchema Schema { get; } = schema;

    public IReadOnlyList<ResultRow> Rows { get; } = rows;

    public BatchCausalToken Token { get; } = token;
}

/// <summary>Result of a batched NON_QUERY: affected-row count plus the trailing causal token.</summary>
internal sealed class BatchNonQueryResult(int affectedRows, BatchCausalToken token)
{
    public int AffectedRows { get; } = affectedRows;

    public BatchCausalToken Token { get; } = token;
}

/// <summary>
/// Tunables for the batcher. Defaults mirror the server's <c>CamusDB.Grpc.Client</c> (itself modeled on
/// Kahuna). <see cref="ChannelPoolSize"/> bounds how many long-lived <c>BatchExecute</c> streams exist
/// per endpoint — not how many transactions can be in flight (many transactions hash onto the same
/// streams and interleave), so a small pool is normal. Coalescing trades a tiny latency delay for fewer,
/// larger writes when a burst of ops arrives together.
/// </summary>
internal sealed class GrpcBatchOptions
{
    /// <summary>Number of long-lived <c>BatchExecute</c> streams multiplexed per endpoint.</summary>
    public int ChannelPoolSize { get; init; } = 2;

    /// <summary>When a pump drain produces fewer than this many ops, wait <see cref="CoalescingDelayMs"/>
    /// to let more accumulate before the next drain. A threshold of 1 (or a zero delay) disables it.</summary>
    public int CoalescingThreshold { get; init; } = 10;

    /// <summary>Coalescing delay in milliseconds.</summary>
    public int CoalescingDelayMs { get; init; } = 2;
}
