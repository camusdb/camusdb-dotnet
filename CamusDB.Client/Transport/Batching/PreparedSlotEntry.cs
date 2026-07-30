
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// A statement's registration on one stream slot: the id the server minted, the parameter names it
/// published, and — critically — the id of the transport the PREPARE was actually written to.
///
/// <para>A server-side handle is only meaningful on the stream that minted it, so the transport id is
/// what makes an entry checkable. When a slot's stream faults and is rebuilt, its new transport gets a
/// new id, every cached entry for that slot stops matching, and the statement is transparently prepared
/// again. No invalidation callback is needed: staleness is a comparison, not an event.</para>
/// </summary>
internal readonly record struct PreparedSlotEntry(long TransportId, int StatementId, string[] ParameterNames);

/// <summary>
/// Raised when an execution is about to be written to a transport that is not the one its prepared
/// statement was registered on.
///
/// <para>The check happens immediately before the write, which is the only place it can be conclusive:
/// anywhere earlier and the transport could still be rebuilt in between. Without it the op would travel
/// to a stream that never saw the PREPARE and come back as an unknown-statement error — the same
/// outcome, but only after a needless round trip. Callers treat this as "re-prepare and retry once",
/// never as a user-facing failure.</para>
/// </summary>
internal sealed class PreparedStatementStaleException()
    : Exception("The prepared statement's stream was rebuilt before this execution could be written");
