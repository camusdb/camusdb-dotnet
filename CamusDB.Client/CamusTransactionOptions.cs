/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Per-transaction concurrency selections passed to <see cref="CamusConnection.BeginTransactionAsync(CamusTransactionOptions, System.Threading.CancellationToken)"/>.
/// Each knob is independent: a transaction is <see cref="IsolationLevel"/> <em>and</em>
/// <see cref="Mode"/> <em>and</em> <see cref="Locking"/>.
///
/// <para>A <see langword="null"/> knob is left unspecified on the wire, so the server applies its
/// configured default (which the connection string can override via <c>IsolationLevel=</c> /
/// <c>TransactionMode=</c> / <c>Locking=</c>). An explicit value here takes precedence over the
/// connection-string default.</para>
/// </summary>
public sealed record CamusTransactionOptions
{
    public CamusIsolationLevel? IsolationLevel { get; init; }

    public CamusTransactionMode? Mode { get; init; }

    public CamusLocking? Locking { get; init; }

    /// <summary>A pessimistic, read-write transaction at the server's default isolation level.</summary>
    public static CamusTransactionOptions Default { get; } = new();

    /// <summary>An optimistic transaction (lock-free writes, commit-time conflict validation).</summary>
    public static CamusTransactionOptions Optimistic { get; } = new() { Locking = CamusLocking.Optimistic };

    /// <summary>A lock-free, consistent read-only snapshot (Serializable read-only).</summary>
    public static CamusTransactionOptions Snapshot { get; } =
        new() { IsolationLevel = CamusIsolationLevel.Serializable, Mode = CamusTransactionMode.ReadOnly };

    /// <summary>Merges another options set as a fallback: this instance's non-null knobs win.</summary>
    internal CamusTransactionOptions WithDefaults(CamusTransactionOptions? fallback)
    {
        if (fallback is null)
            return this;

        return new CamusTransactionOptions
        {
            IsolationLevel = IsolationLevel ?? fallback.IsolationLevel,
            Mode = Mode ?? fallback.Mode,
            Locking = Locking ?? fallback.Locking,
        };
    }

    /// <summary>The wire string for <see cref="IsolationLevel"/>, or <see langword="null"/> to omit it.</summary>
    internal string? IsolationLevelWire => IsolationLevel switch
    {
        CamusIsolationLevel.ReadCommitted => "ReadCommitted",
        CamusIsolationLevel.Serializable => "Serializable",
        _ => null,
    };

    /// <summary>The wire string for <see cref="Mode"/>, or <see langword="null"/> to omit it.</summary>
    internal string? ModeWire => Mode switch
    {
        CamusTransactionMode.ReadWrite => "ReadWrite",
        CamusTransactionMode.ReadOnly => "ReadOnly",
        _ => null,
    };

    /// <summary>The wire string for <see cref="Locking"/>, or <see langword="null"/> to omit it.</summary>
    internal string? LockingWire => Locking switch
    {
        CamusLocking.Pessimistic => "Pessimistic",
        CamusLocking.Optimistic => "Optimistic",
        _ => null,
    };
}
