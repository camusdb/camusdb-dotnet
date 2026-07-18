
using System.Data;
using System.Data.Common;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL transaction to be made in a CamusDB database.
/// A transaction in CamusDB is a set of reads and writes that execute
/// atomically at a single logical point in time across columns, rows, and
/// tables in a database.
/// </summary>
public class CamusTransaction : DbTransaction
{
    private readonly long txnIdPT;

    private readonly uint txnIdCounter;

    private readonly string endpoint;

    private readonly CamusConnection connection;

    protected readonly CamusConnectionStringBuilder builder;

    internal string Endpoint => endpoint;

    public long TxnIdPT => txnIdPT;

    public uint TxnIdCounter => txnIdCounter;

    public string TransactionId => string.Concat(txnIdPT, ":", txnIdCounter);

    /// <summary>
    /// Opaque transport routing hint set by the connection at BEGIN. For the gRPC batching transport it is
    /// the reserved <c>BatchExecute</c> stream slot this transaction's statements and finalize pin to (so
    /// the server orders them on one stream); null under REST, which does not pin.
    /// </summary>
    internal int? StreamSlot { get; set; }

    /// <summary>
    /// The effective concurrency options this transaction was begun with (isolation, mode, locking), after
    /// merging the caller's request with the connection and connection-string defaults. A knob the caller
    /// left unset (deferred to the server default) reads back as <see langword="null"/> here.
    /// </summary>
    public CamusTransactionOptions Options { get; }

    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder)
        : this(txnIdPT, txnIdCounter, endpoint, connection, builder, CamusTransactionOptions.Default) { }

    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder, CamusTransactionOptions options)
    {
        this.txnIdPT = txnIdPT;
        this.txnIdCounter = txnIdCounter;
        this.endpoint = endpoint;
        this.connection = connection;
        this.builder = builder;
        Options = options;
    }

    /// <summary>
    /// The ADO.NET view of this transaction's isolation level. CamusDB's default is Serializable, so an
    /// unspecified isolation reports <see cref="IsolationLevel.Serializable"/>; an explicit Read Committed
    /// reports <see cref="IsolationLevel.ReadCommitted"/>.
    /// </summary>
    public override IsolationLevel IsolationLevel => Options.IsolationLevel switch
    {
        CamusIsolationLevel.ReadCommitted => IsolationLevel.ReadCommitted,
        _ => IsolationLevel.Serializable,
    };

    protected override DbConnection? DbConnection => connection;
    
    /// <summary>
    /// Commits the database transaction synchronously
    /// </summary>
    public override void Commit() => CommitAsync(default).GetAwaiter().GetResult();

    /// <summary>
    /// Rollbacks the database transaction synchronously
    /// </summary>
    public override void Rollback() => RollbackAsync(default).GetAwaiter().GetResult();    

    /// <summary>
    /// Bound on how many times a <c>COMMIT</c>/<c>ROLLBACK</c> that comes back as CADB0509
    /// (<c>TransactionFinalizeUnresolved</c>) is re-issued on the same handle before the error is
    /// surfaced. The Kahuna session timeout is the ultimate server-side backstop.
    /// </summary>
    private const int MaxFinalizeAttempts = 10;

    /// <summary>
    /// Commits the database transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token used for this task.</param>
    public override Task CommitAsync(CancellationToken cancellationToken = default)
        => FinalizeAsync(commit: true, cancellationToken);

    /// <summary>
    /// Rolls back the database transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token used for this task.</param>
    public override Task RollbackAsync(CancellationToken cancellationToken = default)
        => FinalizeAsync(commit: false, cancellationToken);

    /// <summary>
    /// Issues a finalize (commit / rollback) and resolves the non-terminal CADB0509 outcome by re-issuing
    /// the <b>same</b> finalize on the <b>same</b> transaction handle, bounded and backing off. CADB0509
    /// means the commit/rollback outcome is not yet known — the transaction is not dead — so the operation
    /// must never be replayed from <c>BEGIN</c> (that could double-apply an already-durable commit). All
    /// other errors propagate. Transport-agnostic: the same loop covers REST and gRPC.
    /// </summary>
    private async Task FinalizeAsync(bool commit, CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await builder.GetTransport()
                    .FinalizeTransactionAsync(commit, endpoint, builder.Config["Database"], txnIdPT, txnIdCounter, StreamSlot, builder.CommandTimeout, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (CamusException ex)
                when (ex.Code == SerializableRetryHelper.FinalizeUnresolvedCode && attempt < MaxFinalizeAttempts)
            {
                await Task.Delay(ComputeFinalizeDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // Finalize back-off: 50 ms × 2^min(attempt, 6) (caps at ~3.2 s), matching the server's finalize
    // retry contract for an unresolved commit/rollback outcome.
    private static TimeSpan ComputeFinalizeDelay(int attempt)
        => TimeSpan.FromMilliseconds(50d * (1 << Math.Min(attempt, 6)));
}
