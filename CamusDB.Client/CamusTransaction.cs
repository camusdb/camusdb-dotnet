
using System.Data;
using System.Data.Common;
using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL transaction to be made in a CamusDB database.
/// A transaction in CamusDB is a set of reads and writes that execute
/// atomically at a single logical point in time across columns, rows, and
/// tables in a database.
///
/// <para><b>Where a transaction starts.</b> With learned routing off, <c>BEGIN</c> is sent inside
/// <c>BeginTransaction</c>, on the pool's next endpoint, exactly as it always was. With learned routing
/// on, <c>BEGIN</c> is <em>deferred</em>: the transaction is started by its first statement, on the
/// endpoint that statement's learned route names, so a transaction whose first statement touches table
/// <c>T</c> runs where <c>T</c>'s leader is. Once started the transaction is pinned to that endpoint for
/// its whole life; a statement's own route may inform a later transaction, never relocate this one.
/// A caller that knows the transaction's hot statement can pick the endpoint at <c>BEGIN</c> with
/// <see cref="CamusTransactionOptions.Affinity"/> instead.</para>
///
/// <para>Two consequences of the deferral are visible to callers, in routed mode only: the server-minted
/// identity (<see cref="TxnIdPT"/>, <see cref="TxnIdCounter"/>, <see cref="TransactionId"/>) reads as
/// zero until the first statement, and a failure to begin surfaces from that first statement rather than
/// from <c>BeginTransaction</c>, with the same exception it would have carried there.</para>
/// </summary>
public class CamusTransaction : DbTransaction
{
    private long txnIdPT;

    private uint txnIdCounter;

    private string? endpoint;

    private readonly CamusConnection connection;

    protected readonly CamusConnectionStringBuilder builder;

    /// <summary>Guards <see cref="startTask"/> so a deferred <c>BEGIN</c> is sent exactly once even
    /// when several commands race to be the transaction's first statement.</summary>
    private readonly object startSync = new();

    /// <summary>
    /// The single in-flight or completed <c>BEGIN</c> of a deferred transaction, memoized under
    /// <see cref="startSync"/>; null for a transaction begun eagerly. Every racing first command awaits
    /// this same task, and a <c>BEGIN</c> that failed stays failed: nothing was minted, the transaction
    /// is unusable, and the caller begins a new one — the state a failed eager <c>BEGIN</c> leaves too.
    /// </summary>
    private Task? startTask;

    /// <summary>True once the server minted this transaction and its endpoint is pinned.</summary>
    private volatile bool started;

    /// <summary>True once a pipeline committed this transaction, so that a later finalize does not
    /// ask the server about a transaction it no longer knows.</summary>
    private volatile bool committed;

    /// <summary>
    /// The endpoint every statement and the finalize go to, or null while a deferred transaction is
    /// not started yet. A command must resolve it through <see cref="EnsureStartedAsync"/>, which
    /// starts the transaction when needed, rather than read it directly.
    /// </summary>
    internal string? Endpoint => endpoint;

    /// <summary>
    /// True once the server minted this transaction. Always true for a transaction begun with learned
    /// routing off. With routing on, false until the first statement (or an explicit
    /// <see cref="CamusTransactionOptions.Affinity"/>) starts it — see the class summary.
    /// </summary>
    public bool IsStarted => started;

    /// <summary>
    /// The server-minted transaction identity. Zero until the transaction is started; with learned
    /// routing on, that is after the first statement, not after <c>BeginTransaction</c> — see
    /// <see cref="IsStarted"/>.
    /// </summary>
    public long TxnIdPT => txnIdPT;

    /// <inheritdoc cref="TxnIdPT"/>
    public uint TxnIdCounter => txnIdCounter;

    /// <inheritdoc cref="TxnIdPT"/>
    public string TransactionId => string.Concat(txnIdPT, ":", txnIdCounter);

    /// <summary>
    /// Opaque transport routing hint set when the transaction starts. For the gRPC batching transport it
    /// is the reserved <c>BatchExecute</c> stream slot this transaction's statements and finalize pin to
    /// (so the server orders them on one stream); null under REST, which does not pin.
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

    /// <summary>A transaction the server already minted: its identity and endpoint are final.</summary>
    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder, CamusTransactionOptions options)
    {
        this.txnIdPT = txnIdPT;
        this.txnIdCounter = txnIdCounter;
        this.endpoint = endpoint;
        this.connection = connection;
        this.builder = builder;
        Options = options;
        started = true;
    }

    /// <summary>
    /// A deferred transaction: nothing has been sent yet. The first statement (or the finalize, for a
    /// transaction that ran none) starts it through <see cref="EnsureStartedAsync"/>.
    /// </summary>
    internal CamusTransaction(CamusConnection connection, CamusConnectionStringBuilder builder, CamusTransactionOptions options)
    {
        this.connection = connection;
        this.builder = builder;
        Options = options;
    }

    /// <summary>
    /// Starts the transaction if it is not started yet, on <paramref name="preferredEndpoint"/> when
    /// given (a learned route for the statement about to run) and on the pool's rotation otherwise,
    /// then returns the endpoint the transaction is pinned to. The first caller sends <c>BEGIN</c>;
    /// concurrent callers await that same send. Once started, the preferred endpoint of every later
    /// call is ignored: the pin is final.
    /// </summary>
    internal async Task<string> EnsureStartedAsync(string? preferredEndpoint, CancellationToken cancellationToken)
    {
        if (!started)
        {
            Task pending;
            lock (startSync)
                pending = startTask ??= StartOnAsync(preferredEndpoint ?? builder.GetEndpoint(), cancellationToken);

            await pending.ConfigureAwait(false);
        }

        return endpoint!;
    }

    /// <summary>
    /// Sends <c>BEGIN</c> to <paramref name="target"/> and seats the minted identity, the endpoint pin
    /// and the transport's stream slot. Runs at most once per transaction (see <see cref="startTask"/>).
    /// </summary>
    private async Task StartOnAsync(string target, CancellationToken cancellationToken)
    {
        StartTransactionResult result = await builder.GetTransport()
            .StartTransactionAsync(target, builder.Config["Database"], Options, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);

        Seat(result, target);
    }

    private void Seat(in StartTransactionResult result, string target)
    {
        txnIdPT = result.TxnIdPT;
        txnIdCounter = result.TxnIdCounter;
        StreamSlot = result.StreamSlot;
        endpoint = target;
        started = true;
    }

    /// <summary>
    /// Several statements of this transaction sent as one exchange and answered together — see
    /// <see cref="CamusPipeline"/>. A transaction begun with <see cref="CamusTransactionOptions.DeferBegin"/>
    /// (or with learned routing on) puts its <c>BEGIN</c> inside the first pipeline too.
    /// </summary>
    public CamusPipeline CreatePipeline() => new(this, connection, builder);

    /// <summary>
    /// Lets a pipeline begin this transaction inside its first exchange. Returns true, with the
    /// completion the pipeline must settle once the server has answered the <c>BEGIN</c>, when nothing
    /// has begun it yet; false otherwise, with the <c>BEGIN</c> already under way (or null when it is
    /// done) for the pipeline to await instead. The memoized <see cref="startTask"/> is what makes a
    /// pipeline and a command that race to be the first statement agree on one <c>BEGIN</c>.
    /// </summary>
    internal bool TryClaimStart(out TaskCompletionSource? claim, out Task? pending)
    {
        lock (startSync)
        {
            if (started || startTask is not null)
            {
                claim = null;
                pending = started ? null : startTask;
                return false;
            }

            claim = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            startTask = claim.Task;
            pending = null;
            return true;
        }
    }

    /// <summary>Seats the identity a pipeline's <c>BEGIN</c> minted; the pipeline settles its claim after this.</summary>
    internal void SeatStart(in StartTransactionResult result, string target) => Seat(result, target);

    /// <summary>Records that a pipeline's conditional commit committed this transaction.</summary>
    internal void MarkCommitted() => committed = true;

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
    ///
    /// <para>A deferred transaction that ran no statement is started here first, on the pool's rotation,
    /// so the server sees the same <c>BEGIN</c>-then-finalize pair it always did.</para>
    /// </summary>
    private async Task FinalizeAsync(bool commit, CancellationToken cancellationToken)
    {
        // A deferred BEGIN that failed left nothing on the server. The failure already surfaced at the
        // statement that triggered it; a rollback — which a disposing scope issues without looking —
        // has nothing to undo and completes quietly, while a commit still reports the failed start.
        if (!commit && !started && startTask is { IsFaulted: true } or { IsCanceled: true })
            return;

        // A pipeline already committed this transaction. A rollback has nothing to undo; a second
        // commit is a caller mistake, and the server would only answer that the transaction is unknown.
        if (committed)
        {
            if (commit)
                throw new InvalidOperationException("The transaction was already committed by a pipeline.");
            return;
        }

        string target = await EnsureStartedAsync(preferredEndpoint: null, cancellationToken).ConfigureAwait(false);

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await builder.GetTransport()
                    .FinalizeTransactionAsync(commit, target, builder.Config["Database"], txnIdPT, txnIdCounter, StreamSlot, builder.CommandTimeout, cancellationToken)
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
