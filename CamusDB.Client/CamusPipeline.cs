/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.ExceptionServices;
using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// Several statements of one <see cref="CamusTransaction"/>, sent together as one exchange and answered
/// together. A transaction that awaits each statement pays one round trip per statement — six for a
/// bank transfer (<c>BEGIN</c>, two reads, two writes, <c>COMMIT</c>) — and at high concurrency those
/// round trips, not the statements, dominate its latency. A pipeline queues the statements whose inputs
/// are known, sends them as one stream message, and completes each one's task from its own answer.
///
/// <para><b>Shape.</b> Queue with <see cref="QueueReader"/> / <see cref="QueueNonQuery"/>, which return
/// the task for that statement's result; then <see cref="SendAsync"/> to run what is queued, or
/// <see cref="CommitAsync"/> to run it and commit the transaction in the same exchange — a commit that
/// happens only when every statement of the transaction succeeded. A transaction begun with
/// <see cref="CamusTransactionOptions.DeferBegin"/> (or with learned routing on) puts its <c>BEGIN</c>
/// inside the first exchange too. A transfer that needs the values it reads is two exchanges:
/// <c>[BEGIN, SELECT a, SELECT b]</c> then <c>[UPDATE a, UPDATE b, COMMIT]</c>; one whose writes
/// depend on nothing read is one.</para>
///
/// <para><b>Failure.</b> Every queued task completes with its own result or its own
/// <see cref="CamusException"/>, as a lone execution would. The statements after a failed one in the
/// same exchange still get an answer — the transaction has ended, so they fail too — and the send
/// itself throws the first failure, so a caller that awaits only the send still learns of it. After a
/// failed <see cref="CommitAsync"/> the transaction is rolled back on the server; calling
/// <see cref="CamusTransaction.RollbackAsync"/> is then a harmless no-op, as after any failed
/// statement. Replay the whole transaction, from <c>BEGIN</c>, to retry.</para>
///
/// <para><b>Ordering and reuse.</b> The queued statements run in queue order, after every statement of
/// the transaction sent before them, so a pipeline can be sent more than once on one transaction and
/// an ordinary command can run between sends. One send at a time per pipeline. The exchange count is
/// reported in <see cref="Exchanges"/>, which is the number the pipeline exists to lower: one per send
/// over gRPC to a server that reads the pipeline contract, and one per statement — the ordinary cost —
/// over REST or an older server, with the same results either way.</para>
/// </summary>
public sealed class CamusPipeline
{
    private readonly CamusTransaction transaction;
    private readonly CamusConnection connection;
    private readonly CamusConnectionStringBuilder builder;
    private readonly List<Queued> queued = new();
    private int exchanges;
    private bool sending;

    internal CamusPipeline(CamusTransaction transaction, CamusConnection connection, CamusConnectionStringBuilder builder)
    {
        this.transaction = transaction;
        this.connection = connection;
        this.builder = builder;
    }

    /// <summary>The transaction every queued statement runs in.</summary>
    public CamusTransaction Transaction => transaction;

    /// <summary>Statements queued and not yet sent.</summary>
    public int Count => queued.Count;

    /// <summary>
    /// Round trips this pipeline has cost so far: one per send when the whole send travelled as one
    /// stream message, else one per step (<c>BEGIN</c>, each statement, the commit) of the sends that
    /// did not.
    /// </summary>
    public int Exchanges => exchanges;

    /// <summary>
    /// Queues a statement whose result is a reader — a <c>SELECT</c>, or a DML statement with a
    /// RETURNING list — and returns the task for that reader. The task completes when the pipeline is
    /// sent and the statement answered. The command must belong to this pipeline's transaction, or to
    /// none; it is bound to the transaction here.
    /// </summary>
    public Task<CamusDataReader> QueueReader(CamusCommand command)
    {
        Queued item = Add(command, asReader: true);
        return item.Reader!.Task;
    }

    /// <summary>
    /// Queues a statement whose result is its affected-row count and returns the task for that count.
    /// The task completes when the pipeline is sent and the statement answered.
    /// </summary>
    public Task<int> QueueNonQuery(CamusCommand command)
    {
        Queued item = Add(command, asReader: false);
        return item.Count!.Task;
    }

    private Queued Add(CamusCommand command, bool asReader)
    {
        ArgumentNullException.ThrowIfNull(command);

        if (command is CamusInsertCommand or CamusPingCommand)
            throw new NotSupportedException("Only a SQL text command can be pipelined.");
        if (command.Transaction is { } other && !ReferenceEquals(other, transaction))
            throw new InvalidOperationException("The command belongs to another transaction.");
        if (sending)
            throw new InvalidOperationException("A send is in progress on this pipeline; queue after it completes.");

        command.Transaction = transaction;
        Queued item = new(command, asReader);
        queued.Add(item);
        return item;
    }

    /// <summary>Sends the queued statements as one exchange and waits for every answer. Throws the first
    /// failure, if any; each statement's own task carries its own outcome.</summary>
    public Task SendAsync(CancellationToken cancellationToken = default) => FlushAsync(commit: false, cancellationToken);

    /// <summary>
    /// Sends the queued statements and the transaction's commit as one exchange. The commit happens only
    /// when every statement of the transaction — in this send and before it — succeeded; otherwise the
    /// server rolls the transaction back and this throws the first failure. On success the transaction
    /// is committed, and <see cref="CamusTransaction.CommitAsync"/> must not be called again.
    /// </summary>
    public Task CommitAsync(CancellationToken cancellationToken = default) => FlushAsync(commit: true, cancellationToken);

    private async Task FlushAsync(bool commit, CancellationToken cancellationToken)
    {
        if (sending)
            throw new InvalidOperationException("A send is already in progress on this pipeline.");
        if (queued.Count == 0 && !commit)
            return;

        Queued[] batch = queued.ToArray();
        queued.Clear();
        sending = true;

        TaskCompletionSource? claim = null;
        try
        {
            // The BEGIN rides this exchange when nothing has begun the transaction yet. When a command
            // is begining it at this moment, wait for that instead: one BEGIN per transaction.
            if (!transaction.IsStarted && !transaction.TryClaimStart(out claim, out Task? pending) && pending is not null)
                await pending.ConfigureAwait(false);

            string endpoint = transaction.Endpoint ?? ChooseEndpoint(batch);
            string database = builder.Config["Database"];

            TransportPipelineStatement[] statements = new TransportPipelineStatement[batch.Length];
            for (int i = 0; i < batch.Length; i++)
                statements[i] = await batch[i].Command.BuildPipelineStatementAsync(batch[i].AsReader, endpoint, cancellationToken).ConfigureAwait(false);

            TransportPipelineRequest request = new()
            {
                Endpoint = endpoint,
                Database = database,
                Start = claim is not null ? transaction.Options : null,
                TxnIdPT = claim is null ? transaction.TxnIdPT : null,
                TxnIdCounter = claim is null ? transaction.TxnIdCounter : null,
                StreamSlot = claim is null ? transaction.StreamSlot : null,
                Statements = statements,
                Commit = commit,
                TimeoutSeconds = builder.CommandTimeout,
            };

            PipelineTransportResult result = await builder.GetTransport().ExecutePipelineAsync(request, cancellationToken).ConfigureAwait(false);
            exchanges += result.Exchanges;

            if (claim is not null)
            {
                if (result.Started is { } started)
                {
                    transaction.SeatStart(started, endpoint);
                    claim.TrySetResult();
                }
                else
                {
                    Exception failure = result.StartFailure ?? new CamusException("CADB0000", "The transaction did not begin");
                    claim.TrySetException(failure);
                    Observe(claim.Task);
                }
                claim = null;
            }

            for (int i = 0; i < batch.Length; i++)
                batch[i].Settle(result.Statements[i]);

            if (commit && result.Committed)
                transaction.MarkCommitted();

            if (result.FirstFailure is { } first)
                ExceptionDispatchInfo.Capture(first).Throw();
        }
        catch (Exception ex)
        {
            // A failure before or of the exchange itself: nothing has been settled, so settle it all
            // with the one failure, the claim included, so no caller waits forever.
            if (claim is not null)
            {
                claim.TrySetException(ex);
                Observe(claim.Task);
            }
            foreach (Queued item in batch)
                item.Fail(ex);
            throw;
        }
        finally
        {
            sending = false;
        }
    }

    /// <summary>
    /// Where a transaction that is not begun yet begins: the first statement's learned route when the
    /// connection routes, the pool's rotation otherwise — the same choice a lone first statement makes.
    /// </summary>
    private string ChooseEndpoint(Queued[] batch)
    {
        if (builder.Router is { } router && batch.Length > 0)
        {
            CamusRouteOpKind kind = batch[0].AsReader ? CamusRouteOpKind.Query : CamusRouteOpKind.NonQuery;
            if (router.SelectEndpoint(builder.Config["Database"], batch[0].Command.CommandText, kind, out _) is { } learned)
                return learned;
        }

        return builder.GetEndpoint();
    }

    private static void Observe(Task task) => _ = task.Exception;

    /// <summary>One queued statement and the completion its caller holds.</summary>
    private sealed class Queued
    {
        public readonly CamusCommand Command;
        public readonly bool AsReader;
        public readonly TaskCompletionSource<CamusDataReader>? Reader;
        public readonly TaskCompletionSource<int>? Count;

        public Queued(CamusCommand command, bool asReader)
        {
            Command = command;
            AsReader = asReader;
            if (asReader)
                Reader = new TaskCompletionSource<CamusDataReader>(TaskCreationOptions.RunContinuationsAsynchronously);
            else
                Count = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void Settle(PipelineStatementOutcome outcome)
        {
            if (outcome.Failure is { } failure)
            {
                Fail(failure);
                return;
            }

            if (Reader is not null)
            {
                if (outcome.Query is { } query)
                {
                    Command.LastCacheMetadata = query.CacheMetadata;
                    Reader.TrySetResult(new CamusDataReader(query.ResultSet, query.CacheMetadata));
                }
                else if (outcome.NonQuery is { } written)
                {
                    Reader.TrySetResult(written.Returning is { } returning
                        ? new CamusDataReader(CamusRowSource.Buffered(returning, written.AffectedRows))
                        : new CamusDataReader(written.AffectedRows));
                }
                else
                {
                    Fail(new CamusException("CADB0000", "The statement was not answered"));
                }
                return;
            }

            if (outcome.NonQuery is { } result)
                Count!.TrySetResult(result.AffectedRows);
            else if (outcome.Query is { } rows)
                Count!.TrySetResult(rows.ResultSet.RowCount);
            else
                Fail(new CamusException("CADB0000", "The statement was not answered"));
        }

        /// <summary>Faults the caller's task and marks the fault observed: a caller that awaits only
        /// the send, which throws the same failure, must not be charged with an unobserved one.</summary>
        public void Fail(Exception failure)
        {
            if (Reader is not null)
            {
                Reader.TrySetException(failure);
                _ = Reader.Task.Exception;
            }
            else
            {
                Count!.TrySetException(failure);
                _ = Count.Task.Exception;
            }
        }
    }
}
