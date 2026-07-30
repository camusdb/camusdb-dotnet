
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Grpc;

namespace CamusDB.Client.Transport.Batching;

/// <summary>
/// Multiplexes many concurrent operations — from many concurrent transactions — over a small pool of
/// long-lived <c>BatchExecute</c> duplex streams, so the network stays busy without a stream (or a unary
/// round-trip) per op. Ported from the server's <c>CamusDB.Grpc.Client</c>, itself modeled on Kahuna.
///
/// <para><b>How it stays busy, else queues.</b> Every op is registered by a monotonic <c>request_id</c>,
/// dropped on its slot's inbox queue, and drained by that slot's single-flight pump onto its stream —
/// one pump per stream, so the streams write concurrently and a slow write on one never stalls the
/// others (and the single writer per stream needs no write lock). A background reader per stream
/// demultiplexes responses back to the waiting op by id. Responses interleave and arrive out of order
/// across ops.</para>
///
/// <para><b>Two routing regimes.</b> Autocommit ops (no transaction) round-robin across the pool for
/// maximum concurrency; a transaction pins <i>all</i> of its ops — START, statements, COMMIT/ROLLBACK — to
/// one stream (the caller reserves a slot via <see cref="ReserveSlot"/> and passes it on every call) so
/// the server's per-stream ordering chain sees them together. The pool bounds the number of streams, not
/// the number of in-flight transactions.</para>
/// </summary>
internal sealed class GrpcBatcher : IAsyncDisposable
{
    private readonly GrpcBatchOptions options;
    private readonly Slot[] slots;
    private readonly CancellationTokenSource shutdown = new();

    private readonly ConcurrentDictionary<int, PendingOp> pending = new();

    private static int requestIdSeq;
    private int roundRobin = -1;
    private long transportIdSeq;

    /// <summary>
    /// Builds a batcher over <paramref name="options"/>.<see cref="GrpcBatchOptions.ChannelPoolSize"/>
    /// transports produced by <paramref name="transportFactory"/> (the argument is a fresh transport id).
    /// The factory is called again to rebuild a slot after its stream faults.
    /// </summary>
    public GrpcBatcher(GrpcBatchOptions options, Func<long, IBatchTransport> transportFactory)
    {
        this.options = options;
        int poolSize = Math.Max(1, options.ChannelPoolSize);
        slots = new Slot[poolSize];
        for (int i = 0; i < poolSize; i++)
        {
            slots[i] = new Slot(i)
            {
                // Connect the first transport synchronously so a slot is never written to before it exists;
                // the reader loop then owns reads and rebuilds the slot after a fault.
                Transport = transportFactory(Interlocked.Increment(ref transportIdSeq)),
            };
            StartReaderLoop(slots[i], transportFactory);
        }
    }

    /// <summary>Reserves a stream slot for a transaction so all of its ops pin to one stream.</summary>
    public int ReserveSlot() => NextRoundRobin();

    private int NextRoundRobin()
        => (int)((uint)Interlocked.Increment(ref roundRobin) % (uint)slots.Length);

    // ─── Public enqueue surface ───────────────────────────────────────────────

    public Task<BatchQueryResult> EnqueueQueryAsync(
        SqlRequest request, int? slotIndex, CancellationToken ct, long? expectedTransportId = null)
        => EnqueueAsync<BatchQueryResult>(BatchStatementKind.Query, request, slotIndex, ct, expectedTransportId);

    public Task<BatchNonQueryResult> EnqueueNonQueryAsync(
        SqlRequest request, int? slotIndex, CancellationToken ct, long? expectedTransportId = null)
        => EnqueueAsync<BatchNonQueryResult>(BatchStatementKind.NonQuery, request, slotIndex, ct, expectedTransportId);

    public Task<TxnHandle> EnqueueStartAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<TxnHandle>(BatchStatementKind.Start, request, slotIndex, ct);

    public Task<BatchCausalToken> EnqueueCommitAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<BatchCausalToken>(BatchStatementKind.Commit, request, slotIndex, ct);

    public async Task EnqueueRollbackAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => await EnqueueAsync<object?>(BatchStatementKind.Rollback, request, slotIndex, ct).ConfigureAwait(false);

    private async Task<T> EnqueueAsync<T>(
        BatchStatementKind kind, SqlRequest request, int? slotIndex, CancellationToken ct,
        long? expectedTransportId = null)
        => (await EnqueueTrackedAsync<T>(kind, request, slotIndex, ct, expectedTransportId).ConfigureAwait(false)).Result;

    /// <summary>
    /// Enqueues an op and also reports the transport it was written to. PREPARE needs that: the id it
    /// mints is only valid on the stream that carried the PREPARE, so caching the id we <em>hoped</em> to
    /// write to — rather than the one we did — would make every entry a guess.
    /// </summary>
    private async Task<(T Result, long TransportId)> EnqueueTrackedAsync<T>(
        BatchStatementKind kind, SqlRequest request, int? slotIndex, CancellationToken ct,
        long? expectedTransportId = null)
    {
        int slot = slotIndex ?? NextRoundRobin();
        int id = Interlocked.Increment(ref requestIdSeq);

        PendingOp op = new(id);

        if (ct.CanBeCanceled)
            op.Registration = ct.Register(static state =>
            {
                PendingOp o = (PendingOp)state!;
                o.Owner!.Fault(o, new OperationCanceledException());
            }, op);
        op.Owner = this;

        pending[id] = op;

        BatchExecuteRequest wire = new() { RequestId = id, Kind = kind, Request = request };
        Slot target = slots[slot];
        target.Inbox.Enqueue(new QueuedItem(wire, op, expectedTransportId));
        TryStartPump(target);

        object? result = await op.Promise.Task.ConfigureAwait(false);
        return ((T)result!, op.TransportId);
    }

    // ─── Prepared statements ──────────────────────────────────────────────────

    /// <summary>
    /// The cache key for a statement on a slot. Database and SQL together, separated by a character
    /// neither can contain, so two different statements can never collide into one entry.
    /// </summary>
    private static string StatementKey(string database, string sql) => database + "\n" + sql;

    /// <summary>
    /// Returns this slot's registration for the statement, preparing it first if the slot has none for
    /// its <b>current</b> transport.
    ///
    /// <para>This lives on the batcher rather than on the caller because only the batcher can read a
    /// slot's current transport id and write the follow-up op through the same path; a cache kept
    /// anywhere else would be comparing against an id it cannot keep in step.</para>
    ///
    /// <para>Concurrent callers racing to prepare the same statement share one in-flight registration
    /// (the dictionary holds the task, not the result), so the server is not asked to register the same
    /// SQL twice — harmless if it happened, but it would waste a handle from the stream's cap.</para>
    /// </summary>
    public async Task<PreparedSlotEntry> EnsurePreparedAsync(
        int slotIndex, string database, string sql, CancellationToken ct)
    {
        Slot slot = slots[slotIndex];
        string key = StatementKey(database, sql);

        while (true)
        {
            if (slot.Prepared.TryGetValue(key, out Task<PreparedSlotEntry>? existing))
            {
                PreparedSlotEntry entry;
                try
                {
                    entry = await existing.ConfigureAwait(false);
                }
                catch
                {
                    // Whoever created it already reported the failure to its own caller; drop the
                    // poisoned entry and take a fresh turn rather than failing every later execution.
                    Forget(slot, key, existing);
                    continue;
                }

                if (entry.TransportId == slot.Transport?.Id)
                    return entry;

                // The slot's stream was rebuilt since this was registered — the handle died with it.
                Forget(slot, key, existing);
                continue;
            }

            TaskCompletionSource<PreparedSlotEntry> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!slot.Prepared.TryAdd(key, promise.Task))
                continue;   // lost the race; the winner's registration is the one to use.

            try
            {
                (PrepareReply reply, long transportId) = await EnqueueTrackedAsync<PrepareReply>(
                    BatchStatementKind.Prepare,
                    new SqlRequest { Database = database, Sql = sql }, slotIndex, ct).ConfigureAwait(false);

                PreparedSlotEntry entry = new(transportId, reply.StatementId, [.. reply.ParameterNames]);
                promise.SetResult(entry);
                return entry;
            }
            catch (Exception ex)
            {
                Forget(slot, key, promise.Task);
                promise.TrySetException(ex);
                _ = promise.Task.Exception;   // observed here; the rethrow below is what callers see.
                throw;
            }
        }
    }

    /// <summary>
    /// Forgets a slot's registration for a statement, but only if it is still the one the caller was
    /// using — so a concurrent re-prepare that already succeeded is not thrown away by a straggler
    /// reacting to the old entry.
    /// </summary>
    public void InvalidatePrepared(int slotIndex, string database, string sql, PreparedSlotEntry stale)
    {
        Slot slot = slots[slotIndex];
        string key = StatementKey(database, sql);

        if (slot.Prepared.TryGetValue(key, out Task<PreparedSlotEntry>? existing)
            && existing.IsCompletedSuccessfully
            && existing.Result.StatementId == stale.StatementId
            && existing.Result.TransportId == stale.TransportId)
        {
            Forget(slot, key, existing);
        }
    }

    /// <summary>Removes and returns every slot registration for a statement, for a caller that is
    /// releasing it.</summary>
    public IReadOnlyList<(int SlotIndex, PreparedSlotEntry Entry)> TakePrepared(string database, string sql)
    {
        string key = StatementKey(database, sql);
        List<(int, PreparedSlotEntry)> taken = [];

        foreach (Slot slot in slots)
        {
            if (slot.Prepared.TryRemove(key, out Task<PreparedSlotEntry>? entry) && entry.IsCompletedSuccessfully)
                taken.Add((slot.Index, entry.Result));
        }

        return taken;
    }

    /// <summary>
    /// Releases a prepared statement on one slot. Best-effort by contract: the stream may already be
    /// gone, in which case the server freed the handle with it, so a failure here is not worth reporting.
    /// </summary>
    public async Task ClosePreparedAsync(int slotIndex, PreparedSlotEntry entry, CancellationToken ct)
    {
        try
        {
            await EnqueueAsync<object?>(
                BatchStatementKind.Close,
                new SqlRequest { StatementId = entry.StatementId }, slotIndex, ct, entry.TransportId).ConfigureAwait(false);
        }
        catch
        {
            // Stream already gone, or the handle was released with it — nothing to do.
        }
    }

    private static void Forget(Slot slot, string key, Task<PreparedSlotEntry> expected)
        => slot.Prepared.TryRemove(new KeyValuePair<string, Task<PreparedSlotEntry>>(key, expected));

    // ─── Pump ─────────────────────────────────────────────────────────────────

    private void TryStartPump(Slot slot)
    {
        if (Interlocked.CompareExchange(ref slot.Processing, 1, 0) == 0)
            _ = DeliverMessagesAsync(slot);
    }

    private async Task DeliverMessagesAsync(Slot slot)
    {
        try
        {
            while (true)
            {
                int drained = 0;
                while (slot.Inbox.TryDequeue(out QueuedItem item))
                {
                    await WriteItemAsync(slot, item).ConfigureAwait(false);
                    drained++;
                }

                // Coalesce: after writing a small batch, pause briefly so more ops accumulate before the
                // next drain writes them together.
                if (drained > 0 && options.CoalescingThreshold > 1
                    && drained < options.CoalescingThreshold && options.CoalescingDelayMs > 0)
                {
                    try { await Task.Delay(options.CoalescingDelayMs, shutdown.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                }

                Interlocked.Exchange(ref slot.Processing, 0);   // mark idle
                if (slot.Inbox.IsEmpty)
                    return;
                // Items arrived between drain and idle — re-acquire, or bail if another pump took over.
                if (Interlocked.CompareExchange(ref slot.Processing, 1, 0) != 0)
                    return;
            }
        }
        catch
        {
            Interlocked.Exchange(ref slot.Processing, 0);
        }
    }

    // The slot's pump is the only writer to its stream, so no write lock is needed.
    private async Task WriteItemAsync(Slot slot, QueuedItem item)
    {
        try
        {
            IBatchTransport transport = slot.Transport
                ?? throw new InvalidOperationException("Transport slot is not connected");

            // A prepared execution names a handle that exists only on the stream it was registered on.
            // This is the last moment the two can be compared — check any earlier and the stream could
            // still be rebuilt in between — so refuse here rather than send an op the server can only
            // answer with "unknown statement".
            if (item.ExpectedTransportId is long expected && transport.Id != expected)
                throw new PreparedStatementStaleException();

            item.Op.TransportId = transport.Id;

            await transport.SendAsync(item.Request, shutdown.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Fault(item.Op, ex);
        }
    }

    // ─── Reader / demux ───────────────────────────────────────────────────────

    private void StartReaderLoop(Slot slot, Func<long, IBatchTransport> factory)
    {
        _ = Task.Run(async () =>
        {
            while (!shutdown.IsCancellationRequested)
            {
                IBatchTransport transport = slot.Transport!;
                Exception fault = new IOException("gRPC batch stream closed");
                try
                {
                    await foreach (BatchExecuteResponse resp in transport.ReadAllAsync(shutdown.Token).ConfigureAwait(false))
                        Demux(resp);
                }
                catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    fault = ex;
                }
                finally
                {
                    try { await transport.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
                }

                // Fail this transport's still-pending ops so callers see the fault and can replay.
                slot.Transport = null;
                FailTransportPending(transport.Id, fault);
                if (shutdown.IsCancellationRequested)
                    break;

                // Rebuild the slot with a fresh transport for subsequent ops.
                slot.Transport = factory(Interlocked.Increment(ref transportIdSeq));
            }
        });
    }

    private void Demux(BatchExecuteResponse resp)
    {
        if (!pending.TryGetValue(resp.RequestId, out PendingOp? op))
            return;   // cancelled, timed out, or already completed — drop.

        switch (resp.PayloadCase)
        {
            case BatchExecuteResponse.PayloadOneofCase.Schema:
                op.Schema = resp.Schema;
                break;
            case BatchExecuteResponse.PayloadOneofCase.Row:
                (op.Rows ??= []).Add(resp.Row);
                break;
            case BatchExecuteResponse.PayloadOneofCase.QueryComplete:
                Complete(op, new BatchQueryResult(
                    op.Schema ?? new ResultSchema(), op.Rows ?? (IReadOnlyList<ResultRow>)[],
                    new BatchCausalToken(resp.QueryComplete.CausalTokenN, resp.QueryComplete.CausalTokenL, resp.QueryComplete.CausalTokenC),
                    resp.QueryComplete.CacheMetadata));
                break;
            case BatchExecuteResponse.PayloadOneofCase.NonQuery:
                Complete(op, new BatchNonQueryResult(
                    resp.NonQuery.AffectedRows,
                    new BatchCausalToken(resp.NonQuery.CausalTokenN, resp.NonQuery.CausalTokenL, resp.NonQuery.CausalTokenC)));
                break;
            case BatchExecuteResponse.PayloadOneofCase.StartReply:
                Complete(op, resp.StartReply);
                break;
            case BatchExecuteResponse.PayloadOneofCase.CommitReply:
                Complete(op, new BatchCausalToken(
                    resp.CommitReply.CausalTokenN, resp.CommitReply.CausalTokenL, resp.CommitReply.CausalTokenC));
                break;
            case BatchExecuteResponse.PayloadOneofCase.RollbackReply:
                Complete(op, null);
                break;
            case BatchExecuteResponse.PayloadOneofCase.PrepareReply:
                Complete(op, resp.PrepareReply);
                break;
            case BatchExecuteResponse.PayloadOneofCase.CloseReply:
                Complete(op, null);
                break;
            case BatchExecuteResponse.PayloadOneofCase.Error:
                Fault(op, new CamusException(resp.Error.Code, resp.Error.Message));
                break;
        }
    }

    private void Complete(PendingOp op, object? result)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;
        op.Dispose();
        op.Promise.TrySetResult(result);
    }

    private void Fault(PendingOp op, Exception ex)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;
        op.Dispose();
        if (ex is OperationCanceledException oce)
            op.Promise.TrySetCanceled(oce.CancellationToken);
        else
            op.Promise.TrySetException(ex);
    }

    // ConcurrentDictionary enumeration is safe under concurrent mutation, so no snapshot copy is needed;
    // Fault's TryRemove keeps a concurrently-completed op from being faulted twice.
    private void FailTransportPending(long transportId, Exception ex)
    {
        foreach (KeyValuePair<int, PendingOp> entry in pending)
            if (entry.Value.TransportId == transportId)
                Fault(entry.Value, ex);
    }

    public async ValueTask DisposeAsync()
    {
        shutdown.Cancel();
        foreach (Slot slot in slots)
        {
            IBatchTransport? t = slot.Transport;
            if (t is not null)
            {
                try { await t.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
            }
        }
        foreach (KeyValuePair<int, PendingOp> entry in pending)
            Fault(entry.Value, new ObjectDisposedException(nameof(GrpcBatcher)));
        shutdown.Dispose();
    }

    // ─── Nested state ─────────────────────────────────────────────────────────

    private sealed class Slot(int index)
    {
        public readonly int Index = index;
        public readonly ConcurrentQueue<QueuedItem> Inbox = new();
        public int Processing;   // 0 = idle, 1 = this slot's pump loop is running
        public volatile IBatchTransport? Transport;

        /// <summary>
        /// Statements registered on this slot, keyed by (database, sql). The value is the in-flight or
        /// finished registration rather than the result, so concurrent first executions of the same
        /// statement await one PREPARE instead of each sending their own.
        /// </summary>
        public readonly ConcurrentDictionary<string, Task<PreparedSlotEntry>> Prepared = new(StringComparer.Ordinal);
    }

    private readonly record struct QueuedItem(
        BatchExecuteRequest Request, PendingOp Op, long? ExpectedTransportId);

    /// <summary>One in-flight op awaiting its terminal response, plus the accumulator a QUERY needs.</summary>
    private sealed class PendingOp(int requestId)
    {
        public readonly int RequestId = requestId;
        public readonly TaskCompletionSource<object?> Promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ResultSchema? Schema;

        /// <summary>Row accumulator, materialized on the first ROW payload — only a QUERY ever needs it,
        /// and even a QUERY may complete with zero rows.</summary>
        public List<ResultRow>? Rows;

        public long TransportId;
        public GrpcBatcher? Owner;
        public CancellationTokenRegistration Registration;

        public void Dispose() => Registration.Dispose();
    }
}
