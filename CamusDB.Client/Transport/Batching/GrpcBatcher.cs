
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
/// dropped on a single inbox queue, and drained by a single-flight pump onto its stream; a background
/// reader per stream demultiplexes responses back to the waiting op by id. Responses interleave and
/// arrive out of order across ops.</para>
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
    private readonly ConcurrentQueue<QueuedItem> inbox = new();

    private static int requestIdSeq;
    private int roundRobin = -1;
    private long transportIdSeq;
    private int processing;   // 0 = idle, 1 = a pump loop is running

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

    public Task<BatchQueryResult> EnqueueQueryAsync(SqlRequest request, int? slotIndex, CancellationToken ct)
        => EnqueueAsync<BatchQueryResult>(BatchStatementKind.Query, request, slotIndex, ct);

    public Task<BatchNonQueryResult> EnqueueNonQueryAsync(SqlRequest request, int? slotIndex, CancellationToken ct)
        => EnqueueAsync<BatchNonQueryResult>(BatchStatementKind.NonQuery, request, slotIndex, ct);

    public Task<TxnHandle> EnqueueStartAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<TxnHandle>(BatchStatementKind.Start, request, slotIndex, ct);

    public Task<BatchCausalToken> EnqueueCommitAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<BatchCausalToken>(BatchStatementKind.Commit, request, slotIndex, ct);

    public async Task EnqueueRollbackAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => await EnqueueAsync<object?>(BatchStatementKind.Rollback, request, slotIndex, ct).ConfigureAwait(false);

    private async Task<T> EnqueueAsync<T>(
        BatchStatementKind kind, SqlRequest request, int? slotIndex, CancellationToken ct)
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
        inbox.Enqueue(new QueuedItem(wire, slot, op));
        TryStartPump();

        object? result = await op.Promise.Task.ConfigureAwait(false);
        return (T)result!;
    }

    // ─── Pump ─────────────────────────────────────────────────────────────────

    private void TryStartPump()
    {
        if (Interlocked.CompareExchange(ref processing, 1, 0) == 0)
            _ = DeliverMessagesAsync();
    }

    private async Task DeliverMessagesAsync()
    {
        try
        {
            while (true)
            {
                int drained = 0;
                while (inbox.TryDequeue(out QueuedItem item))
                {
                    await WriteItemAsync(item).ConfigureAwait(false);
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

                Interlocked.Exchange(ref processing, 0);   // mark idle
                if (inbox.IsEmpty)
                    return;
                // Items arrived between drain and idle — re-acquire, or bail if another pump took over.
                if (Interlocked.CompareExchange(ref processing, 1, 0) != 0)
                    return;
            }
        }
        catch
        {
            Interlocked.Exchange(ref processing, 0);
        }
    }

    private async Task WriteItemAsync(QueuedItem item)
    {
        try
        {
            Slot slot = slots[item.SlotIndex];
            IBatchTransport transport = slot.Transport
                ?? throw new InvalidOperationException("Transport slot is not connected");
            item.Op.TransportId = transport.Id;

            await slot.WriteLock.WaitAsync(shutdown.Token).ConfigureAwait(false);
            try { await transport.SendAsync(item.Request, shutdown.Token).ConfigureAwait(false); }
            finally { slot.WriteLock.Release(); }
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
                op.Rows.Add(resp.Row);
                break;
            case BatchExecuteResponse.PayloadOneofCase.QueryComplete:
                Complete(op, new BatchQueryResult(
                    op.Schema ?? new ResultSchema(), op.Rows,
                    new BatchCausalToken(resp.QueryComplete.CausalTokenN, resp.QueryComplete.CausalTokenL, resp.QueryComplete.CausalTokenC)));
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

    private void FailTransportPending(long transportId, Exception ex)
    {
        foreach (KeyValuePair<int, PendingOp> entry in pending.ToArray())
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
        foreach (KeyValuePair<int, PendingOp> entry in pending.ToArray())
            Fault(entry.Value, new ObjectDisposedException(nameof(GrpcBatcher)));
        shutdown.Dispose();
    }

    // ─── Nested state ─────────────────────────────────────────────────────────

    private sealed class Slot(int index)
    {
        public readonly int Index = index;
        public readonly SemaphoreSlim WriteLock = new(1, 1);
        public volatile IBatchTransport? Transport;
    }

    private readonly record struct QueuedItem(BatchExecuteRequest Request, int SlotIndex, PendingOp Op);

    /// <summary>One in-flight op awaiting its terminal response, plus the accumulator a QUERY needs.</summary>
    private sealed class PendingOp(int requestId)
    {
        public readonly int RequestId = requestId;
        public readonly TaskCompletionSource<object?> Promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ResultSchema? Schema;
        public readonly List<ResultRow> Rows = [];
        public long TransportId;
        public GrpcBatcher? Owner;
        public CancellationTokenRegistration Registration;

        public void Dispose() => Registration.Dispose();
    }
}
