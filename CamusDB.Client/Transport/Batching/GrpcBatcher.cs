
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
///
/// <para><b>Ops that wait together travel together.</b> The pump writes the ops it drained for one
/// stream as a single stream message — a frame, see <see cref="BatchFrames"/> — when that stream's
/// server announced it reads them, and reads response frames at any time. A frame never waits: only
/// ops already in the inbox are packed, and a lone op is the plain single message it always was. It is
/// a transport optimization only, with no atomicity and no ordering the stream does not already give.</para>
///
/// <para><b>A stream is rotated when the credential it opened with is superseded.</b> A stream presents
/// its bearer token once, in its opening metadata, and then outlives it: the provider renews the token
/// every few minutes, the stream is meant to last a session. Whether the server tolerates that is the
/// server's decision — one that re-checks the opening token per operation ends the stream at the token's
/// expiry, with every transaction on it — so the client does not rely on it. When the slot's next
/// unbound op finds the credential changed, the slot gets a fresh stream, opened under the new token,
/// and the old one is <em>retired</em>: it takes no new work, keeps serving the transactions that began
/// on it (a transaction cannot change streams; the server rolls it back when its stream closes), and is
/// closed as soon as the last of them ends. Rotation happens on the write path, so an idle client rotates
/// on its first op after the renewal, before that op is sent.</para>
/// </summary>
internal sealed class GrpcBatcher : IAsyncDisposable
{
    private readonly GrpcBatchOptions options;
    private readonly Func<long, IBatchTransport> transportFactory;
    private readonly Func<object?>? credentialStamp;
    private readonly Slot[] slots;
    private readonly CancellationTokenSource shutdown = new();

    private readonly ConcurrentDictionary<int, PendingOp> pending = new();

    /// <summary>
    /// The stream each open transaction began on, by handle. Written when a START is answered, removed
    /// when the server answers its COMMIT or ROLLBACK, or when that stream ends. It is what lets a
    /// transaction keep its stream across a rotation of the slot it is pinned to.
    /// </summary>
    private readonly ConcurrentDictionary<(long Pt, uint Counter), StreamLease> openTransactions = new();

    /// <summary>Retired streams that are still draining, so disposal can reach them: a slot only
    /// references its current stream.</summary>
    private readonly ConcurrentDictionary<StreamLease, byte> retiring = new();

    private static int requestIdSeq;
    private int roundRobin = -1;
    private long transportIdSeq;

    /// <summary>
    /// Builds a batcher over <paramref name="options"/>.<see cref="GrpcBatchOptions.ChannelPoolSize"/>
    /// transports produced by <paramref name="transportFactory"/> (the argument is a fresh transport id).
    /// The factory is called again to rebuild a slot after its stream faults — but lazily, on the next
    /// op that needs the slot, never in a retry loop.
    ///
    /// <para><paramref name="credentialStamp"/> reports the credential a stream opened <em>now</em> would
    /// carry — in practice the current bearer token. It is compared, never inspected: a value that
    /// differs from the one a stream opened under is what retires that stream. Null means streams carry
    /// no credential that can change, and nothing is ever rotated.</para>
    /// </summary>
    public GrpcBatcher(
        GrpcBatchOptions options,
        Func<long, IBatchTransport> transportFactory,
        Func<object?>? credentialStamp = null)
    {
        this.options = options;
        this.transportFactory = transportFactory;
        this.credentialStamp = credentialStamp;
        int poolSize = Math.Max(1, options.ChannelPoolSize);
        slots = new Slot[poolSize];
        for (int i = 0; i < poolSize; i++)
        {
            // Connect the first transport synchronously so a slot is never written to before it exists;
            // its reader then owns reads, and the write path rebuilds the slot after a fault.
            slots[i] = new Slot(i);
            Connect(slots[i]);
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

        PendingOp op = new(id, kind, HandleKey(request.TxnHandle));

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
    /// The cache key for a statement on a slot: the (database, sql) pair itself. Holding the two strings
    /// rather than a joined copy keeps the components distinct — so two different statements can never
    /// collide into one entry — and spends no allocation per lookup, which a warm prepared execution
    /// performs on every call.
    /// </summary>
    private readonly record struct StatementKey(string Database, string Sql);

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
        StatementKey key = new(database, sql);

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

                if (entry.TransportId == Volatile.Read(ref slot.Current)?.Transport.Id)
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
    /// True when the transaction began on a stream that has since been rotated out, and is finishing
    /// there.
    ///
    /// <para>Such a transaction must not run prepared. A slot keeps one registration per statement, and
    /// it belongs to the slot's <em>current</em> stream; this transaction's ops go to a different one,
    /// where that handle does not exist. Registering it there instead would evict the entry every
    /// autocommit caller is using, and the two would re-prepare against each other for as long as the
    /// old stream drains. Running inline is always correct, and the window is short.</para>
    /// </summary>
    public bool IsBoundToRetiredStream(long txnIdPt, uint txnIdCounter)
        => openTransactions.TryGetValue((txnIdPt, txnIdCounter), out StreamLease? home) && home.Retired;

    /// <summary>
    /// Forgets a slot's registration for a statement, but only if it is still the one the caller was
    /// using — so a concurrent re-prepare that already succeeded is not thrown away by a straggler
    /// reacting to the old entry.
    /// </summary>
    public void InvalidatePrepared(int slotIndex, string database, string sql, PreparedSlotEntry stale)
    {
        Slot slot = slots[slotIndex];
        StatementKey key = new(database, sql);

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
        StatementKey key = new(database, sql);
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

    private static void Forget(Slot slot, StatementKey key, Task<PreparedSlotEntry> expected)
        => slot.Prepared.TryRemove(new KeyValuePair<StatementKey, Task<PreparedSlotEntry>>(key, expected));

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

                // Ops that arrive while a write is awaited belong to the same drain, as they always did.
                do
                {
                    while (slot.Inbox.TryDequeue(out QueuedItem item))
                    {
                        await StageAsync(slot, item).ConfigureAwait(false);
                        drained++;
                    }

                    await FlushAsync(slot).ConfigureAwait(false);
                }
                while (!slot.Inbox.IsEmpty);

                // Coalesce: after writing a small burst, pause briefly so more ops accumulate before the
                // next drain writes them together. Only after a drain of TWO or more items: a one-item drain
                // is a request/response ping-pong (one caller, whose next op cannot arrive until this one is
                // answered), and sleeping there adds the whole delay to every round trip while gaining
                // nothing — measured on the bank workload as ~3.5 ms per statement, 22 ms per six-statement
                // transfer, and a fifth of the throughput at low concurrency. Ops that are waiting together
                // already share a frame without any pause; the pause only makes the next frame fuller, so it
                // is worth paying only when a burst is demonstrably arriving.
                if (drained >= 2 && options.CoalescingThreshold > 1
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
        catch (Exception ex)
        {
            // Staging and flushing report their failures per op, so this is not expected; but an op left
            // in the run would otherwise wait for an answer to a message that was never written.
            FaultRun(slot, ex);
            slot.Frame.Items.Clear();
            slot.Run.Clear();
            Interlocked.Exchange(ref slot.Processing, 0);
        }
    }

    /// <summary>
    /// Does everything an op needs before it is written — routing, the prepared-statement check, the hold
    /// on its stream — and then either writes it alone or adds it to the slot's run, the ops that will
    /// share the next stream message. All of it is per op: a frame must not let one op's routing or one
    /// op's failure stand in for another's.
    ///
    /// <para>A run holds consecutive ops bound for <b>one</b> stream. The ops of one drain can belong to
    /// different streams — a transaction finishing on a retired stream, autocommit ops on the current one
    /// — so an op for another stream first flushes the run. Ops are never reordered to make a fuller
    /// frame: the server chains the ops of a transaction by arrival order, and inbox order is the only
    /// order the callers gave.</para>
    /// </summary>
    // The slot's pump is the only writer to its stream, so no write lock is needed.
    private async Task StageAsync(Slot slot, QueuedItem item)
    {
        PendingOp op = item.Op;
        StreamLease lease;

        try
        {
            // Cancelled while it sat in the inbox: it holds nothing yet, so it is simply left out.
            if (!MustStillBeWritten(op))
                return;

            lease = Route(slot, op);
            IBatchTransport transport = lease.Transport;

            // A prepared execution names a handle that exists only on the stream it was registered on.
            // This is the last moment the two can be compared — check any earlier and the stream could
            // still be rebuilt in between — so refuse here rather than send an op the server can only
            // answer with "unknown statement".
            if (item.ExpectedTransportId is long expected && transport.Id != expected)
                throw new PreparedStatementStaleException();

            op.TransportId = transport.Id;

            // Counted before it is published on the op: a release that raced in between would otherwise
            // take the count below zero and let a retired stream look drained while this op is on it.
            Interlocked.Increment(ref lease.InFlight);
            Volatile.Write(ref op.Lease, lease);

            // Cancelled between the check above and the publish: nobody is left to release it, so do it
            // here.
            if (!pending.ContainsKey(op.RequestId))
            {
                Release(op);

                if (!MustStillBeWritten(op))
                    return;
            }
        }
        catch (Exception ex)
        {
            Fault(op, ex);
            return;
        }

        // Frames only to a server that announced them on this very stream. The announcement is read, never
        // awaited: until it arrives — and for good against a server that makes none — each op is its own
        // message, written at once as it always was.
        bool framed = options.RequestFrames && lease.Transport.FramesAnnounced;

        if (slot.Run.Count > 0 && (!framed || !ReferenceEquals(slot.RunLease, lease)))
            await FlushAsync(slot).ConfigureAwait(false);

        if (!framed)
        {
            await SendAsync(lease, item.Request, op).ConfigureAwait(false);
            return;
        }

        if (slot.Run.Count == 0)
        {
            // A lone op is never measured: it travels as the plain message whatever its size.
            slot.Run.Add(item);
            slot.RunLease = lease;
            slot.RunBytes = -1;
            return;
        }

        if (slot.RunBytes < 0)
            slot.RunBytes = FrameCost(slot.Run[0].Request);

        int cost = FrameCost(item.Request);

        // Both limits are the sender's duty. The byte budget most of all: a message over the transport's
        // limit is rejected before the server can parse it, which resets the stream for every op on it.
        // An op over the budget on its own ends up alone in its run, and so travels as a single message.
        if (slot.Run.Count >= BatchFrames.MaxItems || (long)slot.RunBytes + cost > BatchFrames.MaxBytes)
        {
            await FlushAsync(slot).ConfigureAwait(false);

            slot.Run.Add(item);
            slot.RunLease = lease;
            slot.RunBytes = cost;
            return;
        }

        slot.Run.Add(item);
        slot.RunBytes += cost;
    }

    /// <summary>
    /// Writes the slot's run: one frame, or the plain single message when only one op is left in it, so a
    /// quiet stream is byte-identical to one without frames.
    ///
    /// <para>A frame is never resent. One whose write failed may or may not have reached the server,
    /// which is true of every op in it, so every op faults with the transport error and the retry
    /// contract above the batcher decides — exactly what happens to the ops of a faulted stream.</para>
    /// </summary>
    private async Task FlushAsync(Slot slot)
    {
        if (slot.Run.Count == 0)
            return;

        BatchExecuteRequest frame = slot.Frame;

        try
        {
            // Cancelled since it was staged: its hold on the stream is already released, leave it out.
            foreach (QueuedItem item in slot.Run)
            {
                if (MustStillBeWritten(item.Op))
                    frame.Items.Add(item.Request);
            }

            if (frame.Items.Count == 1)
                await slot.RunLease!.Transport.SendAsync(frame.Items[0], shutdown.Token).ConfigureAwait(false);
            else if (frame.Items.Count > 1)
                await slot.RunLease!.Transport.SendAsync(frame, shutdown.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            FaultRun(slot, ex);
        }
        finally
        {
            // The envelope is reused, which is safe only because an awaited send has serialized its
            // message by the time it returns.
            frame.Items.Clear();
            slot.Run.Clear();
            slot.RunLease = null;
            slot.RunBytes = -1;
        }
    }

    private async Task SendAsync(StreamLease lease, BatchExecuteRequest request, PendingOp op)
    {
        try
        {
            await lease.Transport.SendAsync(request, shutdown.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Fault(op, ex);
        }
    }

    private void FaultRun(Slot slot, Exception ex)
    {
        foreach (QueuedItem item in slot.Run)
            Fault(item.Op, ex);
    }

    /// <summary>
    /// False for an op whose caller is gone (cancelled, or timed out) before the op was written: the
    /// server is never asked to run it. A ROLLBACK and a CLOSE are written regardless — they only release
    /// what the server holds (a transaction's locks, a statement handle), and nothing else would.
    /// </summary>
    private bool MustStillBeWritten(PendingOp op)
        => op.Kind is BatchStatementKind.Rollback or BatchStatementKind.Close || pending.ContainsKey(op.RequestId);

    /// <summary>What an op adds to a frame: its serialized size, plus the tag and length it is wrapped in.</summary>
    private static int FrameCost(BatchExecuteRequest request)
    {
        int size = request.CalculateSize();
        return 1 + Google.Protobuf.CodedOutputStream.ComputeLengthSize(size) + size;
    }

    // ─── Reader / demux ───────────────────────────────────────────────────────

    /// <summary>
    /// Picks the stream an op is written to. Called only from the slot's pump — the single writer — so
    /// at most one connect or rotation runs per slot at a time, and only when an op actually needs the
    /// stream: a client sitting idle against an unreachable server never cycles through connect attempts.
    ///
    /// <para>An op of an open transaction goes to the stream that transaction began on, retired or not.
    /// Everything else goes to the slot's current stream, which is first replaced if it faulted, or if
    /// the credential it opened under has been superseded.</para>
    /// </summary>
    private StreamLease Route(Slot slot, PendingOp op)
    {
        if (op.Transaction is { } handle
            && op.Kind != BatchStatementKind.Start
            && openTransactions.TryGetValue(handle, out StreamLease? home))
        {
            return home;
        }

        StreamLease? current = Volatile.Read(ref slot.Current);
        if (current is null)
            return Connect(slot);

        // A null stamp is "no token right now" — one was just invalidated and its replacement is not
        // minted yet. That is no reason to trade a working stream for one opened with no credential.
        if (credentialStamp?.Invoke() is { } stamp && !stamp.Equals(current.Stamp))
        {
            Retire(current);
            return Connect(slot);
        }

        return current;
    }

    /// <summary>
    /// Opens a fresh stream for a slot and makes it current. The stamp is read <b>before</b> the factory
    /// reads the credential itself: if the credential is renewed in between, the stream is stamped older
    /// than it is and is rotated once more than needed. Read afterwards, it could be stamped newer than
    /// it is, and would never be rotated at all.
    /// </summary>
    private StreamLease Connect(Slot slot)
    {
        object? stamp = credentialStamp?.Invoke();
        StreamLease lease = new(transportFactory(Interlocked.Increment(ref transportIdSeq)), stamp);

        Volatile.Write(ref slot.Current, lease);
        _ = Task.Run(() => ReadAsync(slot, lease));

        return lease;
    }

    /// <summary>
    /// Takes a stream out of rotation. It stays open for the transactions that began on it and is closed
    /// when the last one ends — or after <see cref="GrpcBatchOptions.StreamDrainTimeoutMs"/>, because a
    /// transaction its caller abandoned would otherwise hold the stream, and its locks, open for good.
    /// Closing the stream is what makes the server roll such a transaction back.
    /// </summary>
    private void Retire(StreamLease lease)
    {
        retiring[lease] = 0;
        lease.Retired = true;

        CloseIfDrained(lease);

        if (Volatile.Read(ref lease.Closed) == 0)
            _ = CloseAfterDrainTimeoutAsync(lease);
    }

    private async Task CloseAfterDrainTimeoutAsync(StreamLease lease)
    {
        try
        {
            await Task.Delay(Math.Max(0, options.StreamDrainTimeoutMs), shutdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { return; }
        catch (ObjectDisposedException) { return; }

        Close(lease);
    }

    private void CloseIfDrained(StreamLease lease)
    {
        if (lease.Retired
            && Volatile.Read(ref lease.InFlight) <= 0
            && Volatile.Read(ref lease.OpenTransactions) <= 0)
        {
            Close(lease);
        }
    }

    /// <summary>Closes a stream once. Its reader then ends and does the rest of the teardown.</summary>
    private static void Close(StreamLease lease)
    {
        if (Interlocked.Exchange(ref lease.Closed, 1) != 0)
            return;

        _ = DisposeQuietlyAsync(lease.Transport);
    }

    private static async Task DisposeQuietlyAsync(IBatchTransport transport)
    {
        try { await transport.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
    }

    /// <summary>Drops an op's hold on its stream, exactly once however many paths race to do it.</summary>
    private void Release(PendingOp op)
    {
        StreamLease? lease = Interlocked.Exchange(ref op.Lease, null);

        if (lease is not null && Interlocked.Decrement(ref lease.InFlight) <= 0)
            CloseIfDrained(lease);
    }

    /// <summary>
    /// The one reader of one stream, for that stream's whole life. When the stream ends — closed by the
    /// server, faulted, or closed here after draining — it fails whatever is still pending on it so the
    /// callers can replay, and forgets the transactions that began on it, which the server has rolled
    /// back. It never reconnects: the next op through the pump does that.
    /// </summary>
    private async Task ReadAsync(Slot slot, StreamLease lease)
    {
        IBatchTransport transport = lease.Transport;
        Exception fault = new IOException("gRPC batch stream closed");

        try
        {
            await foreach (BatchExecuteResponse resp in transport.ReadAllAsync(shutdown.Token).ConfigureAwait(false))
                Demux(resp);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            // Disposal fails the pending ops itself.
        }
        catch (ObjectDisposedException) when (shutdown.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            fault = ex;
        }
        finally
        {
            Interlocked.Exchange(ref lease.Closed, 1);
            await DisposeQuietlyAsync(transport).ConfigureAwait(false);
        }

        // Only if it is still the slot's stream: a retired one was replaced long before it ended.
        Interlocked.CompareExchange(ref slot.Current, null, lease);
        retiring.TryRemove(lease, out _);

        foreach (KeyValuePair<(long Pt, uint Counter), StreamLease> entry in openTransactions)
        {
            if (ReferenceEquals(entry.Value, lease))
                openTransactions.TryRemove(entry);
        }

        FailTransportPending(transport.Id, fault);
    }

    private void Demux(BatchExecuteResponse resp)
    {
        // A response frame: each item is handled exactly as if it had arrived alone, in order, so the
        // messages of one request_id keep theirs. Accepted at any time, whatever this client has sent. A
        // frame inside a frame is dropped, not followed.
        if (resp.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Frame)
        {
            foreach (BatchExecuteResponse item in resp.Frame.Items)
            {
                if (item.PayloadCase != BatchExecuteResponse.PayloadOneofCase.Frame)
                    Demux(item);
            }

            return;
        }

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
                    resp.QueryComplete.CacheMetadata,
                    resp.QueryComplete.Routing));
                break;
            case BatchExecuteResponse.PayloadOneofCase.NonQuery:
                Complete(op, new BatchNonQueryResult(
                    resp.NonQuery.AffectedRows,
                    new BatchCausalToken(resp.NonQuery.CausalTokenN, resp.NonQuery.CausalTokenL, resp.NonQuery.CausalTokenC),
                    resp.NonQuery.Routing));
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
                Fault(op, new CamusException(resp.Error.Code, CamusErrorText.Sanitize(resp.Error.Message)));
                break;
        }
    }

    private void Complete(PendingOp op, object? result)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;

        // Before the op lets go of its stream: a START that has just been answered is the only thing
        // holding a retired stream open until its transaction is on the books.
        if (result is TxnHandle started && op.Kind == BatchStatementKind.Start)
            TransactionBegan((started.TxnIdPt, started.TxnIdCounter), Volatile.Read(ref op.Lease));
        else if (op.Kind is BatchStatementKind.Commit or BatchStatementKind.Rollback)
            TransactionEnded(op.Transaction);

        Release(op);
        op.Dispose();
        op.Promise.TrySetResult(result);
    }

    private void Fault(PendingOp op, Exception ex)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;

        // Only an answer from the server ends the transaction. A COMMIT that timed out or was cancelled
        // here may still be open there, and the ROLLBACK that usually follows must find its stream.
        if (ex is CamusException && op.Kind is BatchStatementKind.Commit or BatchStatementKind.Rollback)
            TransactionEnded(op.Transaction);

        Release(op);
        op.Dispose();
        if (ex is OperationCanceledException oce)
            op.Promise.TrySetCanceled(oce.CancellationToken);
        else
            op.Promise.TrySetException(ex);
    }

    private void TransactionBegan((long Pt, uint Counter) handle, StreamLease? lease)
    {
        if (lease is null)
            return;

        Interlocked.Increment(ref lease.OpenTransactions);

        if (!openTransactions.TryAdd(handle, lease))
            Interlocked.Decrement(ref lease.OpenTransactions);   // a handle is unique; never double-count one
    }

    private void TransactionEnded((long Pt, uint Counter)? handle)
    {
        if (handle is { } key
            && openTransactions.TryRemove(key, out StreamLease? lease)
            && Interlocked.Decrement(ref lease.OpenTransactions) <= 0)
        {
            CloseIfDrained(lease);
        }
    }

    private static (long Pt, uint Counter)? HandleKey(TxnHandle? handle)
        => handle is null ? null : (handle.TxnIdPt, handle.TxnIdCounter);

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
            if (Volatile.Read(ref slot.Current) is { } current)
                await DisposeQuietlyAsync(current.Transport).ConfigureAwait(false);
        }
        foreach (KeyValuePair<StreamLease, byte> entry in retiring)
            await DisposeQuietlyAsync(entry.Key.Transport).ConfigureAwait(false);
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

        /// <summary>The stream new work is written to, or null after it faulted and before an op has
        /// needed the slot again. Written by the pump, and cleared by that stream's own reader.</summary>
        public StreamLease? Current;

        /// <summary>The ops staged for the next stream message, all bound for <see cref="RunLease"/>.
        /// Touched only by the slot's pump, as are the three fields below.</summary>
        public readonly List<QueuedItem> Run = [];

        public StreamLease? RunLease;

        /// <summary>Serialized size of <see cref="Run"/> as frame items, or -1 while it holds a single
        /// op that nobody has had a reason to measure.</summary>
        public int RunBytes = -1;

        /// <summary>The frame envelope, refilled for every frame this slot writes.</summary>
        public readonly BatchExecuteRequest Frame = new() { Kind = BatchStatementKind.Frame };

        /// <summary>
        /// Statements registered on this slot, keyed by (database, sql). The value is the in-flight or
        /// finished registration rather than the result, so concurrent first executions of the same
        /// statement await one PREPARE instead of each sending their own.
        /// </summary>
        public readonly ConcurrentDictionary<StatementKey, Task<PreparedSlotEntry>> Prepared = new();
    }

    /// <summary>
    /// One stream, the credential it opened under, and what is still riding on it — which is what
    /// decides when a retired stream may be closed.
    /// </summary>
    private sealed class StreamLease(IBatchTransport transport, object? stamp)
    {
        public readonly IBatchTransport Transport = transport;

        /// <summary>What the credential stamp read when this stream opened; compared, never inspected.</summary>
        public readonly object? Stamp = stamp;

        /// <summary>Ops written to this stream and not yet terminated.</summary>
        public int InFlight;

        /// <summary>Transactions that began on this stream and that the server has not yet finalized.</summary>
        public int OpenTransactions;

        /// <summary>Set once the slot has moved on to a newer stream; from then on it only drains.</summary>
        public volatile bool Retired;

        /// <summary>0 until the stream is closed, from either end.</summary>
        public int Closed;
    }

    private readonly record struct QueuedItem(
        BatchExecuteRequest Request, PendingOp Op, long? ExpectedTransportId);

    /// <summary>One in-flight op awaiting its terminal response, plus the accumulator a QUERY needs.</summary>
    private sealed class PendingOp(int requestId, BatchStatementKind kind, (long Pt, uint Counter)? transaction)
    {
        public readonly int RequestId = requestId;
        public readonly BatchStatementKind Kind = kind;

        /// <summary>The transaction this op belongs to, from the handle it carries; null for an
        /// autocommit op, and for a START, whose handle does not exist until it is answered.</summary>
        public readonly (long Pt, uint Counter)? Transaction = transaction;

        /// <summary>The stream this op was written to, held until the op terminates. Swapped to null
        /// atomically by whichever path releases it first.</summary>
        public StreamLease? Lease;
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
