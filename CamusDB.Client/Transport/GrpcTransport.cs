
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Net.Http;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Grpc.Core;
using Grpc.Net.Client;

namespace CamusDB.Client.Transport;

/// <summary>
/// The gRPC transport — talks the <c>CamusSql</c> service (see <c>docs/grpc-client-protocol.md</c>). The
/// data plane (queries, non-queries, and the transaction lifecycle) is multiplexed over a small pool of
/// long-lived <c>BatchExecute</c> duplex streams by a <see cref="GrpcBatcher"/> per endpoint, so
/// concurrent operations coalesce onto shared streams instead of each paying a unary round-trip. DDL and
/// Ping stay on the unary RPCs (DDL is not batchable per the protocol). Values cross the wire via
/// <see cref="GrpcValueCodec"/>; the causal token is threaded forward for read-your-writes.
///
/// <para>Autocommit ops round-robin across the pool; a transaction reserves one stream slot at BEGIN and
/// pins its START/statements/COMMIT to it (via <see cref="TransportSqlRequest.StreamSlot"/> /
/// <see cref="StartTransactionResult.StreamSlot"/>) so the server's per-stream ordering chain sees them
/// together.</para>
///
/// <para>The <c>CamusSql</c>/<c>CamusRows</c> proto has no dedicated database-admin RPCs, so the admin
/// operations are expressed as SQL over the unary <c>ExecuteDdl</c> / the batched query path — exactly as
/// the server's own gRPC client does.</para>
/// </summary>
internal sealed class GrpcTransport : ICamusTransport, IDisposable
{
    static GrpcTransport()
    {
        // Allow plaintext HTTP/2 (h2c) so an `http://host:port` endpoint works in local/dev without TLS,
        // mirroring the REST transport's tolerance of plain http. `https://` endpoints are unaffected.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    private static readonly GrpcBatchOptions BatchOptions = new();

    private readonly ConcurrentDictionary<string, ChannelEntry> channels = new(StringComparer.OrdinalIgnoreCase);

    // Latest causal token observed on this transport (HLC N, L, C). Threaded into every request so the
    // server can order this session's operations for read-your-writes. Guarded by tokenLock.
    private readonly object tokenLock = new();
    private int tokenN;
    private long tokenL;
    private long tokenC;

    public CamusProtocol Protocol => CamusProtocol.Grpc;

    private sealed class ChannelEntry(GrpcChannel channel, CamusSql.CamusSqlClient client)
    {
        public GrpcChannel Channel { get; } = channel;
        public CamusSql.CamusSqlClient Client { get; } = client;
        public GrpcBatcher Batcher { get; } = new(BatchOptions, id => new GrpcBatchTransport(id, client));
    }

    private ChannelEntry GetEntry(string endpoint)
        => channels.GetOrAdd(endpoint, static ep =>
        {
            GrpcChannel channel = CreateChannel(ep);
            return new ChannelEntry(channel, new CamusSql.CamusSqlClient(channel));
        });

    private CamusSql.CamusSqlClient GetClient(string endpoint) => GetEntry(endpoint).Client;

    private GrpcBatcher GetBatcher(string endpoint) => GetEntry(endpoint).Batcher;

    // A channel tuned for long-lived batch streams: keep-alive pings so an idle stream is not dropped, and
    // multiple HTTP/2 connections so the stream pool isn't funneled through one connection's stream limit.
    private static GrpcChannel CreateChannel(string endpoint)
    {
        SocketsHttpHandler handler = new()
        {
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            EnableMultipleHttp2Connections = true,
        };

        return GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions { HttpHandler = handler });
    }

    // ─── Transactions ─────────────────────────────────────────────────────────

    public async Task<StartTransactionResult> StartTransactionAsync(
        string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
    {
        GrpcBatcher batcher = GetBatcher(endpoint);
        int slot = batcher.ReserveSlot();

        // START reuses SqlRequest.database + isolation/mode/locking; the sql field is ignored.
        SqlRequest wire = new()
        {
            Database = database,
            IsolationLevel = ToGrpcIsolation(options.IsolationLevel),
            TransactionMode = ToGrpcMode(options.Mode),
            Locking = ToGrpcLocking(options.Locking),
        };

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(timeoutSeconds, cancellationToken);
        try
        {
            TxnHandle handle = await batcher.EnqueueStartAsync(wire, slot, token).ConfigureAwait(false);
            ObserveToken(handle.CausalTokenN, handle.CausalTokenL, handle.CausalTokenC);
            return new StartTransactionResult(handle.TxnIdPt, handle.TxnIdCounter, slot);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            cts?.Dispose();
        }
    }

    public async Task FinalizeTransactionAsync(
        bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
    {
        GrpcBatcher batcher = GetBatcher(endpoint);
        int slot = streamSlot ?? batcher.ReserveSlot();

        SqlRequest wire = new() { Database = database, TxnHandle = BuildHandle(txnIdPT, txnIdCounter) };

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(timeoutSeconds, cancellationToken);
        try
        {
            if (commit)
            {
                BatchCausalToken reply = await batcher.EnqueueCommitAsync(wire, slot, token).ConfigureAwait(false);
                ObserveToken(reply);
            }
            else
            {
                await batcher.EnqueueRollbackAsync(wire, slot, token).ConfigureAwait(false);
            }
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            cts?.Dispose();
        }
    }

    // ─── Data plane (batched) ───────────────────────────────────────────────────

    public async Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        SqlRequest wire = BuildSqlRequest(request);

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(request.TimeoutSeconds, cancellationToken);
        try
        {
            BatchQueryResult result = await GetBatcher(request.Endpoint)
                .EnqueueQueryAsync(wire, request.StreamSlot, token).ConfigureAwait(false);

            ObserveToken(result.Token);

            // gRPC query results carry no cache-metadata channel (the proto has no such field), so cache
            // hints are unavailable on this transport.
            return new QueryTransportResult(BuildResultSet(result.Schema, result.Rows), cacheMetadata: null);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            cts?.Dispose();
        }
    }

    public async Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        SqlRequest wire = BuildSqlRequest(request);

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(request.TimeoutSeconds, cancellationToken);
        try
        {
            BatchNonQueryResult result = await GetBatcher(request.Endpoint)
                .EnqueueNonQueryAsync(wire, request.StreamSlot, token).ConfigureAwait(false);

            ObserveToken(result.Token);

            return result.AffectedRows;
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
        finally
        {
            cts?.Dispose();
        }
    }

    // ─── Unary (DDL, ping) ──────────────────────────────────────────────────────

    public async Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        SqlRequest wire = BuildSqlRequest(request);

        try
        {
            DdlReply reply = await GetClient(request.Endpoint)
                .ExecuteDdlAsync(wire, CallOptions(request.TimeoutSeconds, cancellationToken))
                .ResponseAsync.ConfigureAwait(false);

            ObserveToken(reply.CausalTokenN, reply.CausalTokenL, reply.CausalTokenC);

            // A DDL reply with no error means success; the gRPC path surfaces failure as an RpcException.
            return true;
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    public async Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            PingReply reply = await GetClient(endpoint)
                .PingAsync(new PingRequest(), CallOptions(timeoutSeconds, cancellationToken))
                .ResponseAsync.ConfigureAwait(false);

            return reply is not null;
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    // ─── Database admin (composed SQL) ──────────────────────────────────────────

    public Task CreateDatabaseAsync(
        string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
    {
        string sql = ifNotExists
            ? $"CREATE DATABASE IF NOT EXISTS {QuoteIdentifier(database)}"
            : $"CREATE DATABASE {QuoteIdentifier(database)}";

        return ExecuteAdminDdlAsync(endpoint, database: "", sql, timeoutSeconds, cancellationToken);
    }

    public Task CreateBranchDatabaseAsync(
        string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
    {
        string prefix = ifNotExists ? "CREATE DATABASE IF NOT EXISTS " : "CREATE DATABASE ";
        string sql = $"{prefix}{QuoteIdentifier(branchName)} BRANCH FROM {QuoteIdentifier(sourceDatabaseName)}";

        return ExecuteAdminDdlAsync(endpoint, database: "", sql, timeoutSeconds, cancellationToken);
    }

    public Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
        => ExecuteAdminDdlAsync(endpoint, database: "", $"DROP DATABASE {QuoteIdentifier(database)}", timeoutSeconds, cancellationToken);

    public async Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
    {
        CamusResultSet rows = await QueryAdminAsync(
            endpoint, database, $"SHOW BRANCHES FROM {QuoteIdentifier(database)}", timeoutSeconds, cancellationToken).ConfigureAwait(false);

        return MapBranchRows(rows);
    }

    public async Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
    {
        CamusResultSet rows = await QueryAdminAsync(
            endpoint, database, $"SHOW ANCESTORS FROM {QuoteIdentifier(database)}", timeoutSeconds, cancellationToken).ConfigureAwait(false);

        return MapBranchRows(rows);
    }

    private Task ExecuteAdminDdlAsync(string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
        => ExecuteDdlAsync(
            new TransportSqlRequest { Endpoint = endpoint, Database = database, Sql = sql, TimeoutSeconds = timeoutSeconds },
            cancellationToken);

    private async Task<CamusResultSet> QueryAdminAsync(string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
        => (await ExecuteQueryAsync(
            new TransportSqlRequest { Endpoint = endpoint, Database = database, Sql = sql, TimeoutSeconds = timeoutSeconds },
            cancellationToken).ConfigureAwait(false)).ResultSet;

    // SHOW BRANCHES emits columns [database, id, depth, parent, fork_timestamp]; SHOW ANCESTORS emits
    // [database, id, depth, fork_timestamp] (no parent). Map by column name so either shape reconstructs.
    private static IReadOnlyList<CamusBranchRow> MapBranchRows(CamusResultSet rows)
    {
        int columnCount = rows.ColumnCount;
        int databaseCol = -1, idCol = -1, depthCol = -1, parentCol = -1, forkCol = -1;

        for (int c = 0; c < columnCount; c++)
        {
            switch (rows.ColumnNames[c])
            {
                case "database": databaseCol = c; break;
                case "id": idCol = c; break;
                case "depth": depthCol = c; break;
                case "parent": parentCol = c; break;
                case "fork_timestamp": forkCol = c; break;
            }
        }

        List<CamusBranchRow> result = new(rows.RowCount);
        for (int r = 0; r < rows.RowCount; r++)
        {
            result.Add(new CamusBranchRow
            {
                Database = databaseCol >= 0 ? rows.GetCell(r, databaseCol).StrValue : null,
                Id = idCol >= 0 ? rows.GetCell(r, idCol).StrValue : null,
                Depth = depthCol >= 0 ? (int)rows.GetCell(r, depthCol).LongValue : 0,
                Parent = parentCol >= 0 ? rows.GetCell(r, parentCol).StrValue : null,
                ForkTimestamp = forkCol >= 0 ? rows.GetCell(r, forkCol).StrValue : null,
            });
        }

        return result;
    }

    // ─── Wire building / decoding ───────────────────────────────────────────────

    private static CamusResultSet BuildResultSet(ResultSchema schema, IReadOnlyList<ResultRow> rows)
    {
        int columnCount = schema.Columns.Count;
        string[] names = new string[columnCount];
        ColumnType[] types = new ColumnType[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            ColumnSchema column = schema.Columns[i];
            names[i] = column.Name;
            types[i] = GrpcValueCodec.ToClientColumnType(column.Type);
        }

        ColumnValue[] cells = new ColumnValue[rows.Count * columnCount];
        for (int r = 0; r < rows.Count; r++)
        {
            ResultRow row = rows[r];
            int rowBase = r * columnCount;
            for (int c = 0; c < columnCount; c++)
                cells[rowBase + c] = c < row.Values.Count ? GrpcValueCodec.Decode(row.Values[c]) : ColumnValue.Null;
        }

        return new CamusResultSet(names, cells, rows.Count, types);
    }

    private SqlRequest BuildSqlRequest(TransportSqlRequest request)
    {
        SqlRequest wire = new()
        {
            Database = request.Database,
            Sql = request.Sql,
        };

        if (request.Parameters is { } parameters)
        {
            foreach (KeyValuePair<string, ColumnValue> parameter in parameters)
                wire.Parameters[parameter.Key] = GrpcValueCodec.Encode(parameter.Value);
        }

        if (request.HasTransaction)
        {
            wire.TxnHandle = BuildHandle(request.TxnIdPT!.Value, request.TxnIdCounter!.Value);
        }
        else
        {
            // Autocommit: apply the per-statement concurrency knobs and thread the session's latest token.
            if (request.AutocommitOptions is { } options)
            {
                wire.IsolationLevel = ToGrpcIsolation(options.IsolationLevel);
                wire.TransactionMode = ToGrpcMode(options.Mode);
                wire.Locking = ToGrpcLocking(options.Locking);
            }

            (int n, long l, long c) = CurrentToken();
            wire.CausalTokenN = n;
            wire.CausalTokenL = l;
            wire.CausalTokenC = c;
        }

        return wire;
    }

    // Carries this session's latest observed token on the handle so resumed transaction statements keep
    // causal ordering (all three HLC components must travel — see the protocol doc §4.2).
    private TxnHandle BuildHandle(long txnIdPT, uint txnIdCounter)
    {
        (int n, long l, long c) = CurrentToken();
        return new TxnHandle
        {
            TxnIdPt = txnIdPT,
            TxnIdCounter = txnIdCounter,
            CausalTokenN = n,
            CausalTokenL = l,
            CausalTokenC = c,
        };
    }

    private (int N, long L, long C) CurrentToken()
    {
        lock (tokenLock)
            return (tokenN, tokenL, tokenC);
    }

    private void ObserveToken(BatchCausalToken token) => ObserveToken(token.N, token.L, token.C);

    // Merge a reply's token, keeping the HLC maximum (L first, then C) so the threaded token advances
    // monotonically regardless of reply ordering.
    private void ObserveToken(int n, long l, long c)
    {
        if (l == 0 && c == 0)
            return;

        lock (tokenLock)
        {
            if (l > tokenL || (l == tokenL && c > tokenC))
            {
                tokenL = l;
                tokenC = c;
                tokenN = n;
            }
        }
    }

    // A per-op deadline linked to the caller's token, so a wedged batch stream can't hang the caller
    // forever. Returns a null CTS when no timeout applies (nothing to dispose).
    private static (CancellationToken Token, CancellationTokenSource? Cts) WithTimeout(int timeoutSeconds, CancellationToken cancellationToken)
    {
        if (timeoutSeconds <= 0)
            return (cancellationToken, null);

        CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        return (cts.Token, cts);
    }

    private static CallOptions CallOptions(int timeoutSeconds, CancellationToken cancellationToken)
    {
        DateTime? deadline = timeoutSeconds > 0 ? DateTime.UtcNow.AddSeconds(timeoutSeconds) : null;
        return new CallOptions(deadline: deadline, cancellationToken: cancellationToken);
    }

    private static IsolationLevel ToGrpcIsolation(CamusIsolationLevel? level) => level switch
    {
        CamusIsolationLevel.ReadCommitted => IsolationLevel.ReadCommitted,
        CamusIsolationLevel.Serializable => IsolationLevel.Serializable,
        _ => IsolationLevel.Unspecified,
    };

    private static TransactionMode ToGrpcMode(CamusTransactionMode? mode) => mode switch
    {
        CamusTransactionMode.ReadWrite => TransactionMode.ReadWrite,
        CamusTransactionMode.ReadOnly => TransactionMode.ReadOnly,
        _ => TransactionMode.Unspecified,
    };

    private static LockingMode ToGrpcLocking(CamusLocking? locking) => locking switch
    {
        CamusLocking.Pessimistic => LockingMode.Pessimistic,
        CamusLocking.Optimistic => LockingMode.Optimistic,
        _ => LockingMode.Unspecified,
    };

    // Database names are bare identifiers in the grammar; backtick-escape so a name is never mistaken for a
    // keyword, doubling any embedded backtick.
    private static string QuoteIdentifier(string name) => $"`{name.Replace("`", "``")}`";

    /// <summary>
    /// Maps a gRPC failure to a <see cref="CamusException"/>. Domain errors carry the CamusDB code and
    /// message in the <c>camus-error-code</c> / <c>camus-error-message</c> trailers (per the protocol's
    /// error model); absent those, the status code/detail is surfaced under a generic code. Batched-op
    /// domain errors arrive as an in-band <c>BatchError</c> and are already surfaced as
    /// <see cref="CamusException"/> by the batcher, so they bypass this path.
    /// </summary>
    private static CamusException Translate(RpcException ex)
    {
        string? code = null;
        string? message = null;

        foreach (global::Grpc.Core.Metadata.Entry entry in ex.Trailers)
        {
            if (string.Equals(entry.Key, "camus-error-code", StringComparison.OrdinalIgnoreCase))
                code = entry.Value;
            else if (string.Equals(entry.Key, "camus-error-message", StringComparison.OrdinalIgnoreCase))
                message = entry.Value;
        }

        if (!string.IsNullOrEmpty(code))
            return new CamusException(code, message ?? "");

        return new CamusException("CADB0000", string.IsNullOrEmpty(ex.Status.Detail) ? ex.Message : ex.Status.Detail);
    }

    public void Dispose()
    {
        foreach (ChannelEntry entry in channels.Values)
        {
            try { entry.Batcher.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* best effort */ }
            entry.Channel.Dispose();
        }

        channels.Clear();
    }
}
