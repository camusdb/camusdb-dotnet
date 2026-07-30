
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Net.Http;
using CamusDB.Client.Auth;
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
///
/// <para>Authentication rides in the <c>authorization</c> request metadata, exactly as the REST transport
/// puts it in the HTTP header. Unary calls (DDL, Ping) attach it per call; the long-lived
/// <c>BatchExecute</c> streams attach it once, when the stream opens, because that is when the server
/// resolves the principal for the whole stream. Every batched entry point therefore awaits the token
/// before touching the batcher, so a stream is never opened — or rebuilt after a fault — with a token the
/// provider has not minted yet.</para>
///
/// <para>It is also the <see cref="ICamusLoginClient"/> for gRPC connections, exchanging credentials over
/// the <c>CamusAuth</c> service on the same channel as everything else — so a gRPC deployment never has
/// to expose the HTTP port merely to obtain a token. Those two RPCs deliberately do not consult the token
/// provider (Login has no token yet, Logout is handed the one to revoke), which is what keeps the
/// provider from re-entering itself while it holds its login gate.</para>
/// </summary>
internal sealed class GrpcTransport(CamusTokenProvider auth) : ICamusTransport, ICamusLoginClient, IDisposable
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
    // server can order this session's operations for read-your-writes. Held as one immutable object so
    // the per-request read is a lock-free volatile load; writers advance it by CAS.
    private sealed record CausalToken(int N, long L, long C);

    private static readonly CausalToken EmptyToken = new(0, 0, 0);

    private CausalToken causalToken = EmptyToken;

    public CamusProtocol Protocol => CamusProtocol.Grpc;

    private sealed class ChannelEntry(GrpcChannel channel, CamusSql.CamusSqlClient client, Func<global::Grpc.Core.Metadata?> headers)
    {
        public GrpcChannel Channel { get; } = channel;
        public CamusSql.CamusSqlClient Client { get; } = client;
        public CamusAuth.CamusAuthClient AuthClient { get; } = new(channel);

        // Lazy because the batcher eagerly opens its whole pool of BatchExecute streams: a caller that
        // only logs in, pings, or runs DDL never needs them, and — since a stream carries the token it
        // was opened with — opening them before the first login would open them unauthenticated.
        private readonly Lazy<GrpcBatcher> batcher = new(
            () => new GrpcBatcher(BatchOptions, id => new GrpcBatchTransport(id, client, headers())),
            LazyThreadSafetyMode.ExecutionAndPublication);

        // The batcher rebuilds a faulted stream on its own, so the factory reads the current token on
        // every call rather than closing over one — a stream re-opened after a token refresh carries the
        // new token.
        public GrpcBatcher Batcher => batcher.Value;

        public GrpcBatcher? BatcherIfCreated => batcher.IsValueCreated ? batcher.Value : null;
    }

    private ChannelEntry GetEntry(string endpoint)
        => channels.GetOrAdd(endpoint, ep =>
        {
            GrpcChannel channel = CreateChannel(ep);
            return new ChannelEntry(channel, new CamusSql.CamusSqlClient(channel), CurrentCallHeaders);
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
        await EnsureTokenAsync(cancellationToken).ConfigureAwait(false);

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
        await EnsureTokenAsync(cancellationToken).ConfigureAwait(false);

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
        await EnsureTokenAsync(cancellationToken).ConfigureAwait(false);

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(request.TimeoutSeconds, cancellationToken);
        try
        {
            BatchQueryResult result = await ExecuteBatchedAsync(
                request,
                static (batcher, wire, slot, transportId, ct) => batcher.EnqueueQueryAsync(wire, slot, ct, transportId),
                token).ConfigureAwait(false);

            ObserveToken(result.Token);

            // The cache verdict rides the QUERY terminator (it is known only once the server has drained
            // the cursor) and is absent for an unhinted statement, which maps to null metadata just as on REST.
            return new QueryTransportResult(
                BuildResultSet(result.Schema, result.Rows),
                CamusCacheMetadata.FromProto(result.CacheMetadata));
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

    // gRPC's data plane is multiplexed over shared BatchExecute streams that decode a whole result before
    // returning (see GrpcBatcher). Wiring row-incremental delivery through that batcher is a separate
    // effort, so streaming on gRPC buffers via the normal query path and replays through a buffered source
    // — the ExecuteStreamReaderAsync API stays uniform across transports even though only REST is truly
    // incremental. (The /execute-sql-query-stream endpoint this feature targets is REST-only.)
    public async Task<CamusRowSource> ExecuteQueryStreamAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        QueryTransportResult result = await ExecuteQueryAsync(request, cancellationToken).ConfigureAwait(false);
        return CamusRowSource.Buffered(result.ResultSet);
    }

    public async Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        await EnsureTokenAsync(cancellationToken).ConfigureAwait(false);

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(request.TimeoutSeconds, cancellationToken);
        try
        {
            BatchNonQueryResult result = await ExecuteBatchedAsync(
                request,
                static (batcher, wire, slot, transportId, ct) => batcher.EnqueueNonQueryAsync(wire, slot, ct, transportId),
                token).ConfigureAwait(false);

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

    // ─── Prepared statements ────────────────────────────────────────────────────

    /// <summary>
    /// Registers a statement so later executions can name it instead of resending it.
    ///
    /// <para>Registration happens on one stream, and a handle is meaningless on any other, so this
    /// prepares on a single slot — enough to answer what the caller actually asked (does it parse, and
    /// what is its binding order) without spending a handle on every stream in the pool for a statement
    /// that may only ever run once. Executions register themselves on whichever stream they land on, so
    /// the rest of the pool warms up as it is used.</para>
    /// </summary>
    public async Task<PreparedStatementInfo> PrepareAsync(
        string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
    {
        await EnsureTokenAsync(cancellationToken).ConfigureAwait(false);

        GrpcBatcher batcher = GetBatcher(endpoint);

        (CancellationToken token, CancellationTokenSource? cts) = WithTimeout(timeoutSeconds, cancellationToken);
        try
        {
            PreparedSlotEntry entry = await batcher
                .EnsurePreparedAsync(batcher.ReserveSlot(), database, sql, token).ConfigureAwait(false);

            return new PreparedStatementInfo(entry.ParameterNames);
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

    public async Task ClosePreparedAsync(string endpoint, string database, string sql, CancellationToken cancellationToken)
    {
        // Only a batcher that exists can hold registrations, and asking for one here would open the whole
        // stream pool just to release nothing.
        if (GetEntry(endpoint).BatcherIfCreated is not { } batcher)
            return;

        foreach ((int slot, PreparedSlotEntry entry) in batcher.TakePrepared(database, sql))
            await batcher.ClosePreparedAsync(slot, entry, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one batched op, as a prepared execution when the request asked for it.
    ///
    /// <para>A prepared execution pins to a single slot for its whole attempt: the handle lives on that
    /// slot's stream, so registering on one and executing on another would be guaranteed to fail. An
    /// autocommit statement picks the slot the same way the inline path would; a transaction's statement
    /// uses the slot the transaction already pinned to, which is also what keeps its ops ordered.</para>
    ///
    /// <para>Retries exactly once, and only for the two ways a registration goes stale underneath a
    /// correct caller: the stream was rebuilt between the check and the write
    /// (<see cref="PreparedStatementStaleException"/>), or the server does not know the handle
    /// (<c>CADB0520</c> — a rebuild this client had not noticed yet). Both mean "prepare again and
    /// resend". Everything else propagates: a transport fault on the execution itself is the caller's to
    /// handle under the normal retry taxonomy, and replaying a mutation that may already have been
    /// applied is not this layer's decision to make. A failure to <em>register</em> is absorbed instead —
    /// the statement simply runs inline, since preparing is an optimization and must never be the reason
    /// a working statement fails.</para>
    /// </summary>
    private async Task<T> ExecuteBatchedAsync<T>(
        TransportSqlRequest request,
        Func<GrpcBatcher, SqlRequest, int?, long?, CancellationToken, Task<T>> send,
        CancellationToken cancellationToken)
    {
        GrpcBatcher batcher = GetBatcher(request.Endpoint);

        if (!request.Prepared)
            return await send(batcher, BuildSqlRequest(request), request.StreamSlot, null, cancellationToken).ConfigureAwait(false);

        int slot = request.StreamSlot ?? batcher.ReserveSlot();

        for (int attempt = 0; ; attempt++)
        {
            PreparedSlotEntry entry;
            SqlRequest wire;

            try
            {
                entry = await batcher.EnsurePreparedAsync(slot, request.Database, request.Sql, cancellationToken).ConfigureAwait(false);
                wire = BuildPreparedSqlRequest(request, entry);
            }
            catch (Exception ex) when (ex is CamusException or RpcException)
            {
                return await send(batcher, BuildSqlRequest(request), slot, null, cancellationToken).ConfigureAwait(false);
            }

            try
            {
                return await send(batcher, wire, slot, entry.TransportId, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt == 0 && IsStaleRegistration(ex))
            {
                batcher.InvalidatePrepared(slot, request.Database, request.Sql, entry);
            }
        }
    }

    /// <summary>
    /// True for the failures that mean "this registration is gone", as opposed to a real error: the
    /// pre-write transport check, and the server's own unknown-statement code, which is the backstop for
    /// anything the check misses.
    /// </summary>
    private static bool IsStaleRegistration(Exception ex) =>
        ex is PreparedStatementStaleException ||
        (ex is CamusException camus && camus.Code == CamusPreparedStatementErrorCodes.UnknownPreparedStatement);

    // ─── Unary (DDL, ping) ──────────────────────────────────────────────────────

    public async Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        SqlRequest wire = BuildSqlRequest(request);
        global::Grpc.Core.Metadata? headers = await HeadersAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            DdlReply reply = await GetClient(request.Endpoint)
                .ExecuteDdlAsync(wire, CallOptions(request.TimeoutSeconds, headers, cancellationToken))
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
        global::Grpc.Core.Metadata? headers = await HeadersAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            PingReply reply = await GetClient(endpoint)
                .PingAsync(new PingRequest(), CallOptions(timeoutSeconds, headers, cancellationToken))
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

    // ─── Credential exchange (CamusAuth) ────────────────────────────────────────

    /// <summary>
    /// Exchanges a password for a bearer token over the <c>CamusAuth</c> service. No <c>authorization</c>
    /// metadata is attached — this is the one call a client makes before it has a token, and attaching one
    /// would mean asking the provider for a token while it is minting this very one.
    /// </summary>
    public async Task<CamusLoginResult> LoginAsync(
        string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            LoginReply reply = await GetEntry(endpoint).AuthClient
                .LoginAsync(new LoginRequest { User = user, Password = password }, CallOptions(timeoutSeconds, headers: null, cancellationToken))
                .ResponseAsync.ConfigureAwait(false);

            return new CamusLoginResult(reply.Token, ReadExpiry(reply));
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    /// <summary>Revokes <paramref name="token"/>, which travels in the metadata like on any authenticated
    /// call rather than in the message body.</summary>
    public async Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            await GetEntry(endpoint).AuthClient
                .LogoutAsync(new LogoutRequest(), CallOptions(timeoutSeconds, BuildHeaders(token), cancellationToken))
                .ResponseAsync.ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    /// <summary>
    /// How long the token is good for. Prefers the server-measured duration over the absolute deadline,
    /// so a client whose clock disagrees with the server's still renews on time. Zero means the server
    /// reported nothing, leaving the driver on its configured fallback lifetime.
    /// </summary>
    private static TimeSpan? ReadExpiry(LoginReply reply)
    {
        if (reply.ExpiresInSeconds > 0)
            return TimeSpan.FromSeconds(reply.ExpiresInSeconds);

        if (reply.ExpiresAtUnixMs > 0)
        {
            TimeSpan remaining = DateTimeOffset.FromUnixTimeMilliseconds(reply.ExpiresAtUnixMs) - DateTimeOffset.UtcNow;

            if (remaining > TimeSpan.Zero)
                return remaining;
        }

        return null;
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

        int rowCount = rows.Count;
        ColumnValue[] cells = new ColumnValue[rowCount * columnCount];
        for (int r = 0; r < rowCount; r++)
        {
            // Hoisted out of the cell loop: the RepeatedField indexer/count aren't free per access.
            Google.Protobuf.Collections.RepeatedField<Value> values = rows[r].Values;
            int valueCount = values.Count;
            int rowBase = r * columnCount;
            for (int c = 0; c < columnCount; c++)
                cells[rowBase + c] = c < valueCount ? GrpcValueCodec.Decode(values[c]) : ColumnValue.Null;
        }

        return new CamusResultSet(names, cells, rowCount, types);
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

        return ApplyExecutionContext(wire, request);
    }

    /// <summary>
    /// The wire request for a prepared execution: the handle plus this call's values in the published
    /// binding order. Deliberately carries no <c>database</c>, <c>sql</c> or named <c>parameters</c> —
    /// the handle already names all three, and the server refuses a request that sends both rather than
    /// resolving it by a precedence rule. Everything else — transaction, isolation, causal token —
    /// travels exactly as it does inline, which is what makes a prepared execution indistinguishable
    /// from an inline one past this point.
    /// </summary>
    private SqlRequest BuildPreparedSqlRequest(TransportSqlRequest request, in PreparedSlotEntry entry)
    {
        SqlRequest wire = new() { StatementId = entry.StatementId };

        foreach (ColumnValue value in PreparedStatementBinder.Bind(entry.ParameterNames, request.Parameters, static v => v))
            wire.PositionalParameters.Add(GrpcValueCodec.Encode(value));

        return ApplyExecutionContext(wire, request);
    }

    /// <summary>Applies the transaction handle, or the autocommit knobs plus this session's causal token,
    /// to a request that already carries what it executes.</summary>
    private SqlRequest ApplyExecutionContext(SqlRequest wire, TransportSqlRequest request)
    {
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
        CausalToken token = Volatile.Read(ref causalToken);
        return (token.N, token.L, token.C);
    }

    private void ObserveToken(BatchCausalToken token) => ObserveToken(token.N, token.L, token.C);

    // Merge a reply's token, keeping the HLC maximum (L first, then C) so the threaded token advances
    // monotonically regardless of reply ordering.
    private void ObserveToken(int n, long l, long c)
    {
        if (l == 0 && c == 0)
            return;

        while (true)
        {
            CausalToken current = Volatile.Read(ref causalToken);

            if (l < current.L || (l == current.L && c <= current.C))
                return;

            if (Interlocked.CompareExchange(ref causalToken, new CausalToken(n, l, c), current) == current)
                return;
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

    private static CallOptions CallOptions(int timeoutSeconds, global::Grpc.Core.Metadata? headers, CancellationToken cancellationToken)
    {
        DateTime? deadline = timeoutSeconds > 0 ? DateTime.UtcNow.AddSeconds(timeoutSeconds) : null;
        return new CallOptions(headers: headers, deadline: deadline, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Mints or refreshes the bearer token, for the batched entry points: they await this purely for its
    /// side effect — warming the provider's cache — before handing work to the batcher, whose stream
    /// factory can only read the token synchronously. Builds no metadata, since none is attached per op.
    /// </summary>
    private async ValueTask EnsureTokenAsync(CancellationToken cancellationToken)
        => _ = await auth.GetTokenAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Mints or refreshes the bearer token and returns it as request metadata.</summary>
    private async ValueTask<global::Grpc.Core.Metadata?> HeadersAsync(CancellationToken cancellationToken)
        => HeadersFor(await auth.GetTokenAsync(cancellationToken).ConfigureAwait(false));

    /// <summary>The cached token as metadata, without awaiting — the batch-stream factory's view.</summary>
    private global::Grpc.Core.Metadata? CurrentCallHeaders() => HeadersFor(auth.CurrentToken);

    private sealed record CachedHeaders(string Token, global::Grpc.Core.Metadata Metadata);

    private CachedHeaders? cachedHeaders;

    /// <summary>
    /// Request metadata for <paramref name="token"/>, cached per token — it changes every few minutes at
    /// most, so per-call rebuilding of the <c>Metadata</c> and its <c>"Bearer "</c> string is pure churn.
    /// The single-object cache makes the token/metadata pair swap atomically; gRPC only reads the shared
    /// instance when sending, so reuse across concurrent calls is safe.
    /// </summary>
    private global::Grpc.Core.Metadata? HeadersFor(string? token)
    {
        if (string.IsNullOrEmpty(token))
            return null;

        CachedHeaders? cached = Volatile.Read(ref cachedHeaders);
        if (cached is not null && string.Equals(cached.Token, token, StringComparison.Ordinal))
            return cached.Metadata;

        global::Grpc.Core.Metadata headers = new() { { "authorization", "Bearer " + token } };
        Volatile.Write(ref cachedHeaders, new CachedHeaders(token, headers));
        return headers;
    }

    private static global::Grpc.Core.Metadata? BuildHeaders(string? token)
        => string.IsNullOrEmpty(token) ? null : new global::Grpc.Core.Metadata { { "authorization", "Bearer " + token } };

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

        string detail = string.IsNullOrEmpty(ex.Status.Detail) ? ex.Message : ex.Status.Detail;

        // A rejection raised before the handler runs (the auth gate at stream open) can arrive without
        // trailers; recover the domain code from the status so the token-refresh path still triggers.
        return ex.StatusCode switch
        {
            StatusCode.Unauthenticated => new CamusException(CamusAuthErrorCodes.AuthenticationFailed, detail),
            StatusCode.PermissionDenied => new CamusException(CamusAuthErrorCodes.InsufficientPrivilege, detail),
            _ => new CamusException("CADB0000", detail),
        };
    }

    public void Dispose()
    {
        foreach (ChannelEntry entry in channels.Values)
        {
            if (entry.BatcherIfCreated is { } batcher)
            {
                try { batcher.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* best effort */ }
            }

            entry.Channel.Dispose();
        }

        channels.Clear();
    }
}
