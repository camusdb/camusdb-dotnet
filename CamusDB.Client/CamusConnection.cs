
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// Represents a connection to a single Camus database.
/// When opened, <see cref="CamusConnection" /> will acquire and maintain a session
/// with the target Camus database.
/// <see cref="CamusCommand" /> instances using this <see cref="CamusConnection" />
/// will use this session to execute their operation. Concurrent read operations can
/// share this session, but concurrent write operations may cause additional sessions
/// to be opened to the database.
/// Underlying sessions with the Camus database are pooled and are closed after a
/// configurable
/// <see>
/// <cref>CamusOptions.PoolEvictionDelay</cref>
/// </see>
/// .
/// </summary>
public sealed class CamusConnection : DbConnection
{
    private readonly CamusConnectionStringBuilder builder;

    private ConnectionState state = ConnectionState.Closed;

    [AllowNull]
    public override string ConnectionString { get; set; }

    public override string Database => builder.Config.TryGetValue("Database", out string? database) ? database : "";

    public override string DataSource => builder.Config.TryGetValue("Endpoint", out string? endpoint) ? endpoint : "";

    public override string ServerVersion => "1.0";

    public override ConnectionState State => state;

    /// <summary>
    /// Connection-wide default concurrency options applied to transactions (and autocommit statements)
    /// begun on this connection when the caller does not specify their own. Overrides the
    /// connection-string defaults and is itself overridden by a per-transaction
    /// <see cref="CamusTransactionOptions"/>. The EF provider sets this from
    /// <c>UseOptimisticLocking()</c>; a plain client caller can set it directly.
    /// </summary>
    public CamusTransactionOptions? DefaultTransactionOptions { get; set; }

    public CamusConnection(CamusConnectionStringBuilder builder)
    {
        ConnectionString = builder.ToString();
        this.builder = builder;
    }

    /// <summary>
    /// Resolves the effective options for a transaction/statement: the caller's explicit
    /// <paramref name="requested"/> knobs win, falling back to this connection's
    /// <see cref="DefaultTransactionOptions"/>, then the connection-string defaults, then (for any knob
    /// still unset) the server default.
    /// </summary>
    internal CamusTransactionOptions ResolveTransactionOptions(CamusTransactionOptions? requested)
        => (requested ?? CamusTransactionOptions.Default)
            .WithDefaults(DefaultTransactionOptions)
            .WithDefaults(builder.DefaultTransactionOptions);

    public override void ChangeDatabase(string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        builder.Config["Database"] = databaseName;
    }

    public override void Close()
    {
        state = ConnectionState.Closed;
    }

    public override void Open()
    {
        if (State == ConnectionState.Open)
            return;

        if (!builder.Config.ContainsKey("Endpoint"))
            throw new CamusException("CADB0000", "Endpoint is required");

        if (!builder.Config.ContainsKey("Database"))
            throw new CamusException("CADB0000", "Database is required");

        state = ConnectionState.Open;
    }

    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Open();
        return Task.CompletedTask;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => BeginTransactionAsync(MapIsolationLevel(isolationLevel)).GetAwaiter().GetResult();

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        => await BeginTransactionAsync(MapIsolationLevel(isolationLevel), cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Maps an ADO.NET <see cref="IsolationLevel"/> to CamusDB's options. <see cref="IsolationLevel.Unspecified"/>
    /// leaves the isolation knob unset (server / connection default); <see cref="IsolationLevel.ReadCommitted"/>
    /// and <see cref="IsolationLevel.Serializable"/> map through; <see cref="IsolationLevel.Snapshot"/> maps to a
    /// Serializable read-only snapshot. Other levels are rejected.
    /// </summary>
    private static CamusTransactionOptions? MapIsolationLevel(IsolationLevel isolationLevel) => isolationLevel switch
    {
        IsolationLevel.Unspecified => null,
        IsolationLevel.ReadCommitted => new CamusTransactionOptions { IsolationLevel = CamusIsolationLevel.ReadCommitted },
        IsolationLevel.Serializable => new CamusTransactionOptions { IsolationLevel = CamusIsolationLevel.Serializable },
        IsolationLevel.Snapshot => CamusTransactionOptions.Snapshot,
        _ => throw new NotSupportedException(
            $"CamusDB supports isolation levels {IsolationLevel.ReadCommitted}, {IsolationLevel.Serializable}, {IsolationLevel.Snapshot} and {IsolationLevel.Unspecified}."),
    };

    public new Task<CamusTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) =>
        BeginTransactionImplAsync(null, cancellationToken);

    /// <summary>
    /// Begins a transaction with explicit concurrency <paramref name="options"/> (isolation level,
    /// read/write mode, and pessimistic/optimistic locking). Any knob left <see langword="null"/> falls
    /// back to this connection's <see cref="DefaultTransactionOptions"/>, then the connection-string
    /// defaults, then the server default.
    /// </summary>
    public Task<CamusTransaction> BeginTransactionAsync(CamusTransactionOptions? options, CancellationToken cancellationToken = default) =>
        BeginTransactionImplAsync(options, cancellationToken);

    private async Task<CamusTransaction> BeginTransactionImplAsync(CamusTransactionOptions? options, CancellationToken cancellationToken)
    {
        string database = builder.Config["Database"];
        CamusTransactionOptions effective = ResolveTransactionOptions(options);
        string endpoint = builder.GetEndpoint();

        StartTransactionResult result = await builder.GetTransport()
            .StartTransactionAsync(endpoint, database, effective, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);

        return new CamusTransaction(result.TxnIdPT, result.TxnIdCounter, endpoint, this, builder, effective)
        {
            StreamSlot = result.StreamSlot,
        };
    }

    protected override DbCommand CreateDbCommand()
    {
        return new CamusCommand("", builder, this);
    }

    public CamusCommand CreateCamusCommand(string sql)
    {
        return new CamusCommand(sql, builder, this);
    }

    public CamusInsertCommand CreateInsertCommand(string source)
    {
        return new CamusInsertCommand(source, builder, this);
    }

    public CamusCommand CreateSelectCommand(string sql)
    {
        return new CamusCommand(sql, builder, this);
    }

    public CamusPingCommand CreatePingCommand()
    {
        return new CamusPingCommand("", builder, this);
    }

    /// <summary>
    /// Evicts every query result cache entry in the given family for the current database
    /// (<c>EVICT CACHE 'name'</c>). Family names are matched case-insensitively.
    /// </summary>
    public async Task EvictCacheAsync(string cacheName, CancellationToken cancellationToken = default)
    {
        using CamusCommand command = CreateCamusCommand(CamusCacheHint.Evict(cacheName));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Evicts every query result cache entry for the current database (<c>EVICT CACHE ALL</c>).
    /// Never touches another database's entries.
    /// </summary>
    public async Task EvictAllCacheAsync(CancellationToken cancellationToken = default)
    {
        using CamusCommand command = CreateCamusCommand(CamusCacheHint.EvictAll());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task CreateDatabaseAsync(bool ifNotExists = false, CancellationToken cancellationToken = default)
        => CreateDatabaseWithRetryAsync(Database, ifNotExists, cancellationToken);

    public Task CreateDatabaseAsync(string databaseName, bool ifNotExists = false, CancellationToken cancellationToken = default)
        => CreateDatabaseWithRetryAsync(databaseName, ifNotExists, cancellationToken);

    // Concurrent CreateDatabaseAsync calls can transiently collide while the server allocates the
    // shared database id sequence (e.g. many environments provisioning in parallel); the server reports
    // this as a "MustRetry" condition. Retry using the shared transient-failure classification in
    // SerializableRetryHelper (which the EF execution strategy also uses).
    private async Task CreateDatabaseWithRetryAsync(string databaseName, bool ifNotExists, CancellationToken cancellationToken)
    {
        const int maxAttempts = 5;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await CreateDatabaseImplAsync(databaseName, ifNotExists, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (CamusException ex) when (ifNotExists && IsDatabaseAlreadyExistsError(ex))
            {
                // IF NOT EXISTS means "ensure it exists" — and it now does. The server's idempotent
                // existence check has a TOCTOU gap versus concurrent registration: two racing
                // CREATE ... IF NOT EXISTS for the same name can both pass the check, and the one that
                // loses the registration race is rejected with DatabaseAlreadyExists ("is already
                // registered"). Since the caller asked for IF NOT EXISTS and the database is registered,
                // treat that as success rather than surfacing the race.
                return;
            }
            catch (CamusException ex) when (attempt < maxAttempts && IsTransientCreateDatabaseError(ex))
            {
                double baseMs = Math.Min(50d * (1 << (attempt - 1)), 800d);
                double jitter = baseMs * 0.25 * (2d * Random.Shared.NextDouble() - 1d);

                await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1d, baseMs + jitter)), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static bool IsTransientCreateDatabaseError(CamusException ex)
        => SerializableRetryHelper.IsRetryable(ex);

    // CADB0012 DatabaseAlreadyExists — raised by the server both on the direct "already exists" check and
    // when a CREATE ... IF NOT EXISTS loses a concurrent registration race ("is already registered").
    private static bool IsDatabaseAlreadyExistsError(CamusException ex)
        => ex.Code == "CADB0012";

    private async Task CreateDatabaseImplAsync(string databaseName, bool ifNotExists, CancellationToken cancellationToken)
    {
        string endpoint = builder.GetEndpoint();

        await builder.GetTransport()
            .CreateDatabaseAsync(endpoint, databaseName, ifNotExists, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
    }

    public Task DropDatabaseAsync(CancellationToken cancellationToken = default)
        => DropDatabaseImplAsync(Database, cancellationToken);

    public Task DropDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        => DropDatabaseImplAsync(databaseName, cancellationToken);

    /// <summary>
    /// Creates a copy-on-write branch of <paramref name="sourceDatabaseName"/> named
    /// <paramref name="branchName"/>. Equivalent to
    /// <c>CREATE DATABASE branchName BRANCH FROM sourceDatabaseName</c>.
    /// </summary>
    public Task CreateBranchDatabaseAsync(
        string branchName,
        string sourceDatabaseName,
        bool ifNotExists = false,
        CancellationToken cancellationToken = default)
        => CreateBranchDatabaseWithRetryAsync(branchName, sourceDatabaseName, ifNotExists, cancellationToken);

    private async Task CreateBranchDatabaseWithRetryAsync(
        string branchName,
        string sourceDatabaseName,
        bool ifNotExists,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 5;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await CreateBranchDatabaseImplAsync(branchName, sourceDatabaseName, ifNotExists, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (CamusException ex) when (attempt < maxAttempts && IsTransientCreateDatabaseError(ex))
            {
                double baseMs = Math.Min(50d * (1 << (attempt - 1)), 800d);
                double jitter = baseMs * 0.25 * (2d * Random.Shared.NextDouble() - 1d);

                await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1d, baseMs + jitter)), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task CreateBranchDatabaseImplAsync(
        string branchName,
        string sourceDatabaseName,
        bool ifNotExists,
        CancellationToken cancellationToken)
    {
        string endpoint = builder.GetEndpoint();

        await builder.GetTransport()
            .CreateBranchDatabaseAsync(endpoint, branchName, sourceDatabaseName, ifNotExists, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Returns every transitive descendant of <paramref name="databaseName"/>, ordered
    /// depth-ascending then name-ascending. Equivalent to
    /// <c>SHOW BRANCHES FROM databaseName</c>.
    /// A leaf database (no descendants) returns an empty list.
    /// </summary>
    public Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(
        string databaseName,
        CancellationToken cancellationToken = default)
        => ShowBranchesImplAsync(databaseName, cancellationToken);

    private async Task<IReadOnlyList<CamusBranchRow>> ShowBranchesImplAsync(
        string databaseName,
        CancellationToken cancellationToken)
    {
        string endpoint = builder.GetEndpoint();

        return await builder.GetTransport()
            .ShowBranchesAsync(endpoint, databaseName, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the full ancestry chain of <paramref name="databaseName"/> from nearest parent
    /// to root. Equivalent to <c>SHOW ANCESTORS FROM databaseName</c>.
    /// A root database (no ancestors) returns an empty list.
    /// </summary>
    public Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(
        string databaseName,
        CancellationToken cancellationToken = default)
        => ShowAncestorsImplAsync(databaseName, cancellationToken);

    private async Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsImplAsync(
        string databaseName,
        CancellationToken cancellationToken)
    {
        string endpoint = builder.GetEndpoint();

        return await builder.GetTransport()
            .ShowAncestorsAsync(endpoint, databaseName, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task DropDatabaseImplAsync(string databaseName, CancellationToken cancellationToken)
    {
        string endpoint = builder.GetEndpoint();

        await builder.GetTransport()
            .DropDatabaseAsync(endpoint, databaseName, builder.CommandTimeout, cancellationToken)
            .ConfigureAwait(false);
    }
}
