
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Flurl.Http;
using System.Net;
using System.Text.Json;

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

    public CamusConnection(CamusConnectionStringBuilder builder)
    {
        ConnectionString = builder.ToString();
        this.builder = builder;
    }

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
    {
        if (isolationLevel != IsolationLevel.Unspecified && isolationLevel != IsolationLevel.Serializable)
            throw new NotSupportedException($"CamusDB only supports isolation levels {IsolationLevel.Serializable} and {IsolationLevel.Unspecified}.");

        return BeginTransactionAsync().GetAwaiter().GetResult();
    }

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        if (isolationLevel != IsolationLevel.Unspecified && isolationLevel != IsolationLevel.Serializable)
            throw new NotSupportedException($"CamusDB only supports isolation levels {IsolationLevel.Serializable} and {IsolationLevel.Unspecified}.");

        return await BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
    }

    public new Task<CamusTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) =>    
        BeginTransactionImplAsync(cancellationToken);

    private async Task<CamusTransaction> BeginTransactionImplAsync(CancellationToken cancellationToken)
    {
        string endpoint = "";
        string database = builder.Config["Database"];

        try
        {
            endpoint = builder.GetEndpoint();

            CamusStartTransactionRequest request = new()
            {
                DatabaseName = database
            };

            byte[] responseBytes = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(builder.CommandTimeout)
                                                        .AppendPathSegments("start-transaction")
                                                        .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusStartTransactionRequest), cancellationToken: cancellationToken)
                                                        .ReceiveBytes();

            CamusStartTransactionResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusStartTransactionResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");

            return new CamusTransaction(response.TxnIdPT, response.TxnIdCounter, endpoint, this, builder);
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {

                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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

    private async Task CreateDatabaseImplAsync(string databaseName, bool ifNotExists, CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();

            CamusCreateDatabaseRequest request = new()
            {
                DatabaseName = databaseName,
                IfNotExists = ifNotExists
            };

            byte[] responseBytes = await endpoint
                                            .WithHeader("Accept", "application/json")
                                            .WithTimeout(builder.CommandTimeout)
                                            .AppendPathSegments("create-db")
                                            .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusCreateDatabaseRequest), cancellationToken: cancellationToken)
                                            .ReceiveBytes();

            CamusCreateDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusCreateDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Create database failed");
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();

            CamusCreateBranchDatabaseRequest request = new()
            {
                BranchName = branchName,
                SourceDatabaseName = sourceDatabaseName,
                IfNotExists = ifNotExists
            };

            byte[] responseBytes = await endpoint
                                            .WithHeader("Accept", "application/json")
                                            .WithTimeout(builder.CommandTimeout)
                                            .AppendPathSegments("create-branch-db")
                                            .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusCreateBranchDatabaseRequest), cancellationToken: cancellationToken)
                                            .ReceiveBytes();

            CamusCreateBranchDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusCreateBranchDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Create branch database failed");
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();

            CamusShowBranchesRequest request = new()
            {
                DatabaseName = databaseName
            };

            byte[] responseBytes = await endpoint
                                            .WithHeader("Accept", "application/json")
                                            .WithTimeout(builder.CommandTimeout)
                                            .AppendPathSegments("show-branches")
                                            .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusShowBranchesRequest), cancellationToken: cancellationToken)
                                            .ReceiveBytes();

            CamusShowBranchesResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusShowBranchesResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Show branches failed");

            return (IReadOnlyList<CamusBranchRow>?)response.Branches ?? [];
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();

            CamusShowAncestorsRequest request = new()
            {
                DatabaseName = databaseName
            };

            byte[] responseBytes = await endpoint
                                            .WithHeader("Accept", "application/json")
                                            .WithTimeout(builder.CommandTimeout)
                                            .AppendPathSegments("show-ancestors")
                                            .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusShowAncestorsRequest), cancellationToken: cancellationToken)
                                            .ReceiveBytes();

            CamusShowAncestorsResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusShowAncestorsResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Show ancestors failed");

            return (IReadOnlyList<CamusBranchRow>?)response.Ancestors ?? [];
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
    }

    private async Task DropDatabaseImplAsync(string databaseName, CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();

            CamusDropDatabaseRequest request = new()
            {
                DatabaseName = databaseName
            };

            byte[] responseBytes = await endpoint
                                            .WithHeader("Accept", "application/json")
                                            .WithTimeout(builder.CommandTimeout)
                                            .AppendPathSegments("drop-db")
                                            .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusDropDatabaseRequest), cancellationToken: cancellationToken)
                                            .ReceiveBytes();

            CamusDropDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusDropDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Drop database failed");
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
    }
}
