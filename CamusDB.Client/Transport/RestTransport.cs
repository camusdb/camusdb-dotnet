
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Flurl.Http;

namespace CamusDB.Client.Transport;

/// <summary>
/// The REST/JSON transport — the historical default. Each call is a single HTTP POST/GET to an
/// <c>execute-sql-*</c> / admin endpoint, source-generated JSON in and out, with the same
/// <see cref="FlurlHttpException"/> → <see cref="CamusException"/> translation and endpoint-health
/// bookkeeping the ADO surface used before the transport seam existed. Behavior is byte-identical to the
/// previous inlined code; it now lives behind <see cref="ICamusTransport"/> so gRPC can sit beside it.
/// </summary>
internal sealed class RestTransport(CamusConnectionStringBuilder builder) : ICamusTransport
{
    public CamusProtocol Protocol => CamusProtocol.Rest;

    public async Task<StartTransactionResult> StartTransactionAsync(
        string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusStartTransactionRequest request = new()
            {
                DatabaseName = database,
                IsolationLevel = options.IsolationLevelWire,
                TransactionMode = options.ModeWire,
                Locking = options.LockingWire
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("start-transaction")
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusStartTransactionRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusStartTransactionResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusStartTransactionResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");

            return new StartTransactionResult(response.TxnIdPT, response.TxnIdCounter);
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task FinalizeTransactionAsync(
        bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
    {
        _ = streamSlot;   // REST does not pin transactions to a stream.

        string pathSegment = commit ? "commit-transaction" : "rollback-transaction";
        string failureMessage = commit ? "Commit failed" : "Rollback failed";

        try
        {
            CamusTransactionRequest request = new()
            {
                DatabaseName = database,
                TxnIdPT = txnIdPT,
                TxnIdCounter = txnIdCounter
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments(pathSegment)
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusTransactionRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", failureMessage);
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        string endpoint = request.Endpoint;

        try
        {
            CamusExecuteSqlQueryRequest wire = new()
            {
                DatabaseName = request.Database,
                Sql = request.Sql,
                Parameters = ToDictionary(request.Parameters)
            };

            if (request.HasTransaction)
            {
                wire.TxnIdPT = request.TxnIdPT!.Value;
                wire.TxnIdCounter = request.TxnIdCounter!.Value;
            }

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(request.TimeoutSeconds)
                .AppendPathSegments("execute-sql-query")
                .PostAsync(CamusJsonContent.Create(wire, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusExecuteSqlQueryResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryResponse);

            if (response is null)
                throw new CamusException("CADB0000", "Empty result returned");

            CamusCacheMetadata? cacheMetadata = CamusCacheMetadata.FromResponse(response);

            // The response carries an authoritative `columns` schema plus positional `rows`. Decoding
            // from the schema (not by peeking at the first row) means the reader reports field count,
            // names and types even for an empty result — required by consumers that inspect the schema
            // before reading a row (e.g. EF Core's buffered reader under EnableRetryOnFailure).
            CamusResultSet resultSet = CamusResultSet.FromWire(response.Columns ?? [], response.Rows);

            return new QueryTransportResult(resultSet, cacheMetadata);
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        string endpoint = request.Endpoint;

        try
        {
            CamusExecuteSqlNonQueryRequest wire = BuildNonQueryRequest(request);

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(request.TimeoutSeconds)
                .AppendPathSegments("execute-sql-non-query")
                .PostAsync(CamusJsonContent.Create(wire, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            if (response is null)
                throw new CamusException("CADB0000", "Empty result returned");

            return response.Rows;
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
    {
        string endpoint = request.Endpoint;

        try
        {
            CamusExecuteDDLRequest wire = new()
            {
                DatabaseName = request.Database,
                Sql = request.Sql
            };

            if (request.HasTransaction)
            {
                wire.TxnIdPT = request.TxnIdPT!.Value;
                wire.TxnIdCounter = request.TxnIdCounter!.Value;
            }
            else if (request.AutocommitOptions is { } options)
            {
                wire.IsolationLevel = options.IsolationLevelWire;
                wire.TransactionMode = options.ModeWire;
                wire.Locking = options.LockingWire;
            }

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(request.TimeoutSeconds)
                .AppendPathSegments("execute-sql-ddl")
                .PostAsync(CamusJsonContent.Create(wire, CamusJsonSerializerContext.Default.CamusExecuteDDLRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            return response?.Status == "ok";
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            string responseJson = await endpoint
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("ping")
                .GetStringAsync(cancellationToken: cancellationToken);

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            return response?.Status == "ok";
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task CreateDatabaseAsync(
        string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusCreateDatabaseRequest request = new()
            {
                DatabaseName = database,
                IfNotExists = ifNotExists
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("create-db")
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusCreateDatabaseRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusCreateDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusCreateDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Create database failed");
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task CreateBranchDatabaseAsync(
        string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusCreateBranchDatabaseRequest request = new()
            {
                BranchName = branchName,
                SourceDatabaseName = sourceDatabaseName,
                IfNotExists = ifNotExists
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("create-branch-db")
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusCreateBranchDatabaseRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusCreateBranchDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusCreateBranchDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Create branch database failed");
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusDropDatabaseRequest request = new()
            {
                DatabaseName = database
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("drop-db")
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusDropDatabaseRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusDropDatabaseResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusDropDatabaseResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Drop database failed");
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusShowBranchesRequest request = new()
            {
                DatabaseName = database
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
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
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    public async Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusShowAncestorsRequest request = new()
            {
                DatabaseName = database
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
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
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    private static CamusExecuteSqlNonQueryRequest BuildNonQueryRequest(TransportSqlRequest request)
    {
        CamusExecuteSqlNonQueryRequest wire = new()
        {
            DatabaseName = request.Database,
            Sql = request.Sql,
            Parameters = ToDictionary(request.Parameters)
        };

        if (request.HasTransaction)
        {
            wire.TxnIdPT = request.TxnIdPT!.Value;
            wire.TxnIdCounter = request.TxnIdCounter!.Value;
        }
        else if (request.AutocommitOptions is { } options)
        {
            wire.IsolationLevel = options.IsolationLevelWire;
            wire.TransactionMode = options.ModeWire;
            wire.Locking = options.LockingWire;
        }

        return wire;
    }

    // The ADO layer already hands us a Dictionary; avoid a copy when it does, materialize otherwise.
    private static Dictionary<string, ColumnValue>? ToDictionary(IReadOnlyDictionary<string, ColumnValue>? parameters) => parameters switch
    {
        null => null,
        Dictionary<string, ColumnValue> dict => dict,
        _ => new Dictionary<string, ColumnValue>(parameters)
    };

    /// <summary>
    /// Maps a transport failure to a <see cref="CamusException"/>: marks the endpoint unreachable on a
    /// pure transport error, then prefers the server's <c>{code,message}</c> error body, falling back to
    /// the raw response and finally the exception message. Mirrors the historical inlined handling.
    /// </summary>
    private async Task<CamusException> TranslateAsync(FlurlHttpException ex, string endpoint)
    {
        CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

        string response = await ex.GetResponseStringAsync().ConfigureAwait(false);

        if (!string.IsNullOrEmpty(response))
        {
            try
            {
                CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                if (errorResponse is not null)
                    return new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
            }
            catch (JsonException)
            {
            }

            return new CamusException("CADB0000", response);
        }

        return new CamusException("CADB0000", ex.Message);
    }
}
