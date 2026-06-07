
using System.Data;
using System.Data.Common;
using System.Text.Json;
using Flurl.Http;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL transaction to be made in a CamusDB database.
/// A transaction in CamusDB is a set of reads and writes that execute
/// atomically at a single logical point in time across columns, rows, and
/// tables in a database.
/// </summary>
public class CamusTransaction : DbTransaction
{
    private readonly long txnIdPT;

    private readonly uint txnIdCounter;

    private readonly string endpoint;

    private readonly CamusConnection connection;

    protected readonly CamusConnectionStringBuilder builder;

    internal string Endpoint => endpoint;

    public long TxnIdPT => txnIdPT;

    public uint TxnIdCounter => txnIdCounter;

    public string TransactionId => string.Concat(txnIdPT, ":", txnIdCounter);

    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder)
    {
        this.txnIdPT = txnIdPT;
        this.txnIdCounter = txnIdCounter;
        this.endpoint = endpoint;
        this.connection = connection;
        this.builder = builder;
    }

    public override IsolationLevel IsolationLevel => IsolationLevel.Serializable;

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
    /// Commits the database transaction asynchronously, returning the commit timestamp.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token used for this task.</param>    
    public new async Task CommitAsync(CancellationToken cancellationToken = default)
    {        
        string database = builder.Config["Database"];

        try
        {
            CamusTransactionRequest request = new()
            {
                DatabaseName = database,
                TxnIdPT = txnIdPT,
                TxnIdCounter = txnIdCounter
            };

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusTransactionRequest);

            string responseJson = await this.endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(10)
                                                        .AppendPathSegments("commit-transaction")
                                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                                        .ReceiveString();

            CamusStartTransactionResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusStartTransactionResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");            
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
    /// Rollbacks the database transaction asynchronously, returning the commit timestamp.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token used for this task.</param>    
    public new async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        string database = builder.Config["Database"];

        try
        {
            CamusTransactionRequest request = new()
            {
                DatabaseName = database,
                TxnIdPT = txnIdPT,
                TxnIdCounter = txnIdCounter
            };

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusTransactionRequest);

            string responseJson = await this.endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(10)
                                                        .AppendPathSegments("rollback-transaction")
                                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                                        .ReceiveString();

            CamusStartTransactionResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusStartTransactionResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");
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
