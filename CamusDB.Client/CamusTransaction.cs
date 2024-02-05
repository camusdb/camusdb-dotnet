
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

    protected readonly CamusConnectionStringBuilder builder;

    public long TxnIdPT => txnIdPT;

    public uint TxnIdCounter => txnIdCounter;

    public string TransactionId => string.Concat(txnIdPT, ":", txnIdCounter);

    public CamusTransaction(long txnIdPT, uint txnIdCounter, CamusConnectionStringBuilder builder)
    {
        this.txnIdPT = txnIdPT;
        this.txnIdCounter = txnIdCounter;
        this.builder = builder;
    }

    public override IsolationLevel IsolationLevel => throw new NotImplementedException();

    protected override DbConnection? DbConnection => throw new NotImplementedException();
    
    /// <summary>
    /// Commits the database transaction synchronously
    /// </summary>
    public override void Commit() => Task.Run(() => CommitAsync(default));

    /// <summary>
    /// Rollbacks the database transaction synchronously
    /// </summary>
    public override void Rollback() => Task.Run(() => RollbackAsync(default));    

    /// <summary>
    /// Commits the database transaction asynchronously, returning the commit timestamp.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token used for this task.</param>    
    public new async Task CommitAsync(CancellationToken cancellationToken = default)
    {        
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];

        try
        {
            CamusStartTransactionResponse response = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(10)
                                                        .AppendPathSegments("commit-transaction")
                                                        .PostJsonAsync(new { databaseName = database, txnIdPT = txnIdPT, txnIdCounter = txnIdCounter }, cancellationToken)
                                                        .ReceiveJson<CamusStartTransactionResponse>();

            if (response.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");            
        }
        catch (FlurlHttpException ex)
        {
            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize<CamusErrorResponse>(response);

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
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];

        try
        {
            CamusStartTransactionResponse response = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(10)
                                                        .AppendPathSegments("rollback-transaction")
                                                        .PostJsonAsync(new { databaseName = database, txnIdPT = txnIdPT, txnIdCounter = txnIdCounter }, cancellationToken)
                                                        .ReceiveJson<CamusStartTransactionResponse>();

            if (response.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");
        }
        catch (FlurlHttpException ex)
        {
            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize<CamusErrorResponse>(response);

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