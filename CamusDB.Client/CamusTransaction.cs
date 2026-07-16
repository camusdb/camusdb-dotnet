
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

    /// <summary>
    /// The effective concurrency options this transaction was begun with (isolation, mode, locking), after
    /// merging the caller's request with the connection and connection-string defaults. A knob the caller
    /// left unset (deferred to the server default) reads back as <see langword="null"/> here.
    /// </summary>
    public CamusTransactionOptions Options { get; }

    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder)
        : this(txnIdPT, txnIdCounter, endpoint, connection, builder, CamusTransactionOptions.Default) { }

    public CamusTransaction(long txnIdPT, uint txnIdCounter, string endpoint, CamusConnection connection, CamusConnectionStringBuilder builder, CamusTransactionOptions options)
    {
        this.txnIdPT = txnIdPT;
        this.txnIdCounter = txnIdCounter;
        this.endpoint = endpoint;
        this.connection = connection;
        this.builder = builder;
        Options = options;
    }

    /// <summary>
    /// The ADO.NET view of this transaction's isolation level. CamusDB's default is Serializable, so an
    /// unspecified isolation reports <see cref="IsolationLevel.Serializable"/>; an explicit Read Committed
    /// reports <see cref="IsolationLevel.ReadCommitted"/>.
    /// </summary>
    public override IsolationLevel IsolationLevel => Options.IsolationLevel switch
    {
        CamusIsolationLevel.ReadCommitted => IsolationLevel.ReadCommitted,
        _ => IsolationLevel.Serializable,
    };

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
    public override async Task CommitAsync(CancellationToken cancellationToken = default)
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

            byte[] responseBytes = await this.endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(builder.CommandTimeout)
                                                        .AppendPathSegments("commit-transaction")
                                                        .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusTransactionRequest), cancellationToken: cancellationToken)
                                                        .ReceiveBytes();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Commit failed");
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
    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
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

            byte[] responseBytes = await this.endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(builder.CommandTimeout)
                                                        .AppendPathSegments("rollback-transaction")
                                                        .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusTransactionRequest), cancellationToken: cancellationToken)
                                                        .ReceiveBytes();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            if (response?.Status != "ok")
                throw new CamusException("CADB0000", "Rollback failed");
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
