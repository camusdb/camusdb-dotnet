
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
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

    public override string ConnectionString { get; set; }

    public override string Database => throw new NotImplementedException();

    public override string DataSource => throw new NotImplementedException();

    public override string ServerVersion => throw new NotImplementedException();

    public override ConnectionState State => throw new NotImplementedException();

    public CamusConnection(CamusConnectionStringBuilder builder)
    {
        ConnectionString = "";
        this.builder = builder;
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    public override void Close()
    {
        throw new NotImplementedException();
    }

    public override void Open()
    {
        //throw new NotImplementedException();
    }

    //public override async Task OpenAsync()
    //{
    //    await Task.Delay(1);
    //}

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (isolationLevel != IsolationLevel.Unspecified && isolationLevel != IsolationLevel.Serializable)
            throw new NotSupportedException($"CamusDB only supports isolation levels {IsolationLevel.Serializable} and {IsolationLevel.Unspecified}.");

        var x = Task.Run(() => BeginTransactionAsync());

        return x.Result;
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
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];

        try
        {
            CamusStartTransactionResponse response = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(10)
                                                        .AppendPathSegments("start-transaction")
                                                        .PostJsonAsync(new { databaseName = database }, cancellationToken)
                                                        .ReceiveJson<CamusStartTransactionResponse>();

            if (response.Status != "ok")
                throw new CamusException("CADB0000", "Empty result returned");

            return new CamusTransaction(response.TxnIdPT, response.TxnIdCounter, builder);
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

    protected override DbCommand CreateDbCommand()
    {
        throw new NotImplementedException();
    }

    public CamusCommand CreateCamusCommand(string sql)
    {
        return new CamusCommand(sql, builder);
    }

    public CamusInsertCommand CreateInsertCommand(string source)
    {
        return new CamusInsertCommand(source, builder);
    }

    public CamusCommand CreateSelectCommand(string sql)
    {
        return new CamusCommand(sql, builder);
    }

    public CamusPingCommand CreatePingCommand()
    {
        return new CamusPingCommand("", builder);
    }
}
