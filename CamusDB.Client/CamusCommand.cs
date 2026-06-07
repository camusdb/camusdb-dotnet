
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using CamusDB.Core.Util.ObjectIds;
using Flurl.Http;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL query or command to execute against a Camus database.
/// 
/// If the command is a SQL query, then <see cref="CamusCommand.CommandText"/>
/// contains the entire SQL statement. Use <see cref="CamusCommand.ExecuteReaderAsync()"/>  to obtain results.
///
/// If the command is an update, insert or delete command, then <see cref="CamusCommand.CommandText"/>
/// is simply "[operation] [camus_table]" such as "UPDATE MYTABLE" with the parameter
/// collection containing <see cref="CamusParameter"/> instances whose name matches a column
/// in the target table. Use <see cref="ExecuteNonQueryAsync"/> to execute the command.
///
/// The command may also be a DDL statement such as CREATE TABLE. Use <see cref="ExecuteNonQueryAsync"/>
/// to execute a DDL statement.
/// </summary>
public class CamusCommand : DbCommand, ICloneable
{
    protected readonly CamusConnectionStringBuilder builder;

    protected CamusTransaction? transaction;

    private CamusConnection? connection;

    private bool designTimeVisible;

    private UpdateRowSource updatedRowSource;

    public CamusCommand(string source, CamusConnectionStringBuilder builder, CamusConnection? connection = null)
    {
        this.builder = builder;
        this.connection = connection;
        CommandText = source;
        updatedRowSource = UpdateRowSource.None;
    }

    /// <summary>
    /// The parameters of the SQL statement or command.
    /// </summary>
    public new CamusParameterCollection Parameters { get; } = new CamusParameterCollection();

    [AllowNull]
    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; } = 10;

    public override CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get => designTimeVisible; set => designTimeVisible = value; }

    public override UpdateRowSource UpdatedRowSource { get => updatedRowSource; set => updatedRowSource = value; }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set
        {
            if (value is not null and not CamusConnection)
                throw new ArgumentException("Value must be a CamusConnection.", nameof(value));

            connection = (CamusConnection?)value;
        }
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction? DbTransaction { get => transaction; set => transaction = (CamusTransaction?) value; }

    public override void Cancel()
    {
        // CamusDB uses HTTP requests, so cancellation is cooperative via CancellationToken.
    }

    public object Clone()
    {
        CamusCommand clone = this switch
        {
            CamusInsertCommand => new CamusInsertCommand(CommandText, builder, connection),
            CamusPingCommand => new CamusPingCommand(CommandText, builder, connection),
            _ => new CamusCommand(CommandText, builder, connection)
        };

        clone.CommandTimeout = CommandTimeout;
        clone.CommandType = CommandType;
        clone.DesignTimeVisible = DesignTimeVisible;
        clone.UpdatedRowSource = UpdatedRowSource;
        clone.transaction = transaction;

        foreach (CamusParameter parameter in Parameters)
            clone.Parameters.Add((CamusParameter)parameter.Clone());

        return clone;
    }

    protected string GetRequestTarget() => CommandText;

    protected string GetEndpoint() => transaction?.Endpoint ?? builder.GetEndpoint();

    protected Dictionary<string, ColumnValue> GetCommandParameters()
    {
        Dictionary<string, ColumnValue> commandParameters = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
        {
            if (string.IsNullOrEmpty(parameter.ParameterName))
                throw new CamusException("CADB0400", "Parameter name cannot be null or empty");

            if (parameter.Value is null || parameter.ColumnType == ColumnType.Null)
                commandParameters.Add(parameter.ParameterName, new() { Type = ColumnType.Null });
            else if ((parameter.ColumnType == ColumnType.Id || parameter.ColumnType == ColumnType.String) && parameter.Value is string)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, StrValue = (string)parameter.Value! });
            else if ((parameter.ColumnType == ColumnType.Id) && parameter.Value is CamusObjectIdValue)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, StrValue = parameter.Value!.ToString() });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is int)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, LongValue = (int)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is long)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, LongValue = (long)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Float64 && parameter.Value is float)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, FloatValue = (float)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Float64 && parameter.Value is double)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, FloatValue = (double)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Bool)
                commandParameters.Add(parameter.ParameterName, new() { Type = parameter.ColumnType, BoolValue = (bool)parameter.Value! });
            else
                throw new CamusException("CADB0400", "Unknown parameter column type: " + parameter.ColumnType);
        }

        return commandParameters;
    }    

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync() =>
        ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="cancellationToken">An optional token for canceling the call.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync(CancellationToken cancellationToken) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="behavior">Options for statement execution and data retrieval.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync(CommandBehavior behavior) =>
        ExecuteReaderAsync(behavior, CancellationToken.None);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="behavior">Options for statement execution and data retrieval.</param>
    /// <param name="cancellationToken">An optional token for canceling the call.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new async Task<CamusDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        (CamusDataReader)await ExecuteDbDataReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteDbDataReaderAsync(behavior, default).GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        string endpoint = "";
        string database = builder.Config["Database"];                

        try
        {
            endpoint = GetEndpoint();

            CamusExecuteSqlQueryRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget(),
                Parameters = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryRequest);

            string responseJson = await endpoint
                                        .WithHeader("Accept", "application/json")
                                        .WithTimeout(CommandTimeout)
                                        .AppendPathSegments("execute-sql-query")
                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                        .ReceiveString();

            CamusExecuteSqlQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryResponse);

            if (response?.Rows == null)
                throw new CamusException("CADB0000", "Empty result returned");

            return new CamusDataReader(response.Rows);
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
    /// Executes the command and returns the number of rows affected.
    /// This method runs syncrhonously
    /// </summary>
    /// <returns></returns>
    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(default).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();
            string database = builder.Config["Database"];

            CamusExecuteSqlNonQueryRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget(),
                Parameters = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);

            string responseJson = await endpoint
                                        .WithHeader("Accept", "application/json")
                                        .WithTimeout(CommandTimeout)
                                        .AppendPathSegments("execute-sql-non-query")
                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                        .ReceiveString();

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            if (response is null)
                throw new CamusException("CADB0000", "Empty result returned");

            return response.Rows;
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
    /// Executes a DDL command and returns the success status
    /// This method runs syncrhonously
    /// </summary>
    /// <returns></returns>
    public bool ExecuteDDL()
    {
        return ExecuteDDLAsync(default).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public Task<bool> ExecuteDDLAsync()
    {
        return ExecuteDDLAsync(default);
    }

    /// <inheritdoc />
    public async Task<bool> ExecuteDDLAsync(CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();
            string database = builder.Config["Database"];            

            CamusExecuteDDLRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget()
            };

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteDDLRequest);

            string responseJson = await endpoint
                                    .WithHeader("Accept", "application/json")
                                    .WithTimeout(CommandTimeout)
                                    .AppendPathSegments("execute-sql-ddl")
                                    .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                    .ReceiveString();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            return response?.Status == "ok";
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

    public override object? ExecuteScalar()
    {
        using DbDataReader reader = ExecuteDbDataReader(CommandBehavior.SingleRow);

        if (!reader.Read() || reader.FieldCount == 0)
            return null;

        return reader.GetValue(0);
    }

    public override void Prepare()
    {
        // CamusDB does not currently expose a server-side prepare API.
    }

    protected override DbParameter CreateDbParameter()
    {
        return new CamusParameter();
    }
}
