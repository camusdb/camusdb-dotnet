
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using Flurl.Http;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL query or command to execute against
/// a Camus database.
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
    protected readonly string source;

    protected readonly CamusConnectionStringBuilder builder;

    public CamusCommand(string source, CamusConnectionStringBuilder builder)
    {
        this.source = source;
        this.builder = builder;
    }

    /// <summary>
    /// The parameters of the SQL statement or command.
    /// </summary>
    public new CamusParameterCollection Parameters { get; } = new CamusParameterCollection();

    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; } = 10;

    public override CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    protected override DbConnection? DbConnection { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

    protected override DbTransaction? DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public override void Cancel()
    {
        throw new NotImplementedException();
    }

    public object Clone()
    {
        throw new NotImplementedException();
    }

    protected Dictionary<string, ColumnValue> GetCommandParameters()
    {
        Dictionary<string, ColumnValue> commandParameters = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
        {
            if (parameter.ColumnType == ColumnType.Id || parameter.ColumnType == ColumnType.String)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, StrValue = parameter.Value!.ToString() });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is int)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, LongValue = (int)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is long)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, LongValue = (long)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Bool)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, BoolValue = (bool)parameter.Value! });
            else
                throw new CamusException("Unknown parameter column type");
        }

        return commandParameters;
    }

    public override int ExecuteNonQuery()
    {
        throw new NotImplementedException();
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
        Task.Run(() => ExecuteDbDataReaderAsync(behavior, default)).Result;        

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];

        Dictionary<string, ColumnValue> commandParameters = GetCommandParameters();

        try
        {
            ExecuteSqlQueryResponse response = await endpoint
                                                        .WithTimeout(CommandTimeout)
                                                        .AppendPathSegments("execute-sql-query")
                                                        .PostJsonAsync(new { databaseName = database, sql = source, parameters = commandParameters })
                                                        .ReceiveJson<ExecuteSqlQueryResponse>();

            return new CamusDataReader(response.Rows!);
        }
        catch (FlurlHttpException ex)
        {
            var response = await ex.GetResponseStringAsync();
            if (string.IsNullOrEmpty(response))
                throw new CamusException(ex.Message);

            throw new CamusException(response);
        }
    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        try
        {
            string endpoint = builder.Config["Endpoint"];
            string database = builder.Config["Database"];

            Dictionary<string, ColumnValue> commandParameters = GetCommandParameters();

            ExecuteSqlNonQueryResponse response = await endpoint
                                    .WithTimeout(CommandTimeout)
                                    .AppendPathSegments("execute-sql-non-query")
                                    .PostJsonAsync(new { databaseName = database, sql = source, parameters = commandParameters })
                                    .ReceiveJson<ExecuteSqlNonQueryResponse>();
            
            return response.Rows;
        }
        catch (FlurlHttpException ex)
        {
            var response = await ex.GetResponseStringAsync();
            if (string.IsNullOrEmpty(response))
                throw new CamusException(ex.Message);

            throw new CamusException(response);
        }
    }

    public override object? ExecuteScalar()
    {
        throw new NotImplementedException();
    }

    public override void Prepare()
    {
        throw new NotImplementedException();
    }

    protected override DbParameter CreateDbParameter()
    {
        throw new NotImplementedException();
    }    
}
